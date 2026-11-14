package com.geekq.lab.bloom;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多实例共享的布隆过滤器：位图放在 Redis，哈希偏移在客户端算，一次 EVALSHA 问完 k 个位。
 *
 * 这一组回答：共享布隆要几个核、一次查询几次往返、批量能不能把往返摊薄。
 * 布隆本身不存元素，所以 1 亿位只有 12.5MB，这是它比「空值占位」省内存的地方。
 */
public final class RedisBloomBench {

    static final int HOST_PORT = 6381;
    static final String CHECK_LUA = """
            local key = KEYS[1]
            for i = 1, #ARGV do
              if redis.call('GETBIT', key, ARGV[i]) == 0 then
                return 0
              end
            end
            return 1
            """;
    static final String ADD_LUA = """
            local key = KEYS[1]
            for i = 1, #ARGV do
              redis.call('SETBIT', key, ARGV[i], 1)
            end
            return 1
            """;

    public static void main(String[] args) throws Exception {
        String out = arg(args, "--out", "results/redis-bloom.json");
        int n = Integer.parseInt(arg(args, "--keys", "200000"));
        int bpk = 10;
        int seconds = Integer.parseInt(arg(args, "--seconds", "5"));

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(64);
        cfg.setMaxIdle(64);
        try (JedisPool pool = new JedisPool(cfg, "127.0.0.1", HOST_PORT, 3000)) {
            try (Jedis j = pool.getResource()) {
                j.flushDB();
            }
            Bloom bloom = new Bloom(n, bpk);
            String shaCheck;
            String shaAdd;
            try (Jedis j = pool.getResource()) {
                shaCheck = j.scriptLoad(CHECK_LUA);
                shaAdd = j.scriptLoad(ADD_LUA);
            }

            // 构建：客户端算 7 个偏移，pipeline 每批 5000 条 SETBIT
            long t0 = System.nanoTime();
            long setbits = 0;
            int[] off = new int[bloom.hashes()];
            try (Jedis j = pool.getResource()) {
                Pipeline p = j.pipelined();
                int inBatch = 0;
                for (int i = 0; i < n; i++) {
                    long id = 1_000_000L + i;
                    bloom.offsets(id, off);
                    for (int o : off) {
                        p.setbit("bf:goods", o, true);
                        setbits++;
                        if (++inBatch >= 5000) {
                            p.sync();
                            inBatch = 0;
                        }
                    }
                }
                if (inBatch > 0) {
                    p.sync();
                }
            }
            double buildSeconds = (System.nanoTime() - t0) / 1e9;

            long filterBits;
            long filterBytes;
            try (Jedis j = pool.getResource()) {
                filterBits = j.strlen("bf:goods") * 8;
                filterBytes = j.strlen("bf:goods");
                // 1 亿位的位图实际占多少内存
                j.setbit("bf:hundred-million", 100_000_000L, true);
            }
            long hundredMillionBytes;
            try (Jedis j = pool.getResource()) {
                hundredMillionBytes = j.strlen("bf:hundred-million");
            }

            // 校验：已写入的一定命中
            int missed = 0;
            try (Jedis j = pool.getResource()) {
                for (int i = 0; i < 2000; i++) {
                    bloom.offsets(1_000_000L + i, off);
                    List<String> argv = toList(off);
                    long r = (Long) j.evalsha(shaCheck, List.of("bf:goods"), argv);
                    if (r == 0) {
                        missed++;
                    }
                }
            }

            Result lua = run(seconds, 16, () -> {
                try (Jedis j = pool.getResource()) {
                    bloom.offsets(Thread.currentThread().getId() * 1_000_003L + System.nanoTime(), off);
                    return (Long) j.evalsha(shaCheck, List.of("bf:goods"), toList(off));
                }
            });

            AtomicLong seq = new AtomicLong(9_000_000L);
            Result pipe = run(seconds, 16, () -> {
                try (Jedis j = pool.getResource()) {
                    Pipeline p = j.pipelined();
                    for (int i = 0; i < 100; i++) {
                        long id = seq.incrementAndGet();
                        bloom.offsets(id, off);
                        for (int o : off) {
                            p.getbit("bf:goods", o);
                        }
                    }
                    p.sync();
                    return 1L;
                }
            });

            AtomicLong addSeq = new AtomicLong(50_000_000L);
            Result addLua = run(seconds, 16, () -> {
                try (Jedis j = pool.getResource()) {
                    long id = addSeq.incrementAndGet();
                    bloom.offsets(id, off);
                    return (Long) j.evalsha(shaAdd, List.of("bf:goods"), toList(off));
                }
            });

            // 不用 Lua，客户端自己发 k 次 GETBIT：一次查询 k 次往返
            Result naive = run(seconds, 1, () -> {
                try (Jedis j = pool.getResource()) {
                    long id = 7_000_000L + System.nanoTime();
                    bloom.offsets(id, off);
                    for (int o : off) {
                        j.getbit("bf:goods", o);
                    }
                    return 1L;
                }
            });

            // 同样的 7 次往返，16 条连接：看「多开连接」能不能补回往返的损失
            Result naive16 = run(seconds, 16, () -> {
                try (Jedis j = pool.getResource()) {
                    long id = 17_000_000L + System.nanoTime();
                    bloom.offsets(id, off);
                    for (int o : off) {
                        j.getbit("bf:goods", o);
                    }
                    return 1L;
                }
            });

            String json = "{\"date\":\"" + java.time.Instant.now() + "\",\n"
                    + "\"port\":" + HOST_PORT + ",\"keys\":" + n + ",\"bitsPerKey\":" + bpk
                    + ",\"hashes\":" + bloom.hashes()
                    + ",\"filterBits\":" + filterBits
                    + ",\"filterBytes\":" + filterBytes
                    + ",\"buildSeconds\":" + String.format("%.3f", buildSeconds)
                    + ",\"setbitCalls\":" + setbits
                    + ",\"setbitPerSec\":" + String.format("%.0f", setbits / buildSeconds)
                    + ",\"hundredMillionBitsBytes\":" + hundredMillionBytes
                    + ",\"missedAfterBuild\":" + missed
                    + ",\"luaSingle\":" + lua.json()
                    + ",\"pipeline100\":" + pipe.json(100)
                    + ",\"luaAdd\":" + addLua.json()
                    + ",\"naive7RoundTrips\":" + naive.json()
                    + ",\"naive16\":" + naive16.json()
                    + "}";
            Files.createDirectories(Path.of(out).getParent());
            Files.writeString(Path.of(out), json);
            System.out.println(json);
        }
    }

    static List<String> toList(int[] off) {
        List<String> l = new ArrayList<>(off.length);
        for (int o : off) {
            l.add(Integer.toString(o));
        }
        return l;
    }

    interface Op {
        Long call() throws Exception;
    }

    static final class Result {
        long ops;
        long seconds;
        long errors;
        final Hist hist = new Hist();

        String json() {
            return json(1);
        }

        String json(int batch) {
            return "{\"ops\":" + ops + ",\"seconds\":" + String.format("%.2f", (double) seconds)
                    + ",\"opsPerSec\":" + String.format("%.0f", (double) ops / seconds)
                    + ",\"requestsPerSec\":" + String.format("%.0f", (double) ops * batch / seconds)
                    + ",\"errors\":" + errors
                    + ",\"latency\":" + hist.json() + "}";
        }
    }

    static Result run(int seconds, int threads, Op op) throws Exception {
        Result r = new Result();
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong ops = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        Thread[] ts = new Thread[threads];
        long t0 = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            ts[t] = new Thread(() -> {
                Hist local = new Hist();
                long c = 0;
                while (!stop.get()) {
                    long q0 = System.nanoTime();
                    try {
                        op.call();
                        c++;
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                    local.record((System.nanoTime() - q0) / 1000);
                }
                ops.addAndGet(c);
                synchronized (r.hist) {
                    r.hist.mergeFrom(local);
                }
            });
            ts[t].start();
        }
        Thread.sleep(seconds * 1000L);
        stop.set(true);
        for (Thread t : ts) {
            t.join();
        }
        r.seconds = System.nanoTime() - t0;
        r.seconds = Math.max(1, r.seconds) / 1_000_000_000L;
        r.ops = ops.get();
        r.errors = errors.get();
        return r;
    }

    static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return def;
    }
}
