package com.geekq.lab;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 三种"扣库存"方式的微基准：本地锁 / Redis 分布式锁 / Redis Lua 原子脚本。
 * 同一个热点 key、同样的线程数，只比较临界区实现，隔离网络与业务差异。
 */
public class LockBench {

    private static final String DEDUCT_LUA =
            "local s = tonumber(redis.call('get', KEYS[1])) "
                    + "if (not s) or s <= 0 then return -1 end "
                    + "return redis.call('decr', KEYS[1])";

    private static final String UNLOCK_LUA =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    public static void main(String[] args) throws Exception {
        String strategy = args.length > 0 ? args[0] : "all";
        int threads = args.length > 1 ? Integer.parseInt(args[1]) : 8;
        int opsPerThread = args.length > 2 ? Integer.parseInt(args[2]) : 3000;
        long totalOps = (long) threads * opsPerThread;

        List<String> strategies = strategy.equals("all")
                ? List.of("local", "redlock", "lua")
                : List.of(strategy);

        List<String> results = new ArrayList<>();
        for (String s : strategies) {
            results.add(run(s, threads, opsPerThread, totalOps));
        }
        System.out.println("[" + String.join(",", results) + "]");
    }

    private static String run(String strategy, int threads, int opsPerThread, long totalOps) throws Exception {
        String stockKey = "hc:bench:stock";
        String lockKey = "hc:bench:lock";
        try (Jedis setup = new Jedis("127.0.0.1", 6379)) {
            setup.set(stockKey, String.valueOf(totalOps));
            setup.del(lockKey);
        }

        AtomicLong counter = new AtomicLong(totalOps);
        Object monitor = new Object();
        AtomicLong retries = new AtomicLong();
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>(threads * opsPerThread));
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            workers.add(Thread.ofPlatform().name("bench-" + i).start(() -> {
                Jedis j = new Jedis("127.0.0.1", 6379, 2000);
                ready.countDown();
                try {
                    go.await();
                    for (int k = 0; k < opsPerThread; k++) {
                        long t0 = System.nanoTime();
                        switch (strategy) {
                            case "local" -> {
                                synchronized (monitor) {
                                    counter.decrementAndGet();
                                }
                            }
                            case "redlock" -> {
                                String token = UUID.randomUUID().toString();
                                while (true) {
                                    String r = j.set(lockKey, token,
                                            redis.clients.jedis.params.SetParams.setParams().nx().px(3000));
                                    if ("OK".equals(r)) {
                                        break;
                                    }
                                    retries.incrementAndGet();
                                    LockSupport.parkNanos(50_000L); // 50us 退避，模拟生产中带 backoff 的自旋
                                }
                                try {
                                    int stock = Integer.parseInt(j.get(stockKey));
                                    if (stock > 0) {
                                        j.decr(stockKey);
                                    }
                                } finally {
                                    j.eval(UNLOCK_LUA, List.of(lockKey), List.of(token));
                                }
                            }
                            case "lua" -> j.eval(DEDUCT_LUA, List.of(stockKey), Collections.emptyList());
                            default -> throw new IllegalArgumentException(strategy);
                        }
                        latencies.add(System.nanoTime() - t0);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    j.close();
                }
            }));
        }

        ready.await();
        long t0 = System.nanoTime();
        go.countDown();
        for (Thread t : workers) {
            t.join();
        }
        double secs = (System.nanoTime() - t0) / 1e9;

        long[] arr = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
        double p50 = pct(arr, 50) / 1000.0;
        double p99 = pct(arr, 99) / 1000.0;
        long qps = Math.round(arr.length / secs);

        String json = String.format(
                "{\"strategy\":\"%s\",\"threads\":%d,\"ops\":%d,\"qps\":%d,"
                        + "\"p50Us\":%.1f,\"p99Us\":%.1f,\"retries\":%d,\"seconds\":%.2f}",
                strategy, threads, arr.length, qps, p50, p99, retries.get(), secs);
        System.out.println("[lockbench] " + json);
        return json;
    }

    private static double pct(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.ceil(sorted.length * p / 100.0) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }
}
