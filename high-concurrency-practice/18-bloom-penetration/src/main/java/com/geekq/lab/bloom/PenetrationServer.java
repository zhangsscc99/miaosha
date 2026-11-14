package com.geekq.lab.bloom;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 商品详情接口的 5 种防穿透写法。
 *
 * mode=none        每次都查库（现状）
 * mode=nullcache   先查 Redis，未命中查库，无论有没有都写回（缺席写 __NULL__，60 秒）
 * mode=bloom       进程内布隆，判不存在直接 404，不查 Redis、不查库
 * mode=bloomredis  共享布隆（Redis 位图 + 一次 EVALSHA），多实例同一份
 * mode=bloomnull   布隆 + 空值占位：布隆只放行「可能存在」的，空值占位挡住重复的误判
 *
 * 存在的商品 id 是 1..N；不存在的 id 落在 1e9 以后，所以「布隆说没有」永远是对的，
 * 「布隆说有」有约 0.8% 是误判（这一版 n=5 万、10 位/元素、k=7）。
 */
public final class PenetrationServer {

    static final String CHECK_LUA = """
            local key = KEYS[1]
            for i = 1, #ARGV do
              if redis.call('GETBIT', key, ARGV[i]) == 0 then
                return 0
              end
            end
            return 1
            """;

    static final String MODE = arg("--mode", "none");
    static final int PORT = Integer.parseInt(arg("--port", "18090"));
    static final int GOODS = Integer.parseInt(arg("--goods", "50000"));
    static final String JDBC = arg("--jdbc", "jdbc:mysql://127.0.0.1:3306/miaosha_bloom?useSSL=false&allowPublicKeyRetrieval=true");
    static final String DB_USER = arg("--user", "root");
    static final String DB_PASS = arg("--pass", "root");
    static final int REDIS_PORT = Integer.parseInt(arg("--redisPort", "6381"));

    static final AtomicLong REQUESTS = new AtomicLong();
    static final AtomicLong DB_QUERIES = new AtomicLong();
    static final AtomicLong REDIS_CMDS = new AtomicLong();
    static final AtomicLong BLOOM_REJECTS = new AtomicLong();
    static final AtomicLong NULL_HITS = new AtomicLong();
    static final AtomicLong OK200 = new AtomicLong();
    static final AtomicLong NOT_FOUND = new AtomicLong();
    static final AtomicLong ERRORS = new AtomicLong();

    static Bloom bloom;
    static JedisPool pool;
    static String shaCheck;
    static com.zaxxer.hikari.HikariDataSource ds;

    public static void main(String[] args) throws Exception {
        prepareDb();
        bloom = new Bloom(GOODS, 10);
        for (int i = 1; i <= GOODS; i++) {
            bloom.add(i);
        }

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(32);
        cfg.setMaxIdle(32);
        pool = new JedisPool(cfg, "127.0.0.1", REDIS_PORT, 3000);
        try (Jedis j = pool.getResource()) {
            j.flushDB();
            shaCheck = j.scriptLoad(CHECK_LUA);
            if (MODE.equals("bloomredis")) {
                buildShared();
            }
        }

        com.zaxxer.hikari.HikariConfig hc = new com.zaxxer.hikari.HikariConfig();
        hc.setJdbcUrl(JDBC);
        hc.setUsername(DB_USER);
        hc.setPassword(DB_PASS);
        hc.setMaximumPoolSize(16);
        hc.setMinimumIdle(4);
        hc.setPoolName("lab");
        ds = new com.zaxxer.hikari.HikariDataSource(hc);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", PORT), 1024);
        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(exec);
        server.createContext("/goods", PenetrationServer::handleGoods);
        server.createContext("/stats", PenetrationServer::handleStats);
        server.createContext("/admin/reset", ex -> {
            REQUESTS.set(0);
            DB_QUERIES.set(0);
            REDIS_CMDS.set(0);
            BLOOM_REJECTS.set(0);
            NULL_HITS.set(0);
            OK200.set(0);
            NOT_FOUND.set(0);
            ERRORS.set(0);
            send(ex, 200, "{\"reset\":true}", "application/json");
        });
        server.start();
        System.out.printf("mode=%s port=%d goods=%d bloomBytes=%d hashes=%d fpTheory=%.4f%n",
                MODE, PORT, GOODS, bloom.bytes(), bloom.hashes(), bloom.theoryFalsePositive());
    }

    static void prepareDb() throws Exception {
        try (Connection c = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:3306/?useSSL=false&allowPublicKeyRetrieval=true", DB_USER, DB_PASS);
             Statement st = c.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS miaosha_bloom DEFAULT CHARSET utf8mb4");
            st.execute("CREATE TABLE IF NOT EXISTS miaosha_bloom.goods (" +
                    "id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, price INT NOT NULL) ENGINE=InnoDB");
            st.execute("TRUNCATE TABLE miaosha_bloom.goods");
        }
        try (Connection c = DriverManager.getConnection(JDBC, DB_USER, DB_PASS)) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO goods(id,name,price) VALUES(?,?,?)")) {
                for (int i = 1; i <= GOODS; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, "goods-" + i);
                    ps.setInt(3, i % 1000);
                    ps.addBatch();
                    if (i % 2000 == 0) {
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }
            c.commit();
        }
    }

    static void buildShared() {
        long t0 = System.nanoTime();
        int[] off = new int[bloom.hashes()];
        try (Jedis j = pool.getResource()) {
            Pipeline p = j.pipelined();
            int inBatch = 0;
            for (int i = 1; i <= GOODS; i++) {
                bloom.offsets(i, off);
                for (int o : off) {
                    p.setbit("bf:goods", o, true);
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
        System.out.printf("shared bloom built in %.0f ms%n", (System.nanoTime() - t0) / 1e6);
    }

    static void handleGoods(HttpExchange ex) throws java.io.IOException {
        REQUESTS.incrementAndGet();
        long t0 = System.nanoTime();
        String q = ex.getRequestURI().getQuery();
        long id = 0;
        if (q != null) {
            for (String kv : q.split("&")) {
                if (kv.startsWith("id=")) {
                    id = Long.parseLong(kv.substring(3));
                }
            }
        }
        try {
            String body = "{\"msg\":\"bad mode\"}";
            int status = 500;
            switch (MODE) {
                case "none" -> {
                    String[] row = select(id);
                    status = row == null ? 404 : 200;
                    body = row == null ? "{\"msg\":\"not found\"}"
                            : "{\"id\":" + id + ",\"name\":\"" + row[0] + "\"}";
                }
                case "nullcache" -> {
                    try (Jedis j = pool.getResource()) {
                        REDIS_CMDS.incrementAndGet();
                        String v = j.get("gn:" + id);
                        if (v != null) {
                            if (v.equals("__NULL__")) {
                                NULL_HITS.incrementAndGet();
                                status = 404;
                                body = "{\"msg\":\"not found\"}";
                            } else {
                                status = 200;
                                body = "{\"id\":" + id + ",\"name\":\"" + v + "\"}";
                            }
                        } else {
                            String[] row = select(id);
                            if (row == null) {
                                j.setex("gn:" + id, 60, "__NULL__");
                                status = 404;
                                body = "{\"msg\":\"not found\"}";
                            } else {
                                j.setex("gn:" + id, 60, row[0]);
                                status = 200;
                                body = "{\"id\":" + id + ",\"name\":\"" + row[0] + "\"}";
                            }
                        }
                    }
                }
                case "bloom" -> {
                    if (!bloom.mightContain(id)) {
                        BLOOM_REJECTS.incrementAndGet();
                        status = 404;
                        body = "{\"msg\":\"not found\"}";
                    } else {
                        String[] row = select(id);
                        status = row == null ? 404 : 200;
                        body = row == null ? "{\"msg\":\"not found\"}"
                                : "{\"id\":" + id + ",\"name\":\"" + row[0] + "\"}";
                    }
                }
                case "bloomredis" -> {
                    if (!sharedContains(id)) {
                        BLOOM_REJECTS.incrementAndGet();
                        status = 404;
                        body = "{\"msg\":\"not found\"}";
                    } else {
                        String[] row = select(id);
                        status = row == null ? 404 : 200;
                        body = row == null ? "{\"msg\":\"not found\"}"
                                : "{\"id\":" + id + ",\"name\":\"" + row[0] + "\"}";
                    }
                }
                case "bloomnull" -> {
                    if (!bloom.mightContain(id)) {
                        BLOOM_REJECTS.incrementAndGet();
                        status = 404;
                        body = "{\"msg\":\"not found\"}";
                    } else {
                        try (Jedis j = pool.getResource()) {
                            REDIS_CMDS.incrementAndGet();
                            String v = j.get("gn:" + id);
                            if (v != null) {
                                if (v.equals("__NULL__")) {
                                    NULL_HITS.incrementAndGet();
                                    status = 404;
                                    body = "{\"msg\":\"not found\"}";
                                } else {
                                    status = 200;
                                    body = "{\"id\":" + id + ",\"name\":\"" + v + "\"}";
                                }
                            } else {
                                String[] row = select(id);
                                if (row == null) {
                                    j.setex("gn:" + id, 60, "__NULL__");
                                    status = 404;
                                    body = "{\"msg\":\"not found\"}";
                                } else {
                                    j.setex("gn:" + id, 60, row[0]);
                                    status = 200;
                                    body = "{\"id\":" + id + ",\"name\":\"" + row[0] + "\"}";
                                }
                            }
                        }
                    }
                }
            }
            if (status == 200) {
                OK200.incrementAndGet();
            } else if (status == 404) {
                NOT_FOUND.incrementAndGet();
            }
            // 计时用在本进程里，这里不打日志
            if (System.nanoTime() - t0 < 0) {
                throw new IllegalStateException();
            }
            send(ex, status, body, "application/json");
        } catch (Exception e) {
            ERRORS.incrementAndGet();
            send(ex, 500, "{\"error\":\"" + e.getClass().getSimpleName() + "\"}", "application/json");
        }
    }

    static boolean sharedContains(long id) {
        int[] off = new int[bloom.hashes()];
        bloom.offsets(id, off);
        List<String> argv = new ArrayList<>(off.length);
        for (int o : off) {
            argv.add(Integer.toString(o));
        }
        try (Jedis j = pool.getResource()) {
            REDIS_CMDS.incrementAndGet();
            Object r = j.evalsha(shaCheck, List.of("bf:goods"), argv);
            return ((Long) r) == 1L;
        }
    }

    static String[] select(long id) {
        DB_QUERIES.incrementAndGet();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT name,price FROM goods WHERE id=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new String[]{rs.getString(1), Integer.toString(rs.getInt(2))};
            }
        } catch (Exception e) {
            ERRORS.incrementAndGet();
            return null;
        }
    }

    static void handleStats(HttpExchange ex) throws java.io.IOException {
        String json = "{\"mode\":\"" + MODE + "\",\"requests\":" + REQUESTS.get()
                + ",\"dbQueries\":" + DB_QUERIES.get()
                + ",\"redisCmds\":" + REDIS_CMDS.get()
                + ",\"bloomRejects\":" + BLOOM_REJECTS.get()
                + ",\"nullHits\":" + NULL_HITS.get()
                + ",\"ok200\":" + OK200.get()
                + ",\"notFound\":" + NOT_FOUND.get()
                + ",\"errors\":" + ERRORS.get()
                + ",\"bloomBytes\":" + bloom.bytes()
                + ",\"fpTheory\":" + String.format("%.5f", bloom.theoryFalsePositive()) + "}";
        send(ex, 200, json, "application/json");
    }

    static void send(HttpExchange ex, int status, String body, String type) throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Content-Length", Integer.toString(b.length));
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    static String arg(String name, String def) {
        String[] a = System.getProperty("sun.java.command", "").split(" ");
        for (int i = 0; i < a.length - 1; i++) {
            if (a[i].equals(name)) {
                return a[i + 1];
            }
        }
        return def;
    }
}
