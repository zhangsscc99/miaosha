package com.geekq.lab;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 秒杀实验服务。每个 mode 对应一档优化，接口与压测参数完全一致，便于横向对比：
 *
 *   v0 DB 直接扣减 + 同步写订单（基线）
 *   v1 Redis Lua 原子扣减 + 同步写订单
 *   v2 Redis Lua + RabbitMQ 异步下单（批量落库）
 *   v3 本地分段库存缓存 + RabbitMQ 异步下单 + 批量落库（单机最优路径）
 *   v4 v3 + 入口令牌桶限流
 */
public class SeckillServer {

    public static void main(String[] args) throws Exception {
        String mode = arg(args, "--mode", "v0");
        int port = Integer.parseInt(arg(args, "--port", "8080"));
        int rate = Integer.parseInt(arg(args, "--rate", "8000"));
        long goodsId = Long.parseLong(arg(args, "--goods", "1"));

        Db db = new Db();
        db.ping();
        RedisStock redis = new RedisStock();
        LocalStockCache cache = new LocalStockCache();
        int chunk = Integer.parseInt(arg(args, "--chunk", "2000"));
        ChunkedStock chunked = mode.equals("v5")
                ? new ChunkedStock(redis, cache, goodsId, chunk) : null;
        OrderPublisher mq = null;
        if (mode.equals("v2") || mode.equals("v3") || mode.equals("v4") || mode.equals("v5")) {
            mq = new OrderPublisher(db);
        }
        RateLimiter limiter = mode.equals("v4") ? new RateLimiter(rate, rate) : null;

        Metrics metrics = new Metrics();
        AtomicLong seq = new AtomicLong();
        OrderPublisher mqRef = mq;

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 4096);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        server.createContext("/seckill", exchange -> {
            long t0 = System.nanoTime();
            int code;
            try {
                metrics.requests.increment();
                Map<String, String> q = query(exchange.getRequestURI());
                long goods = Long.parseLong(q.getOrDefault("goodsId", String.valueOf(goodsId)));
                long user = Long.parseLong(q.getOrDefault("userId", "0"));
                String orderNo = "HC" + goods + "-" + user + "-" + seq.incrementAndGet();

                if (limiter != null && !limiter.tryAcquire()) {
                    metrics.limited.increment();
                    code = 429;
                } else if (mode.equals("v0")) {
                    code = db.deductAndCreateOrder(goods, user, orderNo) == 1 ? 200 : 410;
                } else if (mode.equals("v1")) {
                    long left = redis.deduct(goods);
                    if (left < 0) {
                        code = 410;
                    } else {
                        try {
                            db.insertOrder(goods, user, orderNo);
                            code = 200;
                        } catch (Exception e) {
                            redis.giveBack(goods); // 补偿，避免少卖
                            metrics.errors.increment();
                            code = 500;
                        }
                    }
                } else if (mode.equals("v2")) {
                    long left = redis.deduct(goods);
                    if (left < 0) {
                        code = 410;
                    } else if (!mqRef.enqueue(new OrderRow(orderNo, user, goods))) {
                        redis.giveBack(goods); // 队列满：回滚库存，返回背压
                        metrics.backpressure.increment();
                        code = 503;
                    } else {
                        code = 202;
                    }
                } else {
                    boolean got = chunked != null ? chunked.tryDeduct() : cache.tryDeduct();
                    if (!got) {
                        code = 410;
                    } else if (!mqRef.enqueue(new OrderRow(orderNo, user, goods))) {
                        if (chunked != null) {
                            chunked.giveBack();
                        } else {
                            cache.giveBack();
                        }
                        metrics.backpressure.increment();
                        code = 503;
                    } else {
                        code = 202;
                    }
                }
                switch (code) {
                    case 200 -> metrics.ok.increment();
                    case 202 -> metrics.accepted.increment();
                    case 410 -> metrics.soldOut.increment();
                    case 429 -> { /* already counted */ }
                    default -> metrics.errors.increment();
                }
            } catch (Exception e) {
                metrics.errors.increment();
                code = 500;
                if (metrics.errors.sum() % 1000 == 1) {
                    e.printStackTrace();
                }
            }
            respond(exchange, code, "{\"code\":" + code + "}", t0);
        });

        server.createContext("/stats", exchange -> {
            StringBuilder sb = new StringBuilder("{\"mode\":\"").append(mode).append("\",")
                    .append("\"metrics\":").append(metrics.toJson());
            if (mqRef != null) {
                sb.append(",\"mq\":").append(mqRef.statsJson());
            }
            if (mode.equals("v1") || mode.equals("v2")) {
                sb.append(",\"redisStock\":").append(redis.getStock(goodsId));
            }
            if (mode.equals("v3") || mode.equals("v4")) {
                sb.append(",\"localStock\":").append(cache.remaining())
                        .append(",\"soldOut\":").append(cache.isSoldOut());
            }
            if (mode.equals("v5")) {
                sb.append(",\"localStock\":").append(chunked.remaining())
                        .append(",\"redisStock\":").append(redis.getStock(goodsId))
                        .append(",\"soldOut\":").append(chunked.isSoldOut());
            }
            sb.append("}");
            respond(exchange, 200, sb.toString(), System.nanoTime());
        });

        server.createContext("/admin/reset", exchange -> {
            Map<String, String> q = query(exchange.getRequestURI());
            long goods = Long.parseLong(q.getOrDefault("goodsId", String.valueOf(goodsId)));
            int stock = Integer.parseInt(q.getOrDefault("stock", "1000"));
            try {
                db.reset(goods, stock);
                redis.setStock(goods, stock);
                cache.reset(mode.equals("v5") ? 0 : stock);
                metrics.reset();
                respond(exchange, 200, "{\"reset\":true,\"stock\":" + stock + "}", System.nanoTime());
            } catch (Exception e) {
                e.printStackTrace();
                respond(exchange, 500, "{\"reset\":false,\"error\":\"" + e.getMessage() + "\"}", System.nanoTime());
            }
        });

        server.createContext("/admin/reconcile", exchange -> {
            Map<String, String> q = query(exchange.getRequestURI());
            long goods = Long.parseLong(q.getOrDefault("goodsId", String.valueOf(goodsId)));
            try {
                int remain;
                if (mode.equals("v3") || mode.equals("v4")) {
                    remain = cache.remaining();
                } else if (mode.equals("v5")) {
                    // 单 Pod 视角：Redis 全局余量 + 本 Pod 未卖完的批发库存
                    remain = Math.max(0, redis.getStock(goods)) + chunked.remaining();
                } else {
                    remain = redis.getStock(goods);
                }
                db.updateStock(goods, remain);
                respond(exchange, 200, "{\"reconciled\":true,\"dbStock\":" + Math.max(0, remain) + "}", System.nanoTime());
            } catch (Exception e) {
                respond(exchange, 500, "{\"reconciled\":false}", System.nanoTime());
            }
        });

        server.start();
        System.out.printf("[server] mode=%s port=%d pid=%d java=%s%n",
                mode, port, ProcessHandle.current().pid(), System.getProperty("java.version"));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[server] shutting down");
            server.stop(0);
            if (limiter != null) {
                limiter.close();
            }
            if (mqRef != null) {
                mqRef.close();
            }
            redis.close();
            db.close();
        }));
    }

    private static void respond(HttpExchange ex, int code, String body, long startNanos) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
        ex.close();
    }

    private static Map<String, String> query(URI uri) {
        Map<String, String> map = new HashMap<>();
        String raw = uri.getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return map;
        }
        for (String pair : raw.split("&")) {
            int i = pair.indexOf('=');
            if (i > 0) {
                map.put(pair.substring(0, i), pair.substring(i + 1));
            }
        }
        return map;
    }

    private static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(name) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (args[i].startsWith(name + "=")) {
                return args[i].substring(name.length() + 1);
            }
        }
        return def;
    }
}
