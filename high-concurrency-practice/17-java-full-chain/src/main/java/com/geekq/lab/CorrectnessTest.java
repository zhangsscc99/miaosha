package com.geekq.lab;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 正确性验证：库存 N 件、M 个不同用户并发抢购，最终必须恰好生成 N 个订单，
 * 不能超卖也不能少卖（异步模式下等待 MQ 消费完，再调用 /admin/reconcile 对账）。
 */
public class CorrectnessTest {

    public static void main(String[] args) throws Exception {
        String base = args.length > 0 ? args[0] : "http://127.0.0.1:18080";
        int stock = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int requests = args.length > 2 ? Integer.parseInt(args[2]) : 3000;
        int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 128;
        boolean lenient = args.length > 4 && args[4].equalsIgnoreCase("lenient");
        long goodsId = 1;

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        client.send(HttpRequest.newBuilder(URI.create(base + "/admin/reset?goodsId=1&stock=" + stock)).GET().build(),
                HttpResponse.BodyHandlers.discarding());

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger soldOut = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger sent = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        Thread[] ts = new Thread[Math.min(concurrency, requests)];
        for (int i = 0; i < ts.length; i++) {
            final int tid = i;
            ts[i] = Thread.ofVirtual().start(() -> {
                try {
                    go.await();
                    for (int k = tid; k < requests; k += ts.length) {
                        sent.incrementAndGet();
                        long uid = 900_000_000L + k;
                        HttpResponse<Void> r = client.send(
                                HttpRequest.newBuilder(URI.create(base + "/seckill?goodsId=1&userId=" + uid))
                                        .GET().timeout(Duration.ofSeconds(5)).build(),
                                HttpResponse.BodyHandlers.discarding());
                        int sc = r.statusCode();
                        if (sc == 200 || sc == 202) {
                            accepted.incrementAndGet();
                        } else if (sc == 410) {
                            soldOut.incrementAndGet();
                        } else {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
        }
        long t0 = System.nanoTime();
        go.countDown();
        for (Thread t : ts) {
            t.join();
        }
        double secs = (System.nanoTime() - t0) / 1e9;

        try (Db db = new Db()) {
            long deadline = System.currentTimeMillis() + 30_000;
            long orders;
            while (true) {
                orders = db.orderCount(goodsId);
                if (orders >= accepted.get() || System.currentTimeMillis() > deadline) {
                    break;
                }
                Thread.sleep(200);
            }

            // 异步模式下 DB 的库存行不是实时扣减的，活动结束后按 Redis/本地余量对账
            client.send(HttpRequest.newBuilder(URI.create(base + "/admin/reconcile?goodsId=1")).GET().build(),
                    HttpResponse.BodyHandlers.discarding());

            String stats = client.send(HttpRequest.newBuilder(URI.create(base + "/stats")).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            int redisStock = intField(stats, "redisStock");
            int localStock = intField(stats, "localStock");
            int dbStock = db.stock(goodsId);
            boolean coreOk = sent.get() == requests
                    && accepted.get() == stock
                    && orders == stock
                    && errors.get() == 0;
            // 多 Pod 模式下，DB 库存与各 Pod 本地余量由独立对账任务汇总，这里只校验"不超卖/不少卖"
            boolean ok = lenient
                    ? coreOk
                    : coreOk && dbStock == 0
                    && (redisStock < 0 || redisStock == 0)
                    && (localStock < 0 || localStock == 0);

            String json = String.format(
                    "{\"stock\":%d,\"requests\":%d,\"sent\":%d,\"accepted\":%d,\"soldOut\":%d,\"errors\":%d,"
                            + "\"dbOrders\":%d,\"dbStock\":%d,\"redisStock\":%d,\"localStock\":%d,"
                            + "\"seconds\":%.2f,\"pass\":%b}",
                    stock, requests, sent.get(), accepted.get(), soldOut.get(), errors.get(),
                    orders, dbStock, redisStock, localStock, secs, ok);
            System.out.println("[correctness] " + json);
            if (!ok) {
                System.exit(1);
            }
        }
    }

    private static int intField(String json, String key) {
        int i = json.indexOf("\"" + key + "\":");
        if (i < 0) {
            return -1;
        }
        int j = i + key.length() + 3;
        int e = j;
        while (e < json.length() && (Character.isDigit(json.charAt(e)) || json.charAt(e) == '-')) {
            e++;
        }
        try {
            return Integer.parseInt(json.substring(j, e));
        } catch (NumberFormatException ex) {
            return -1;
        }
    }
}
