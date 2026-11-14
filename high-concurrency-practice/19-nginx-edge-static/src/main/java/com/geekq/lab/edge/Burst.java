package com.geekq.lab.edge;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 一发齐射：N 个虚拟线程用 CyclicBarrier 对齐，同时打同一个冷 URL，每个线程只发一次。
 * 用来量「缓存击穿时有多少请求真的到了上游」，延迟由 Java 直方图记录。
 */
public final class Burst {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(arg(args, "--port", "18084"));
        int n = Integer.parseInt(arg(args, "--n", "256"));
        String path = arg(args, "--path", "/dyn?id=7&b=" + System.currentTimeMillis());
        int statsPort = Integer.parseInt(arg(args, "--statsPort", "18090"));

        Map<String, Long> before = stats(statsPort);
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier barrier = new CyclicBarrier(n);
        Hist hist = new Hist();
        Map<Integer, AtomicLong> codes = new ConcurrentHashMap<>();
        Map<String, AtomicLong> cache = new ConcurrentHashMap<>();
        AtomicInteger done = new AtomicInteger();
        long t0 = System.nanoTime();

        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    long q0 = System.nanoTime();
                    HttpResponse<byte[]> resp = client.send(
                            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                            HttpResponse.BodyHandlers.ofByteArray());
                    long us = (System.nanoTime() - q0) / 1000;
                    hist.record(us);
                    codes.computeIfAbsent(resp.statusCode(), k -> new AtomicLong()).incrementAndGet();
                    cache.computeIfAbsent(resp.headers().firstValue("X-Cache").orElse("none"),
                            k -> new AtomicLong()).incrementAndGet();
                } catch (Exception e) {
                    codes.computeIfAbsent(-1, k -> new AtomicLong()).incrementAndGet();
                } finally {
                    done.incrementAndGet();
                }
            });
        }
        pool.shutdown();
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
            System.out.println("{\"error\":\"timeout\"}");
            return;
        }
        long wallMs = (System.nanoTime() - t0) / 1_000_000;
        Thread.sleep(300); // 等后台更新（stale-while-revalidate）落完
        Map<String, Long> after = stats(statsPort);

        StringBuilder sb = new StringBuilder("{\"n\":").append(n)
                .append(",\"wallMs\":").append(wallMs)
                .append(",\"done\":").append(done.get())
                .append(",\"latency\":").append(hist.json())
                .append(",\"codes\":{");
        boolean first = true;
        for (Map.Entry<Integer, AtomicLong> e : new TreeMap<>(codes).entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(e.getKey() == -1 ? "err" : e.getKey().toString()).append("\":")
                    .append(e.getValue().get());
            first = false;
        }
        sb.append("},\"cache\":{");
        first = true;
        for (Map.Entry<String, AtomicLong> e : cache.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(e.getKey()).append("\":").append(e.getValue().get());
            first = false;
        }
        sb.append("},\"originPage\":").append(after.getOrDefault("page", 0L) - before.getOrDefault("page", 0L))
                .append(",\"originRedis\":").append(after.getOrDefault("redis", 0L) - before.getOrDefault("redis", 0L))
                .append('}');
        System.out.println(sb);
    }

    private static Map<String, Long> stats(int port) {
        Map<String, Long> m = new ConcurrentHashMap<>();
        try {
            HttpClient c = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            HttpResponse<String> r = c.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + "/stats")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            String s = r.body();
            for (String k : new String[]{"page", "api", "redis", "errors"}) {
                int i = s.indexOf('"' + k + "\":");
                if (i >= 0) {
                    int start = i + k.length() + 3;
                    int end = start;
                    while (end < s.length() && Character.isDigit(s.charAt(end))) {
                        end++;
                    }
                    m.put(k, Long.parseLong(s.substring(start, end)));
                }
            }
        } catch (Exception ignore) {
        }
        return m;
    }

    private static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return def;
    }
}
