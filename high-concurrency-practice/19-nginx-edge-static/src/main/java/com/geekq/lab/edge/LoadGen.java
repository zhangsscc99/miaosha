package com.geekq.lab.edge;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 压测客户端：随机打 goods 个商品 id，记录 RPS、p50/p99、状态码分布、响应体字节数。
 * 响应体一律读完（8KB 页面要真的走一遍网络和拷贝），所以 RPS 不是「只看响应头」的虚数。
 */
public final class LoadGen {

    public static void main(String[] args) throws Exception {
        String url = arg(args, "--url", "http://127.0.0.1:18080");
        String prefix = arg(args, "--prefix", "/static/p");
        String suffix = arg(args, "--suffix", ".html");
        String label = arg(args, "--label", "run");
        int seconds = Integer.parseInt(arg(args, "--seconds", "10"));
        int warmup = Integer.parseInt(arg(args, "--warmup", "3"));
        int concurrency = Integer.parseInt(arg(args, "--concurrency", "64"));
        int goods = Integer.parseInt(arg(args, "--goods", "1000"));
        long fixedId = Long.parseLong(arg(args, "--fixedId", "-1"));
        String header = arg(args, "--header", null);
        boolean acceptGzip = Boolean.parseBoolean(arg(args, "--gzip", "false"));
        String out = arg(args, "--out", null);

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        AtomicBoolean recording = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong ok200 = new AtomicLong();
        AtomicLong notFound = new AtomicLong();
        AtomicLong unavailable = new AtomicLong();
        AtomicLong other = new AtomicLong();
        AtomicLong bytes = new AtomicLong();
        AtomicLong contentLength = new AtomicLong();
        Map<String, AtomicLong> headerCounts = new HashMap<>();
        Hist hist = new Hist();
        Object lock = new Object();

        for (int t = 0; t < concurrency; t++) {
            pool.submit(() -> {
                Hist local = new Hist();
                while (!stop.get()) {
                    long id = fixedId > 0 ? fixedId : 1 + ThreadLocalRandom.current().nextLong(goods);
                    long q0 = System.nanoTime();
                    try {
                        HttpRequest.Builder rb = HttpRequest.newBuilder(
                                URI.create(url + prefix + id + suffix)).GET();
                        if (acceptGzip) {
                            rb.header("Accept-Encoding", "gzip");
                        }
                        HttpRequest req = rb.build();
                        HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
                        long us = (System.nanoTime() - q0) / 1000;
                        int code = resp.statusCode();
                        if (!recording.get()) {
                            continue;
                        }
                        if (code == 200) {
                            local.record(us);
                            bytes.addAndGet(resp.body().length);
                            resp.headers().firstValue("content-length")
                                    .ifPresent(v -> contentLength.addAndGet(Long.parseLong(v)));
                            if (header != null) {
                                String hv = resp.headers().firstValue(header).orElse("none");
                                synchronized (headerCounts) {
                                    headerCounts.computeIfAbsent(hv, k -> new AtomicLong()).incrementAndGet();
                                }
                            }
                            ok200.incrementAndGet();
                        } else if (code == 404) {
                            notFound.incrementAndGet();
                        } else if (code == 503 || code == 429) {
                            unavailable.incrementAndGet();
                        } else {
                            other.incrementAndGet();
                        }
                    } catch (Exception e) {
                        if (recording.get()) {
                            other.incrementAndGet();
                        }
                    }
                }
                synchronized (lock) {
                    hist.mergeFrom(local);
                }
            });
        }

        Thread.sleep(warmup * 1000L);
        if (out != null) {
            java.nio.file.Files.writeString(java.nio.file.Path.of(out + ".start"),
                    Long.toString(System.currentTimeMillis()));
        }
        long t0 = System.nanoTime();
        recording.set(true);
        Thread.sleep(seconds * 1000L);
        recording.set(false);
        if (out != null) {
            java.nio.file.Files.writeString(java.nio.file.Path.of(out + ".end"),
                    Long.toString(System.currentTimeMillis()));
        }
        double elapsed = (System.nanoTime() - t0) / 1e9;
        Thread.sleep(300);
        stop.set(true);
        pool.shutdown();
        pool.awaitTermination(20, java.util.concurrent.TimeUnit.SECONDS);

        long reqs = ok200.get() + notFound.get() + unavailable.get() + other.get();
        StringBuilder hc = new StringBuilder("{");
        synchronized (headerCounts) {
            boolean first = true;
            for (Map.Entry<String, AtomicLong> e : headerCounts.entrySet()) {
                if (!first) {
                    hc.append(',');
                }
                hc.append('"').append(e.getKey()).append("\":").append(e.getValue().get());
                first = false;
            }
        }
        hc.append('}');

        String json = "{\"label\":\"" + label + "\""
                + ",\"prefix\":\"" + prefix + "\""
                + ",\"concurrency\":" + concurrency
                + ",\"seconds\":" + String.format("%.2f", elapsed)
                + ",\"requests\":" + reqs
                + ",\"rps\":" + String.format("%.0f", reqs / elapsed)
                + ",\"ok200\":" + ok200.get()
                + ",\"notFound\":" + notFound.get()
                + ",\"unavailable\":" + unavailable.get()
                + ",\"other\":" + other.get()
                + ",\"bodyBytes\":" + bytes.get()
                + ",\"contentLengthBytes\":" + contentLength.get()
                + ",\"headerCounts\":" + hc
                + ",\"latency\":" + hist.json() + "}";
        if (out != null) {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of(out).getParent());
            java.nio.file.Files.writeString(java.nio.file.Path.of(out), json);
        }
        System.out.println(json);
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
