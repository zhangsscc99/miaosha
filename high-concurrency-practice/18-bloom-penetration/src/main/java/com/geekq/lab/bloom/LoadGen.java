package com.geekq.lab.bloom;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 压测客户端。workload=scan 是「扫号」：每次换一个新的不存在 id（爬虫、脚本枚举）；
 * workload=replay 是「重放」：同一批 1000 个不存在 id 反复打（缓存空值才有机会生效）。
 * scan 里 10% 的请求换成 1..N 里真实存在的商品，用来核对「存在的必须 200」。
 */
public final class LoadGen {

    public static void main(String[] args) throws Exception {
        String host = arg(args, "--url", "http://127.0.0.1:18090");
        String workload = arg(args, "--workload", "scan");
        String label = arg(args, "--label", "run");
        int seconds = Integer.parseInt(arg(args, "--seconds", "10"));
        int warmup = Integer.parseInt(arg(args, "--warmup", "3"));
        int concurrency = Integer.parseInt(arg(args, "--concurrency", "64"));
        int goods = Integer.parseInt(arg(args, "--goods", "50000"));
        long scanBase = Long.parseLong(arg(args, "--scanBase", "1000000000"));
        int replayIds = Integer.parseInt(arg(args, "--replayIds", "1000"));
        long seqOffset = Long.parseLong(arg(args, "--seqOffset", "0"));
        String out = arg(args, "--out", null);

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        URI uri = URI.create(host + "/goods");

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        AtomicBoolean recording = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong ok = new AtomicLong();
        AtomicLong notFound = new AtomicLong();
        AtomicLong other = new AtomicLong();
        AtomicLong existingSent = new AtomicLong();
        AtomicLong missingSent = new AtomicLong();
        Hist hist = new Hist();
        Object lock = new Object();

        // 请求 id 由「序号」决定，客户端和服务端不共享随机种子
        AtomicLong seqGen = new AtomicLong(seqOffset);
        for (int t = 0; t < concurrency; t++) {
            final int tid = t;
            pool.submit(() -> {
                Hist local = new Hist();
                while (!stop.get()) {
                    long seq = seqGen.getAndIncrement();
                    boolean existing = (seq % 10) == 0;
                    long id;
                    if (existing) {
                        id = 1 + Math.floorMod(seq / 10, goods);
                    } else if (workload.equals("replay")) {
                        id = scanBase + Math.floorMod(seq, replayIds);
                    } else {
                        id = scanBase + Math.floorMod(seq, 100_000_000L);
                    }
                    long q0 = System.nanoTime();
                    try {
                        HttpRequest req = HttpRequest.newBuilder(URI.create(uri + "?id=" + id)).GET().build();
                        HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
                        long us = (System.nanoTime() - q0) / 1000;
                        if (recording.get()) {
                            local.record(us);
                        }
                        int code = resp.statusCode();
                        if (recording.get()) {
                            if (code == 200) {
                                ok.incrementAndGet();
                            } else if (code == 404) {
                                notFound.incrementAndGet();
                            } else {
                                other.incrementAndGet();
                            }
                            if (existing) {
                                existingSent.incrementAndGet();
                            } else {
                                missingSent.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        if (other.get() < 3) {
                            System.err.println("loadgen error: " + e);
                        }
                        other.incrementAndGet();
                    }
                }
                synchronized (lock) {
                    hist.mergeFrom(local);
                }
            });
        }

        Thread.sleep(warmup * 1000L);
        long t0 = System.nanoTime();
        recording.set(true);
        Thread.sleep(seconds * 1000L);
        recording.set(false);
        double elapsed = (System.nanoTime() - t0) / 1e9;
        Thread.sleep(300);
        stop.set(true);
        pool.shutdown();
        pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

        long reqs = ok.get() + notFound.get() + other.get();
        String json = "{\"label\":\"" + label + "\",\"workload\":\"" + workload + "\""
                + ",\"concurrency\":" + concurrency
                + ",\"seconds\":" + String.format("%.2f", elapsed)
                + ",\"requests\":" + reqs
                + ",\"rps\":" + String.format("%.0f", reqs / elapsed)
                + ",\"ok200\":" + ok.get()
                + ",\"notFound404\":" + notFound.get()
                + ",\"other\":" + other.get()
                + ",\"existingSent\":" + existingSent.get()
                + ",\"missingSent\":" + missingSent.get()
                + ",\"correct\":" + (ok.get() == existingSent.get() && notFound.get() == missingSent.get())
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
