package com.geekq.lab;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 压测客户端：虚拟线程 + 阻塞式 HttpClient，每个请求使用不同 userId（避免业务上的重复下单拦截）。
 * 只统计 200/202 的成功请求延迟，410/429/5xx 只计数。
 */
public class LoadGen {

    static final class LongList {
        private long[] a = new long[1024];
        private int n;

        void add(long v) {
            if (n == a.length) {
                a = Arrays.copyOf(a, a.length * 2);
            }
            a[n++] = v;
        }

        int size() {
            return n;
        }

        long[] toArray() {
            return Arrays.copyOf(a, n);
        }
    }

    public static void main(String[] args) throws Exception {
        String url = arg(args, "--url", "http://127.0.0.1:8080/seckill?goodsId=1");
        String urlsArg = arg(args, "--urls", null);
        String[] urls = urlsArg != null ? urlsArg.split(",") : new String[]{url};
        String label = arg(args, "--label", "run");
        int seconds = Integer.parseInt(arg(args, "--seconds", "15"));
        int warmup = Integer.parseInt(arg(args, "--warmup", "5"));
        int concurrency = Integer.parseInt(arg(args, "--concurrency", "64"));
        long userBase = Long.parseLong(arg(args, "--userBase", "1000000"));
        String out = arg(args, "--out", null);

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        AtomicBoolean recording = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong ok = new AtomicLong();
        AtomicLong okWindow = new AtomicLong();
        AtomicLong totalWindow = new AtomicLong();
        AtomicLong soldOut = new AtomicLong();
        AtomicLong limited = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        List<LongList> lists = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            final long base = userBase + (long) i * 10_000_000L;
            final String myUrl = urls[i % urls.length];
            LongList mine = new LongList();
            lists.add(mine);
            threads.add(Thread.ofVirtual().name("lg-" + i).start(() -> {
                long seq = 0;
                while (!stop.get()) {
                    long uid = base + (seq++);
                    URI uri = URI.create(myUrl + (myUrl.contains("?") ? "&" : "?") + "userId=" + uid);
                    long t0 = System.nanoTime();
                    try {
                        HttpResponse<Void> resp = client.send(
                                HttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(5)).build(),
                                HttpResponse.BodyHandlers.discarding());
                        long dt = System.nanoTime() - t0;
                        int sc = resp.statusCode();
                        if (recording.get()) {
                            totalWindow.incrementAndGet();
                        }
                        if (sc == 200 || sc == 202) {
                            ok.incrementAndGet();
                            if (recording.get()) {
                                okWindow.incrementAndGet();
                                mine.add(dt);
                            }
                        } else if (sc == 410) {
                            soldOut.incrementAndGet();
                        } else if (sc == 429) {
                            limited.incrementAndGet();
                        } else {
                            errors.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            }));
        }

        Thread.sleep(warmup * 1000L);
        long t0 = System.nanoTime();
        recording.set(true);
        Thread.sleep(seconds * 1000L);
        long elapsedNanos = System.nanoTime() - t0;
        recording.set(false);
        stop.set(true);
        for (Thread t : threads) {
            t.join(3000);
        }

        long total = 0;
        for (LongList l : lists) {
            total += l.size();
        }
        long[] all = new long[(int) total];
        int p = 0;
        for (LongList l : lists) {
            long[] part = l.toArray();
            System.arraycopy(part, 0, all, p, part.length);
            p += part.length;
        }
        Arrays.sort(all);
        double secs = elapsedNanos / 1e9;
        long qps = Math.round(okWindow.get() / secs);
        long rps = Math.round(totalWindow.get() / secs);
        double p50 = pct(all, 50) / 1e6;
        double p90 = pct(all, 90) / 1e6;
        double p99 = pct(all, 99) / 1e6;
        double p999 = pct(all, 99.9) / 1e6;

        String json = String.format(
                "{\"label\":\"%s\",\"url\":\"%s\",\"concurrency\":%d,\"seconds\":%.2f,"
                        + "\"okInWindow\":%d,\"qps\":%d,\"rps\":%d,\"ok\":%d,\"soldOut\":%d,\"limited\":%d,\"errors\":%d,"
                        + "\"p50Ms\":%.3f,\"p90Ms\":%.3f,\"p99Ms\":%.3f,\"p999Ms\":%.3f}",
                label, url, concurrency, secs, okWindow.get(), qps, rps,
                ok.get(), soldOut.get(), limited.get(), errors.get(), p50, p90, p99, p999);

        System.out.println("[loadgen] " + json);
        if (out != null) {
            Files.writeString(Path.of(out), json + System.lineSeparator(), StandardCharsets.UTF_8);
        }
    }

    private static double pct(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.ceil(sorted.length * p / 100.0) - 1;
        idx = Math.max(0, Math.min(sorted.length - 1, idx));
        return sorted[idx];
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
