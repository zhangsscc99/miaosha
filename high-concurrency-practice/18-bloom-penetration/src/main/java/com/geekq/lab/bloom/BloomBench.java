package com.geekq.lab.bloom;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 布隆过滤器本身的物理参数：位/元素、哈希个数、实际误判率、内存、构建时间、查询吞吐。
 *
 * 这一组不碰 Redis、不碰 MySQL，只回答「布隆这一层自己有多贵、有多准」。
 */
public final class BloomBench {

    static final int[] BITS_PER_KEY = {7, 10, 14};
    static final int[] SIZES = {100_000, 1_000_000, 10_000_000};
    static final int FP_QUERIES = 1_000_000;

    public static void main(String[] args) throws Exception {
        String out = arg(args, "--out", "results/bloom-params.json");
        StringBuilder sb = new StringBuilder();
        sb.append("{\"date\":\"").append(java.time.Instant.now()).append("\",\n");
        sb.append("\"jvm\":\"").append(System.getProperty("java.version")).append("\",\n");
        sb.append("\"configs\":[\n");

        List<String> rows = new ArrayList<>();
        for (int n : SIZES) {
            for (int bpk : BITS_PER_KEY) {
                // 1000 万 × 14 位 = 17.5MB，可以跑；但三档都跑会让这一节太慢，只跑 10 位
                if (n == 10_000_000 && bpk != 10) {
                    continue;
                }
                rows.add(runOne(n, bpk));
            }
        }
        sb.append(String.join(",\n", rows));
        sb.append("\n]");

        // 单线程查询：拿 100 万那档，5,000,000 次
        Bloom single = new Bloom(1_000_000, 10);
        for (int i = 0; i < 1_000_000; i++) {
            single.add(1_000_000L + i);
        }
        long hits = 0;
        long t0 = System.nanoTime();
        for (int i = 0; i < 5_000_000; i++) {
            if (single.mightContain(9_000_000_000L + i)) {
                hits++;
            }
        }
        long ns = System.nanoTime() - t0;
        sb.append(",\n\"singleThread\":{\"lookups\":5000000,\"nsPerLookup\":")
                .append(String.format("%.2f", (double) ns / 5_000_000))
                .append(",\"falsePositives\":").append(hits).append("}");

        // 8 线程并发查询：布隆是只读位图，多核应该能线性放大
        AtomicLong total = new AtomicLong();
        int threads = 8;
        int perThread = 2_000_000;
        t0 = System.nanoTime();
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            ts[t] = new Thread(() -> {
                Bloom b = single;
                long c = 0;
                long base = 20_000_000_000L + (long) tid * perThread;
                for (int i = 0; i < perThread; i++) {
                    if (b.mightContain(base + i)) {
                        c++;
                    }
                }
                total.addAndGet(c);
            });
            ts[t].start();
        }
        for (Thread t : ts) {
            t.join();
        }
        double seconds = (System.nanoTime() - t0) / 1e9;
        sb.append(",\n\"multiThread\":{\"threads\":").append(threads)
                .append(",\"lookups\":").append((long) threads * perThread)
                .append(",\"seconds\":").append(String.format("%.3f", seconds))
                .append(",\"opsPerSec\":").append(String.format("%.0f", threads * (double) perThread / seconds))
                .append(",\"falsePositives\":").append(total.get()).append("}");

        // 重建一版 1000 万商品的全量布隆要多久（商品下架不能从布隆里删，只能重建或计数布隆）
        int n = 10_000_000;
        t0 = System.nanoTime();
        Bloom rebuild = new Bloom(n, 10);
        for (int i = 0; i < n; i++) {
            rebuild.add(i);
        }
        double rebuildSeconds = (System.nanoTime() - t0) / 1e9;
        sb.append(",\n\"rebuild\":{\"keys\":").append(n)
                .append(",\"seconds\":").append(String.format("%.3f", rebuildSeconds))
                .append(",\"bytes\":").append(rebuild.bytes())
                .append(",\"bitsSet\":").append(rebuild.cardinality()).append("}");

        sb.append("\n}\n");
        Files.createDirectories(Path.of(out).getParent());
        Files.writeString(Path.of(out), sb.toString());
        System.out.println(sb);
    }

    static String runOne(int n, int bpk) {
        Bloom b = new Bloom(n, bpk);
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            b.add(1_000_000L + i);
        }
        double insertMs = (System.nanoTime() - t0) / 1e6;

        int fp = 0;
        long q0 = System.nanoTime();
        for (int i = 0; i < FP_QUERIES; i++) {
            if (b.mightContain(8_000_000_000L + i)) {
                fp++;
            }
        }
        double queryMs = (System.nanoTime() - q0) / 1e6;

        return "{\"keys\":" + n
                + ",\"bitsPerKey\":" + bpk
                + ",\"bits\":" + b.bits()
                + ",\"hashes\":" + b.hashes()
                + ",\"bytes\":" + b.bytes()
                + ",\"bytesPerKey\":" + String.format("%.2f", (double) b.bytes() / n)
                + ",\"insertMs\":" + String.format("%.1f", insertMs)
                + ",\"fpQueries\":" + FP_QUERIES
                + ",\"fpFalsePositives\":" + fp
                + ",\"fpRateMeasured\":" + String.format("%.6f", (double) fp / FP_QUERIES)
                + ",\"fpRateTheory\":" + String.format("%.6f", b.theoryFalsePositive())
                + ",\"fpQueryMs\":" + String.format("%.1f", queryMs)
                + ",\"bitsSet\":" + b.cardinality()
                + "}";
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
