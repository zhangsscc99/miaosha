package com.geekq.lab.bloom;

import java.util.Arrays;

/** 微秒直方图，p50/p99/max 都是桶上界。 */
public final class Hist {

    private final long[] buckets;
    private long count;
    private long sum;

    public Hist() {
        this.buckets = new long[1 << 20];
    }

    public void record(long micros) {
        int i = micros < 0 ? 0 : (int) Math.min(micros, buckets.length - 1);
        buckets[i]++;
        count++;
        sum += micros;
    }

    public long count() {
        return count;
    }

    public double mean() {
        return count == 0 ? 0 : (double) sum / count;
    }

    public long percentile(double p) {
        if (count == 0) {
            return 0;
        }
        long target = (long) Math.ceil(p * count);
        long acc = 0;
        for (int i = 0; i < buckets.length; i++) {
            acc += buckets[i];
            if (acc >= target) {
                return i;
            }
        }
        return buckets.length - 1;
    }

    public long max() {
        for (int i = buckets.length - 1; i >= 0; i--) {
            if (buckets[i] > 0) {
                return i;
            }
        }
        return 0;
    }

    public String json() {
        return "{\"count\":" + count
                + ",\"meanUs\":" + String.format("%.1f", mean())
                + ",\"p50Us\":" + percentile(0.50)
                + ",\"p99Us\":" + percentile(0.99)
                + ",\"maxUs\":" + max() + "}";
    }

    public static long[] sorted(long[] a) {
        long[] b = Arrays.copyOf(a, a.length);
        Arrays.sort(b);
        return b;
    }

    public void mergeFrom(Hist other) {
        long[] src = other.buckets;
        for (int i = 0; i < src.length; i++) {
            if (src[i] != 0) {
                buckets[i] += src[i];
            }
        }
        count += other.count;
        sum += other.sum;
    }
}
