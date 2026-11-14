package com.geekq.lab.bloom;

/**
 * 一个按 64 位 id 直接算偏移的布隆过滤器。
 *
 * 设计取舍：
 * - 偏移在客户端算，Redis 侧只负责 GETBIT/SETBIT，这样 Lua 脚本里不用做 64 位哈希
 *   （Lua 5.1 的 number 是 double，做 64 位乘法会掉精度）。
 * - 两个 64 位哈希 h1/h2，第 i 个位置用 h1 + i*h2。这是 Kirsch-Mitzenmacher 的
 *   双哈希构造，理论误判率和 k 个独立哈希一样。
 * - 位图用 long[]，只读阶段没有锁；写入阶段由调用方保证只有一个线程。
 */
public final class Bloom {

    private final long[] words;
    private final int m;      // 位数
    private final int k;      // 哈希个数
    private final int n;      // 预期元素个数

    public Bloom(int expectedKeys, int bitsPerKey) {
        this.n = expectedKeys;
        this.m = Math.max(64, expectedKeys * bitsPerKey);
        this.k = Math.max(1, (int) Math.round(Math.log(2) * bitsPerKey));
        this.words = new long[(m + 63) >>> 6];
    }

    public int bits() {
        return m;
    }

    public int hashes() {
        return k;
    }

    public int expectedKeys() {
        return n;
    }

    public long bytes() {
        return (long) words.length * 8L;
    }

    /** 理论误判率 (1 - e^(-kn/m))^k，用来和实测对照。 */
    public double theoryFalsePositive() {
        double e = Math.exp(-(double) k * n / m);
        return Math.pow(1 - e, k);
    }

    public long hash1(long id) {
        long x = id * 0x9E3779B97F4A7C15L + 0x165667B19E3779F9L;
        return fmix64(x);
    }

    public long hash2(long id) {
        long x = fmix64(id ^ 0xD6E8FEB86659FD93L);
        return x | 1L; // 保证步长是奇数，和 2 的幂大小的位图互质
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /** 把 id 的 k 个位偏移写进 out，返回 out。 */
    public int[] offsets(long id, int[] out) {
        long h1 = hash1(id);
        long h2 = hash2(id);
        for (int i = 0; i < k; i++) {
            long v = h1 + (long) i * h2;
            out[i] = (int) ((v & Long.MAX_VALUE) % m);
        }
        return out;
    }

    public void add(long id) {
        int[] off = new int[k];
        offsets(id, off);
        for (int i = 0; i < k; i++) {
            int o = off[i];
            words[o >>> 6] |= 1L << o;
        }
    }

    public boolean mightContain(long id) {
        long h1 = hash1(id);
        long h2 = hash2(id);
        for (int i = 0; i < k; i++) {
            long v = h1 + (long) i * h2;
            int o = (int) ((v & Long.MAX_VALUE) % m);
            if ((words[o >>> 6] & (1L << o)) == 0) {
                return false;
            }
        }
        return true;
    }

    /** 已经置 1 的位数，用来看填充率。 */
    public long cardinality() {
        long c = 0;
        for (long w : words) {
            c += Long.bitCount(w);
        }
        return c;
    }
}
