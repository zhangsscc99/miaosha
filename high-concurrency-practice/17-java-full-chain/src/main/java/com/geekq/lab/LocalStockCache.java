package com.geekq.lab;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内库存缓存（L1）：把库存按 16 个分段计数，降低 CAS 竞争；售罄后置只读标记，
 * 后续请求在 JVM 内直接返回，不再访问 Redis。
 *
 * 生产上单个 Pod 只持有总库存的一个配额（例如 N 个 Pod 各持有 stock/N），
 * 卖完即"售罄标记"，剩余库存回收到 Redis 供其它 Pod 继续售卖。
 */
public class LocalStockCache {

    private static final int SEGMENTS = 16;
    private final AtomicInteger[] segments = new AtomicInteger[SEGMENTS];
    private final AtomicLong deductions = new AtomicLong();
    private volatile boolean soldOut;

    public LocalStockCache() {
        for (int i = 0; i < SEGMENTS; i++) {
            segments[i] = new AtomicInteger(0);
        }
    }

    public void reset(int stock) {
        int base = stock / SEGMENTS;
        int rest = stock % SEGMENTS;
        for (int i = 0; i < SEGMENTS; i++) {
            segments[i].set(base + (i < rest ? 1 : 0));
        }
        soldOut = stock <= 0;
        deductions.set(0);
    }

    /** @return true 表示本机成功占用一件库存。 */
    public boolean tryDeduct() {
        if (soldOut) {
            return false;
        }
        int start = (int) (Thread.currentThread().threadId() & 0x7fffffff) % SEGMENTS;
        for (int k = 0; k < SEGMENTS; k++) {
            AtomicInteger seg = segments[(start + k) % SEGMENTS];
            while (true) {
                int v = seg.get();
                if (v <= 0) {
                    break;
                }
                if (seg.compareAndSet(v, v - 1)) {
                    deductions.incrementAndGet();
                    return true;
                }
            }
        }
        soldOut = true;
        return false;
    }

    public void giveBack() {
        segments[0].incrementAndGet();
        soldOut = false;
    }

    /** 从 Redis 批量领取到本地时使用。 */
    public void addStock(int n) {
        if (n <= 0) {
            return;
        }
        int base = n / SEGMENTS;
        int rest = n % SEGMENTS;
        for (int i = 0; i < SEGMENTS; i++) {
            segments[i].addAndGet(base + (i < rest ? 1 : 0));
        }
        soldOut = false;
    }

    public int remaining() {
        int sum = 0;
        for (AtomicInteger s : segments) {
            sum += s.get();
        }
        return sum;
    }

    public boolean isSoldOut() {
        return soldOut;
    }

    public long deductions() {
        return deductions.get();
    }
}
