package com.geekq.lab;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单机令牌桶：每 10ms 补充 rate/100 个令牌，桶容量为 burst。
 * 放在网关/服务入口，超出的请求快速失败（429），保护后面的 Redis / MQ / DB。
 */
public class RateLimiter implements AutoCloseable {

    private final int burst;
    private final int perTick;
    private final AtomicInteger tokens;
    private final ScheduledExecutorService scheduler;

    public RateLimiter(int ratePerSecond, int burst) {
        this.burst = burst;
        this.perTick = Math.max(1, ratePerSecond / 100);
        this.tokens = new AtomicInteger(burst);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "token-refill");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::refill, 10, 10, TimeUnit.MILLISECONDS);
    }

    private void refill() {
        while (true) {
            int cur = tokens.get();
            if (cur >= burst) {
                return;
            }
            int next = Math.min(burst, cur + perTick);
            if (tokens.compareAndSet(cur, next)) {
                return;
            }
        }
    }

    public boolean tryAcquire() {
        while (true) {
            int cur = tokens.get();
            if (cur <= 0) {
                return false;
            }
            if (tokens.compareAndSet(cur, cur - 1)) {
                return true;
            }
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
