package com.geekq.lab;

/**
 * 多 Pod 版库存：本地缓存只持有"批发"来的一个 chunk，卖完再向 Redis 领下一批。
 * 一次 Redis 往返服务 chunk 个订单，把 Redis QPS 压到 订单QPS/chunk。
 */
public class ChunkedStock {

    private final RedisStock redis;
    private final LocalStockCache cache;
    private final long goodsId;
    private final int chunk;
    private final Object refillLock = new Object();
    private volatile boolean soldOut;

    public ChunkedStock(RedisStock redis, LocalStockCache cache, long goodsId, int chunk) {
        this.redis = redis;
        this.cache = cache;
        this.goodsId = goodsId;
        this.chunk = chunk;
    }

    public boolean tryDeduct() {
        for (int attempt = 0; attempt < 4; attempt++) {
            if (cache.tryDeduct()) {
                return true;
            }
            if (soldOut) {
                return false;
            }
            synchronized (refillLock) {
                if (cache.remaining() > 0) {
                    continue;
                }
                if (soldOut) {
                    return false;
                }
                int got = redis.refill(goodsId, chunk);
                if (got <= 0) {
                    soldOut = true;
                    return false;
                }
                cache.addStock(got);
            }
        }
        return cache.tryDeduct();
    }

    public void giveBack() {
        // 入队失败的回滚：退回本地即可，活动结束后由对账任务处理差值
        cache.giveBack();
        soldOut = false;
    }

    public int remaining() {
        return cache.remaining();
    }

    public boolean isSoldOut() {
        return soldOut;
    }
}
