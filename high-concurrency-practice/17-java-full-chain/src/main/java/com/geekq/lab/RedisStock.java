package com.geekq.lab;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.List;

/**
 * Redis 库存扣减。核心是 Lua：读取 + 判断 + 扣减在 Redis 单线程内原子完成，
 * 不需要分布式锁，也不会超卖。
 */
public class RedisStock implements AutoCloseable {

    private static final String DEDUCT_LUA =
            "local s = tonumber(redis.call('get', KEYS[1])) "
                    + "if (not s) or s <= 0 then return -1 end "
                    + "return redis.call('decr', KEYS[1])";

    /** 批量领取：一次从 Redis 领走最多 ARGV[1] 件，返回真正领到的数量（0 表示售罄）。 */
    private static final String REFILL_LUA =
            "local s = tonumber(redis.call('get', KEYS[1])) or 0 "
                    + "local take = math.min(tonumber(ARGV[1]), s) "
                    + "if take <= 0 then return 0 end "
                    + "redis.call('decrby', KEYS[1], take) "
                    + "return take";

    /** 释放锁：只有 token 匹配才删除，避免误删别人的锁。 */
    private static final String UNLOCK_LUA =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    private final JedisPool pool;

    public RedisStock() {
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(Integer.getInteger("lab.redis.pool", 64));
        cfg.setMaxIdle(64);
        cfg.setMinIdle(16);
        pool = new JedisPool(cfg,
                System.getProperty("lab.redis.host", "127.0.0.1"),
                Integer.getInteger("lab.redis.port", 6379),
                2000);
    }

    public static String stockKey(long goodsId) {
        return "hc:stock:" + goodsId;
    }

    public static String lockKey(long goodsId) {
        return "hc:lock:" + goodsId;
    }

    /** @return 扣减后的剩余库存；-1 表示已售罄。 */
    public long deduct(long goodsId) {
        try (Jedis j = pool.getResource()) {
            Object r = j.eval(DEDUCT_LUA, List.of(stockKey(goodsId)), Collections.emptyList());
            return (Long) r;
        }
    }

    public void giveBack(long goodsId) {
        try (Jedis j = pool.getResource()) {
            j.incr(stockKey(goodsId));
        }
    }

    /** 多 Pod 场景：每个 Pod 一次领取 chunk 件到本地，领完再领，避免每单一次 Redis 往返。 */
    public int refill(long goodsId, int chunk) {
        try (Jedis j = pool.getResource()) {
            Object r = j.eval(REFILL_LUA, List.of(stockKey(goodsId)), List.of(String.valueOf(chunk)));
            return ((Long) r).intValue();
        }
    }

    public void setStock(long goodsId, int stock) {
        try (Jedis j = pool.getResource()) {
            j.set(stockKey(goodsId), String.valueOf(stock));
        }
    }

    public int getStock(long goodsId) {
        try (Jedis j = pool.getResource()) {
            String v = j.get(stockKey(goodsId));
            return v == null ? -1 : Integer.parseInt(v);
        }
    }

    public boolean tryLock(long goodsId, String token, int ttlMs) {
        try (Jedis j = pool.getResource()) {
            return "OK".equals(j.set(lockKey(goodsId), token, redis.clients.jedis.params.SetParams.setParams()
                    .nx().px(ttlMs)));
        }
    }

    public void unlock(long goodsId, String token) {
        try (Jedis j = pool.getResource()) {
            j.eval(UNLOCK_LUA, List.of(lockKey(goodsId)), List.of(token));
        }
    }

    /** 压测微基准用：不经过连接池，单连接直连。 */
    public Jedis rawConnection() {
        return new Jedis(System.getProperty("lab.redis.host", "127.0.0.1"),
                Integer.getInteger("lab.redis.port", 6379), 2000);
    }

    @Override
    public void close() {
        pool.close();
    }
}
