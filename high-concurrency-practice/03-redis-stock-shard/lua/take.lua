-- 把这个桶里剩下的库存一次性取走，留给另一个进程。
-- 和扣减脚本在同一条 Redis 线程上串行，所以不会把正在卖的库存算两次。
-- KEYS[1] 库存
-- 返回取走的件数。进程在返回之后、另一个进程 INCRBY 之前崩溃，这批库存就丢了。
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil or stock <= 0 then
    return 0
end
redis.call('SET', KEYS[1], '0')
return stock
