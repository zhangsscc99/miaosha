-- 按订单 id 关单。支付和关单抢同一单时，状态只改一次。
-- KEYS[1] 到期有序集合
-- KEYS[2] 订单状态哈希
-- KEYS[3] 库存
-- ARGV[1] 订单 id
-- 返回 1 这次关掉并加库存，2 已经支付，3 已经关掉，0 没有这张单
local id = ARGV[1]
local st = redis.call('HGET', KEYS[2], id)
if st == false then
    return 0
end
redis.call('ZREM', KEYS[1], id)
if st == 'PAID' then
    return 2
end
if st == 'CLOSED' then
    return 3
end
redis.call('HSET', KEYS[2], id, 'CLOSED')
redis.call('INCR', KEYS[3])
return 1
