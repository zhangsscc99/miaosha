-- 支付。已经关掉的不能再付。已经付过的不改状态。
-- KEYS[1] 到期有序集合
-- KEYS[2] 订单状态哈希
-- ARGV[1] 订单 id
-- 返回 1 支付成功，2 已经付过，3 已经关掉，0 没有这张单
local id = ARGV[1]
local st = redis.call('HGET', KEYS[2], id)
if st == false then
    return 0
end
if st == 'PAID' then
    return 2
end
if st == 'CLOSED' then
    redis.call('ZREM', KEYS[1], id)
    return 3
end
redis.call('HSET', KEYS[2], id, 'PAID')
redis.call('ZREM', KEYS[1], id)
return 1
