-- 结束一张已经领走的订单。重复执行不会把库存加两次。
-- KEYS[1] 订单状态哈希
-- KEYS[2] 处理中列表
-- KEYS[3] 库存
-- ARGV[1] 订单 id
-- 返回 1 这次关掉并加库存，2 已经关掉，3 已经支付
local id = ARGV[1]
local st = redis.call('HGET', KEYS[1], id)
if st == 'CLOSED' then
    redis.call('LREM', KEYS[2], 1, id)
    return 2
end
if st == 'PAID' then
    redis.call('LREM', KEYS[2], 1, id)
    return 3
end
redis.call('HSET', KEYS[1], id, 'CLOSED')
redis.call('INCR', KEYS[3])
redis.call('LREM', KEYS[2], 1, id)
return 1
