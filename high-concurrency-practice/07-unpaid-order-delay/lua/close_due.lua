-- 关掉一个已经到期的未支付订单，并把 1 件库存加回去。
-- 整段脚本是一次往返。ZRANGEBYSCORE 只看 score <= now，没到期的不会被弹出来。
-- KEYS[1] 到期有序集合，member 是订单 id，score 是截止时间（毫秒）
-- KEYS[2] 订单状态哈希
-- KEYS[3] 库存
-- ARGV[1] 当前时间（毫秒）
-- 返回值：正数是被关掉的订单 id；0 没有到期订单；-1 已经支付；-2 已经关掉
local r = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', '0', '1')
if r[1] == nil then
    return 0
end
local id = r[1]
local st = redis.call('HGET', KEYS[2], id)
redis.call('ZREM', KEYS[1], id)
if st == 'PAID' then
    return -1
end
if st == 'CLOSED' then
    return -2
end
redis.call('HSET', KEYS[2], id, 'CLOSED')
redis.call('INCR', KEYS[3])
return tonumber(id)
