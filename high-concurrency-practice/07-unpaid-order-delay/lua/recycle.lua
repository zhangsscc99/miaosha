-- 压测用。做完 ZREM、HSET、INCR，再把同一张单放回未支付，基数保持不变。
-- 比 close_due 多一次 HSET 和一次 ZADD。多出来的写没有让它更慢，数字不能代替生产脚本那一组。
-- KEYS[1] 到期有序集合
-- KEYS[2] 订单状态哈希
-- KEYS[3] 库存
-- ARGV[1] 当前时间（毫秒）
local r = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', '0', '1')
if r[1] == nil then
    return 0
end
local id = r[1]
redis.call('ZREM', KEYS[1], id)
redis.call('HSET', KEYS[2], id, 'CLOSED')
redis.call('INCR', KEYS[3])
redis.call('HSET', KEYS[2], id, 'UNPAID')
redis.call('ZADD', KEYS[1], 0, id)
return 1
