-- 只把到期订单位从有序集合搬到处理中列表，先不加库存。
-- 进程在这之后停掉的话，订单还在列表里，恢复时可以再执行 finish。
-- KEYS[1] 到期有序集合
-- KEYS[2] 处理中列表
-- ARGV[1] 当前时间（毫秒）
-- 返回正数订单 id，0 表示没有到期订单
local r = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', '0', '1')
if r[1] == nil then
    return 0
end
local id = r[1]
redis.call('ZREM', KEYS[1], id)
redis.call('LPUSH', KEYS[2], id)
return tonumber(id)
