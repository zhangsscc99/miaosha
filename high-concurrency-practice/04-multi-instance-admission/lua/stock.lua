-- 原子扣库存 + 一人一单。
-- KEYS[1] 库存（整数）
-- KEYS[2] 已成功用户集合
-- ARGV[1] 用户 id
-- 返回 0 售罄，1 成功，2 重复下单
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return 2
end
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil or stock <= 0 then
    return 0
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
