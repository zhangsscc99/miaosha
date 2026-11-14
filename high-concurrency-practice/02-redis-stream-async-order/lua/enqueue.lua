-- 原子：一人一单 + 扣库存 + 入队。
-- 三段写在同一次 Lua 里。脚本失败时 Redis 会丢掉这次写，不会出现「库存没了、队列里没有这个人」。
-- KEYS[1] 库存
-- KEYS[2] 已成功用户
-- KEYS[3] 订单流
-- ARGV[1] 用户 id
-- ARGV[2] 商品 id
-- ARGV[3] 近似最大长度，0 表示不裁剪。压测用它挡住内存，正确性测试传 0。
-- 返回 0 售罄，1 成功并入队，2 重复
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return 2
end
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil or stock <= 0 then
    return 0
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
local cap = tonumber(ARGV[3])
if cap ~= nil and cap > 0 then
    redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', 'u', ARGV[1], 'g', ARGV[2])
else
    redis.call('XADD', KEYS[3], '*', 'u', ARGV[1], 'g', ARGV[2])
end
return 1
