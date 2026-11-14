-- 固定窗口限流。测试时窗口 id 由调用方传入，生产环境把窗口换成 unix 秒。
-- 窗口边界允许最多两个窗口的量叠在一起，这是固定窗口的固有误差，不是实现写错。
-- KEYS[1] 限流 key
-- ARGV[1] 窗口内上限
-- ARGV[2] 过期毫秒
-- 返回 1 通过，0 被限流
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current + 1 > tonumber(ARGV[1]) then
    return 0
end
local n = redis.call('INCR', KEYS[1])
if n == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 1
