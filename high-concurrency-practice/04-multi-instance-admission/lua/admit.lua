-- 固定窗口准入。每个请求都要打到 Redis，所以它挡不住把单核打满。
-- 用来和进程内额度对照：限流放在 Redis 里省不了命令。
-- KEYS[1] 计数器
-- ARGV[1] 窗口内允许的次数
-- ARGV[2] 窗口毫秒
-- 返回 1 放行，0 拒绝
local n = redis.call('INCR', KEYS[1])
if n == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if n > tonumber(ARGV[1]) then
    return 0
end
return 1
