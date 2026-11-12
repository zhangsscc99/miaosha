-- 比较并删除，避免把别人的锁删掉。
-- 旧仓库 luaLockScript.lua 把判断写进了 redis.call 的参数里，而且用了 KEY/AVG，脚本本身跑不起来。
-- KEYS[1] 锁
-- ARGV[1] 持有者 token
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
