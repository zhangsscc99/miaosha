-- 共享布隆的「写」：把 k 个位置 1。
-- 一次 EVALSHA 写 k 位，比客户端发 k 次 SETBIT 少 k-1 次往返。
local key = KEYS[1]
for i = 1, #ARGV do
  redis.call('SETBIT', key, ARGV[i], 1)
end
return 1
