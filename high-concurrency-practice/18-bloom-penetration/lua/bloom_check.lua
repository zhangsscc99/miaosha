-- 共享布隆的「查」：KEYS[1] 是位图，ARGV 是客户端算好的 k 个位偏移。
-- 偏移在客户端算，是因为 Lua 5.1 的 number 是 double，做 64 位哈希会掉精度。
-- 任意一位是 0 就返回 0（一定不存在）；全为 1 返回 1（可能存在，约 0.8% 是误判）。
local key = KEYS[1]
for i = 1, #ARGV do
  if redis.call('GETBIT', key, ARGV[i]) == 0 then
    return 0
  end
end
return 1
