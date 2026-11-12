-- 滑动日志。分数落在 (now-window, now] 里的成员数不超过上限。
-- 成员是递增序号，同一毫秒的多次放行不会互相覆盖。
-- KEYS[1] 前缀
-- ARGV[1] 上限
-- ARGV[2] 窗口毫秒
-- ARGV[3] now 毫秒，负数表示用 Redis TIME。0 是一个合法时间戳。
-- ARGV[4] 不使用
-- 返回 1 放行，0 拒绝

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
if now < 0 then
  now = now_ms()
end
local key = KEYS[1]
local seqKey = key .. ':seq'
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local n = tonumber(redis.call('ZCARD', key))
if n >= limit then
  redis.call('PEXPIRE', key, window * 2)
  return 0
end
local seq = redis.call('INCR', seqKey)
redis.call('ZADD', key, now, seq)
redis.call('PEXPIRE', key, window * 2)
redis.call('PEXPIRE', seqKey, window * 2)
return 1
