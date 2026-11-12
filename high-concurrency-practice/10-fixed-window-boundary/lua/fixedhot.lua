-- 固定窗口的另一种写法：超限也 INCR。第 4 章的准入脚本是这种。
-- 放行次数仍然是每个窗口 limit 次，拒绝的请求会把计数继续往上推。
-- KEYS[1] 前缀
-- ARGV[1] 上限
-- ARGV[2] 窗口毫秒
-- ARGV[3] now 毫秒，负数表示用 Redis TIME。0 是一个合法时间戳。
-- ARGV[4] TTL 毫秒，0 表示两个窗口。只在计数从 0 变成 1 时设置。

local function slot_of(now, window)
  local slot = math.floor(now / window)
  if slot * window > now then
    slot = slot - 1
  elseif (slot + 1) * window <= now then
    slot = slot + 1
  end
  return slot
end

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
local ttl = tonumber(ARGV[4])
if ttl == nil or ttl <= 0 then
  ttl = window * 2
end
local key = KEYS[1] .. ':' .. slot_of(now, window)
local n = redis.call('INCR', key)
if n == 1 then
  redis.call('PEXPIRE', key, ttl)
end
if n > limit then
  return 0
end
return 1
