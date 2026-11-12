-- 固定窗口，对应 RedisLimitRateWithLUA 里那段脚本。
-- 先看本窗口的计数，没超再 INCR，每次放行都刷新 TTL。拒绝不加计数。
-- 窗口号是 floor(now / window)。相邻两个窗口各有一份额度。
-- KEYS[1] 前缀
-- ARGV[1] 上限
-- ARGV[2] 窗口毫秒
-- ARGV[3] now 毫秒，负数表示用 Redis TIME。0 是一个合法时间戳。
-- ARGV[4] TTL 毫秒，0 表示两个窗口
-- 返回 1 放行，0 拒绝

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
local current = tonumber(redis.call('GET', key) or '0')
if current + 1 > limit then
  return 0
end
redis.call('INCR', key)
redis.call('PEXPIRE', key, ttl)
return 1
