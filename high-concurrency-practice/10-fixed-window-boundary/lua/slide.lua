-- 滑动窗口计数。上一窗的放行数按「这一窗还没过去的比例」留在公式里。
-- 请求如果堆在上一窗的末尾，比例下降之后，那些请求其实还在窗口内，公式却会再放行。
-- 拒绝不加计数。
-- KEYS[1] 前缀
-- ARGV[1] 上限
-- ARGV[2] 窗口毫秒
-- ARGV[3] now 毫秒，负数表示用 Redis TIME。0 是一个合法时间戳。
-- ARGV[4] TTL 毫秒，0 表示两个窗口

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
local slot = slot_of(now, window)
local elapsed = now - slot * window
local prev = tonumber(redis.call('GET', KEYS[1] .. ':' .. (slot - 1)) or '0')
local key = KEYS[1] .. ':' .. slot
local curr = tonumber(redis.call('GET', key) or '0')
-- 整数比较：prev * 剩余毫秒 + (curr+1) * 窗口 <= limit * 窗口
local numer = prev * (window - elapsed) + (curr + 1) * window
if numer > limit * window then
  return 0
end
redis.call('INCR', key)
redis.call('PEXPIRE', key, ttl)
return 1
