-- 令牌桶。每窗口补充量和容量分开。容量为 0 时等于补充量。
-- 内部用毫令牌，1000 毫令牌 = 放行 1 次。补充量用 floor，不产生小数。
-- 拒绝也把残余毫令牌写回去，否则下一次会把同一段时间再补一遍。
-- KEYS[1] 键
-- ARGV[1] 每个窗口补充多少次
-- ARGV[2] 窗口毫秒
-- ARGV[3] now 毫秒，负数表示用 Redis TIME。0 是一个合法时间戳。
-- ARGV[4] 容量，0 表示等于补充量

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local rate = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
if now < 0 then
  now = now_ms()
end
local capN = tonumber(ARGV[4])
if capN == nil or capN <= 0 then
  capN = rate
end
local scale = 1000
local cap = capN * scale
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = nil
local ts = nil
if data[1] then
  tokens = tonumber(data[1])
end
if data[2] then
  ts = tonumber(data[2])
end
if tokens == nil or ts == nil then
  tokens = cap
  ts = now
else
  local elapsed = now - ts
  if elapsed < 0 then
    elapsed = 0
  end
  if elapsed > window then
    elapsed = window
  end
  if elapsed > 0 then
    tokens = tokens + math.floor(elapsed * rate * scale / window)
    if tokens > cap then
      tokens = cap
    end
    ts = now
  end
end
if tokens < scale then
  redis.call('HSET', KEYS[1], 'tokens', string.format('%.0f', tokens), 'ts', string.format('%.0f', ts))
  redis.call('PEXPIRE', KEYS[1], window * 2)
  return 0
end
tokens = tokens - scale
redis.call('HSET', KEYS[1], 'tokens', string.format('%.0f', tokens), 'ts', string.format('%.0f', ts))
redis.call('PEXPIRE', KEYS[1], window * 2)
return 1
