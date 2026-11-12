-- 返回修正后的窗口号，用来和第 10 章 Go 里的整数除法对照。
-- ARGV[1] now 毫秒
-- ARGV[2] 窗口毫秒
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local slot = math.floor(now / window)
if slot * window > now then
  slot = slot - 1
elseif (slot + 1) * window <= now then
  slot = slot + 1
end
return slot
