-- 数一段毫秒里，未修正的 math.floor(now/window) 和修正后的窗口号差了几次。
-- ARGV[1] 窗口毫秒
-- ARGV[2] 起始 now（含）
-- ARGV[3] 结束 now（含）
-- 返回 bad,firstNow,checked。firstNow 没有时是 -1。
local window = tonumber(ARGV[1])
local from = tonumber(ARGV[2])
local to = tonumber(ARGV[3])
local bad = 0
local first = -1
local checked = 0
for now = from, to do
  checked = checked + 1
  local approx = math.floor(now / window)
  local slot = approx
  if slot * window > now then
    slot = slot - 1
  elseif (slot + 1) * window <= now then
    slot = slot + 1
  end
  if slot ~= approx then
    bad = bad + 1
    if first < 0 then
      first = now
    end
  end
end
return bad .. "," .. first .. "," .. checked
