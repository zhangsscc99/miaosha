-- 售罄热路径：只读库存。库存已经是 0 时，脚本在第一次 GET 后返回。
-- KEYS[1] 库存
-- 返回 0 售罄，1 仍有库存（本场景用于压测售罄，正常应一直是 0）
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil or stock <= 0 then
    return 0
end
return 1
