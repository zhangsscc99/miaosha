-- 库存和已购集合必须在同一个槽。Cluster 上靠 hash tag，不靠两个 key 的名字碰巧撞槽。
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return 2
end
local s = tonumber(redis.call('GET', KEYS[1]) or '-1')
if s <= 0 then
  return 0
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
