#!/usr/bin/env bash
# 用本机一个 64MB Redis 把第 1 章的场景跑完。不要带生产流量。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-redis-6379 /tmp/miaosha-redis-6380

stop_redis() {
  redis-cli -p 6379 shutdown nosave >/dev/null 2>&1 || true
  redis-cli -p 6380 shutdown nosave >/dev/null 2>&1 || true
  sleep 0.2
}

wait_ping() {
  local port="$1"
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.2
  done
  echo "redis :$port did not start" >&2
  return 1
}

stop_redis
redis-server "$PWD/redis-64mb.conf"
wait_ping 6379

go build -o /tmp/miaosha-stockbench .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$(redis-server --version)"
  echo "nproc=$(nproc)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  echo "--- redis 64mb ---"
  redis-cli INFO server | awk -F: '/redis_version|process_id|tcp_port|os/'
  redis-cli CONFIG GET maxmemory
  redis-cli CONFIG GET appendonly
} > results/env.txt

run() {
  local name="$1"
  shift
  echo "=== ${name} ==="
  /tmp/miaosha-stockbench -lua "$PWD/lua" -scenario "$name" -out "results/${name}.json" "$@"
}

run unsafe-correct -workers 64 -stock 100 -users 400 -repeat 10
run lua-correct -workers 64 -stock 100 -users 400 -repeat 10
run lock-correct -workers 64 -stock 100 -users 400 -repeat 10
run ratelimit -workers 32 -attempts 4000 -limit 100
run local-shed -workers 32 -attempts 20000 -stock 100

run lua-tps -workers 32 -duration 4s -warmup 1s -stock 5000000
/tmp/miaosha-stockbench -lua "$PWD/lua" -scenario lua-tps -workers 16 -duration 4s -warmup 1s -stock 5000000 -out results/lua-tps-w16.json
run lua-soldout-tps -workers 16 -duration 4s -warmup 1s
# 16 worker 的结果会被下面的同名场景覆盖，先改存。
mv results/lua-soldout-tps.json results/lua-soldout-tps-w16.json
run lua-soldout-tps -workers 32 -duration 4s -warmup 1s
cp results/lua-soldout-tps.json results/lua-soldout-tps-w32.json
run lua-soldout-tps -workers 128 -duration 4s -warmup 1s
cp results/lua-soldout-tps.json results/lua-soldout-tps-w128.json
mv results/lua-soldout-tps-w32.json results/lua-soldout-tps.json

run lock-tps -workers 32 -duration 4s -warmup 1s -stock 5000000
run mem-tps -workers 32 -duration 2s -warmup 200ms -stock 200000
run local-tps -workers 32 -duration 2s -warmup 200ms
/tmp/miaosha-stockbench -lua "$PWD/lua" -scenario http-local -workers 32 -duration 4s -warmup 1s -out results/http-local-w32.json
/tmp/miaosha-stockbench -lua "$PWD/lua" -scenario http-local -workers 64 -duration 4s -warmup 1s -out results/http-local.json
/tmp/miaosha-stockbench -lua "$PWD/lua" -scenario http-redis -workers 32 -duration 4s -warmup 1s -out results/http-redis.json
run shard-one-soldout -workers 32 -duration 4s -warmup 1s
mv results/shard-one-soldout.json results/shard-one-soldout-64mb.json

echo "=== aof everysec ==="
redis-cli CONFIG SET appendonly yes >/dev/null
redis-cli CONFIG SET appendfsync everysec >/dev/null
/tmp/miaosha-stockbench -lua "$PWD/lua" -scenario lua-tps -workers 32 -duration 4s -warmup 1s -stock 5000000 -out results/lua-tps-aof.json
/tmp/miaosha-stockbench -lua "$PWD/lua" -scenario lua-soldout-tps -workers 32 -duration 4s -warmup 1s -out results/lua-soldout-aof.json
redis-cli CONFIG SET appendonly no >/dev/null

echo "=== two redis processes, 32MB + 32MB ==="
stop_redis
redis-server "$PWD/redis-32mb-6379.conf"
redis-server "$PWD/redis-32mb-6380.conf"
wait_ping 6379
wait_ping 6380
run shard-one-soldout -workers 32 -duration 4s -warmup 1s
mv results/shard-one-soldout.json results/shard-one-soldout-32mb.json
run shard-two-soldout -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 32 -duration 4s -warmup 1s

echo "=== sqlite hot row ==="
python3 sqlite_hotrow.py --sync OFF --threads 8 --stock 500 --attempts 8000 --out results/sqlite-off.json
python3 sqlite_hotrow.py --sync NORMAL --threads 8 --stock 500 --attempts 8000 --out results/sqlite-normal.json
python3 sqlite_hotrow.py --sync FULL --threads 8 --stock 500 --attempts 4000 --out results/sqlite-full.json

stop_redis
echo "ALL PASS"
