#!/usr/bin/env bash
# 用两个很小的 Redis 进程压成功扣减。不要对已经有数据的实例运行，脚本会 FLUSHDB。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-redis-6379 /tmp/miaosha-redis-6380

if [[ -n "${REDIS_SERVER:-}" ]]; then
  REDIS_SERVER="$(command -v "$REDIS_SERVER" || true)"
elif command -v redis-server >/dev/null 2>&1; then
  REDIS_SERVER="$(command -v redis-server)"
elif [[ -x /tmp/redis-7015/bin/redis-server ]]; then
  REDIS_SERVER=/tmp/redis-7015/bin/redis-server
else
  echo "redis-server not found; build 7.0.15 into /tmp/redis-7015 or set REDIS_SERVER" >&2
  exit 1
fi
REDIS_CLI="${REDIS_CLI:-$(dirname "$REDIS_SERVER")/redis-cli}"
if [[ ! -x "$REDIS_CLI" ]]; then
  echo "redis-cli not found next to $REDIS_SERVER" >&2
  exit 1
fi

stop_redis() {
  "$REDIS_CLI" -p 6379 shutdown nosave >/dev/null 2>&1 || true
  "$REDIS_CLI" -p 6380 shutdown nosave >/dev/null 2>&1 || true
  sleep 0.2
}

wait_ping() {
  local port="$1"
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if "$REDIS_CLI" -p "$port" ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.2
  done
  echo "redis :$port did not start" >&2
  return 1
}

start_one() {
  local conf="$1"
  shift
  stop_redis
  "$@" "$REDIS_SERVER" "$PWD/$conf"
  wait_ping 6379
}

start_two() {
  local conf_a="$1"
  local conf_b="$2"
  shift 2
  stop_redis
  "$@" "$REDIS_SERVER" "$PWD/$conf_a"
  "$@" "$REDIS_SERVER" "$PWD/$conf_b"
  wait_ping 6379
  wait_ping 6380
}

trap stop_redis EXIT
stop_redis

go build -o /tmp/miaosha-stockshard .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
} > results/env.txt

run() {
  local name="$1"
  shift
  echo "=== ${name} ==="
  /tmp/miaosha-stockshard -lua "$PWD/lua" -scenario "$name" -out "results/${name}.json" "$@"
}

start_one redis-64mb.conf
"$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|tcp_port|process_id|os/' >> results/env.txt
"$REDIS_CLI" -p 6379 CONFIG GET maxmemory >> results/env.txt
"$REDIS_CLI" -p 6379 CONFIG GET appendonly >> results/env.txt

run one-tps -workers 16 -duration 4s -warmup 1s -stock 5000000
mv results/one-tps.json results/one-tps-w16.json
run one-tps -workers 32 -duration 4s -warmup 1s -stock 5000000
mv results/one-tps.json results/one-tps-w32.json
run two-keys-tps -workers 16 -duration 4s -warmup 1s -stock 5000000
mv results/two-keys-tps.json results/two-keys-w16.json
run drain-one -workers 32 -stock 200000

echo "=== two processes, 64MB + 64MB ==="
start_two redis-64mb-6379.conf redis-64mb-6380.conf
TWO=127.0.0.1:6379,127.0.0.1:6380
run hash-correct -addrs "$TWO" -workers 32 -users 400 -repeat 10
run hash-skew -addrs "$TWO" -workers 32 -users 400 -repeat 10
run hash-unfair -addrs "$TWO" -workers 32 -users 400 -repeat 10
run fallback-dup -addrs "$TWO" -users 100
run rebalance-gap -addrs "$TWO" -stock 50
run rebalance-unsafe -addrs "$TWO" -workers 16 -stock 200000
run rebalance-lua -addrs "$TWO" -workers 16 -stock 200000
run two-tps -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 32 -duration 4s -warmup 1s -stock 8000000
mv results/two-tps.json results/two-64-w32.json
run two-tps -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 16 -duration 4s -warmup 1s -stock 8000000
mv results/two-tps.json results/two-64-w16.json
run drain-two -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 32 -stock 200000

echo "=== aof everysec on both processes ==="
"$REDIS_CLI" -p 6379 CONFIG SET appendonly yes >/dev/null
"$REDIS_CLI" -p 6379 CONFIG SET appendfsync everysec >/dev/null
"$REDIS_CLI" -p 6380 CONFIG SET appendonly yes >/dev/null
"$REDIS_CLI" -p 6380 CONFIG SET appendfsync everysec >/dev/null
sleep 0.3
run two-tps -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 32 -duration 4s -warmup 1s -stock 8000000
mv results/two-tps.json results/two-64-aof.json

echo "=== two processes, 32MB + 32MB, shorter window so the winner set fits ==="
start_two redis-32mb-6379.conf redis-32mb-6380.conf
run two-tps -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 32 -duration 2s -warmup 300ms -stock 4000000
mv results/two-tps.json results/two-32.json

echo "=== pin redis to cpu 0 and 1, bench to cpu 2,3 ==="
stop_redis
taskset -c 0 "$REDIS_SERVER" "$PWD/redis-64mb-6379.conf"
taskset -c 1 "$REDIS_SERVER" "$PWD/redis-64mb-6380.conf"
wait_ping 6379
wait_ping 6380
echo "=== two-tps-pin ==="
taskset -c 2,3 /tmp/miaosha-stockshard -lua "$PWD/lua" -scenario two-tps \
  -addrs 127.0.0.1:6379,127.0.0.1:6380 -workers 32 -duration 4s -warmup 1s -stock 8000000 \
  -out results/two-64-pin.json

stop_redis
echo "ALL PASS"
