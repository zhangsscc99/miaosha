#!/usr/bin/env bash
# 同机两个或三个 Redis 7.0.15 进程。会关掉本机 6379、6380、6381 上的 Redis。不要对有数据的实例运行。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results \
  /tmp/miaosha-redis-repl-6379 \
  /tmp/miaosha-redis-repl-6380 \
  /tmp/miaosha-redis-repl-6381

if [[ ! -x /tmp/redis-7015/bin/redis-server ]]; then
  if [[ ! -d /tmp/redis-7.0.15 ]]; then
    curl -fsSL -o /tmp/redis-7.0.15.tar.gz https://download.redis.io/releases/redis-7.0.15.tar.gz
    tar -C /tmp -xzf /tmp/redis-7.0.15.tar.gz
  fi
  make -C /tmp/redis-7.0.15 -j"$(nproc)" PREFIX=/tmp/redis-7015 install
fi
REDIS_SERVER=/tmp/redis-7015/bin/redis-server
REDIS_CLI=/tmp/redis-7015/bin/redis-cli

stop_redis() {
  "$REDIS_CLI" -p 6379 shutdown nosave >/dev/null 2>&1 || true
  "$REDIS_CLI" -p 6380 shutdown nosave >/dev/null 2>&1 || true
  "$REDIS_CLI" -p 6381 shutdown nosave >/dev/null 2>&1 || true
  sleep 0.3
}

wait_ping() {
  local port="$1"
  local i
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25; do
    if "$REDIS_CLI" -p "$port" ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.2
  done
  echo "redis :$port did not start" >&2
  cat "/tmp/miaosha-redis-repl-${port}.log" >&2 || true
  return 1
}

start_master() {
  "$REDIS_SERVER" "$PWD/redis-master.conf"
  wait_ping 6379
}

start_master_aof() {
  "$REDIS_SERVER" "$PWD/redis-master-aof.conf"
  wait_ping 6379
}

start_replica() {
  "$REDIS_SERVER" "$PWD/redis-replica.conf"
  wait_ping 6380
}

start_replica2() {
  "$REDIS_SERVER" "$PWD/redis-replica2.conf"
  wait_ping 6381
}

fresh_dirs() {
  rm -rf /tmp/miaosha-redis-repl-6379 /tmp/miaosha-redis-repl-6380 /tmp/miaosha-redis-repl-6381
  mkdir -p /tmp/miaosha-redis-repl-6379 /tmp/miaosha-redis-repl-6380 /tmp/miaosha-redis-repl-6381
}

fresh_master_dir() {
  rm -rf /tmp/miaosha-redis-repl-6379
  mkdir -p /tmp/miaosha-redis-repl-6379
}

trap stop_redis EXIT
stop_redis
fresh_dirs

go build -o /tmp/miaosha-replica .

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
  local scenario="$1"
  local out="$2"
  shift 2
  echo "=== ${out} ==="
  /tmp/miaosha-replica -scenario "$scenario" -out "results/${out}.json" "$@"
}

start_master
start_replica
start_replica2
"$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|process_id|os|gcc_version|tcp_port/' >> results/env.txt
"$REDIS_CLI" -p 6379 INFO memory | awk -F: '/mem_allocator|maxmemory/' >> results/env.txt

run ready ready
run copy copy
run readonly readonly
run wait-ok wait-ok
run wait-down wait-down
run stale-data stale-data
run stale stale -keys 10000
run min-one min-one
run min-two min-two
run lag lag
run partial partial
run full full
run promote promote
run gap gap
run delay5 delay5
run memory memory
run sync sync-diskless -from diskless
run sync sync-disk -from disk
run buffer buffer

run bench set-r0-w16 -from set -replicas 0 -workers 16 -payload 16
run bench set-r1-w16 -from set -replicas 1 -workers 16 -payload 16
run bench set-r1-w32 -from set -replicas 1 -workers 32 -payload 16
run bench set-r2-w16 -from set -replicas 2 -workers 16 -payload 16
run bench set-wait-r1-w16 -from set-wait -replicas 1 -workers 16 -wait-for 1 -wait-ms 1000
run bench set-wait-r2-w16 -from set-wait -replicas 2 -workers 16 -wait-for 2 -wait-ms 1000 -payload 16
run bench set-200-r1-w16 -from set -replicas 1 -workers 16 -payload 200
run bench get-master-w16 -from master -replicas 1 -workers 16
run bench get-master-w32 -from master -replicas 1 -workers 32
run bench get-replica-w16 -from replica -replicas 1 -workers 16
run bench get-both-w16 -from both -replicas 1 -workers 16

run stale stale-delay -delay 2ms -keys 2000
run gap gap-delay -delay 2ms
run bench set-delay-w16 -from set -replicas 1 -workers 16 -payload 16 -delay 2ms
run bench set-wait-delay-w16 -from set-wait -replicas 1 -workers 16 -payload 16 -wait-for 1 -wait-ms 1000 -delay 2ms
run wait-timeout wait-timeout

echo "=== loss, no aof ==="
stop_redis
fresh_dirs
start_master
start_replica
run loss loss -replica2 ""
fresh_master_dir
start_master
run loss-after loss-after -replica2 ""

aof_case() {
  local name="$1"
  local policy="$2"
  echo "=== aof ${name} ==="
  stop_redis
  fresh_dirs
  start_master_aof
  start_replica
  run aof-cut "${name}-cut" -fsync "$policy" -replica2 "" -datadir /tmp/miaosha-redis-repl-6379
  start_master_aof
  run aof-after "${name}-after" -fsync "$policy" -replica2 "" -datadir /tmp/miaosha-redis-repl-6379
}

aof_case aof-everysec everysec
aof_case aof-always always

stop_redis
echo "ALL PASS"
