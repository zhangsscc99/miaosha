#!/usr/bin/env bash
# 固定窗口跨秒双倍额度，以及拦截器先 GET 再写的计数。会 FLUSHDB 6379。
# 不要对已经有数据的实例运行。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-redis-6379

if [[ "$(getconf CLK_TCK)" != "100" ]]; then
  echo "CLK_TCK is $(getconf CLK_TCK), cpu percent divides by 100" >&2
  exit 1
fi

REDIS_PREFIX="${REDIS_PREFIX:-/tmp/redis-7015}"
if [[ ! -x "$REDIS_PREFIX/bin/redis-server" ]]; then
  mkdir -p /tmp/redis-src
  curl -fL --retry 3 --retry-delay 2 -o /tmp/redis-7.0.15.tar.gz \
    https://download.redis.io/releases/redis-7.0.15.tar.gz
  tar -xzf /tmp/redis-7.0.15.tar.gz -C /tmp/redis-src
  make -C /tmp/redis-src/redis-7.0.15 -j"$(nproc)"
  make -C /tmp/redis-src/redis-7.0.15 PREFIX="$REDIS_PREFIX" install
fi
REDIS_SERVER="$REDIS_PREFIX/bin/redis-server"
REDIS_CLI="$REDIS_PREFIX/bin/redis-cli"

stop_redis() {
  "$REDIS_CLI" -p 6379 shutdown nosave >/dev/null 2>&1 || true
  sleep 0.2
}

wait_ping() {
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if "$REDIS_CLI" -p 6379 ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.2
  done
  echo "redis :6379 did not start" >&2
  return 1
}

trap stop_redis EXIT
stop_redis
"$REDIS_SERVER" "$PWD/redis-64mb.conf"
wait_ping

export GOTOOLCHAIN=local
go test -count=1 -timeout 60s .
go build -o /tmp/miaosha-window .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  lscpu | awk -F: '/Model name|^CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -n 2
  "$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|process_id|redis_mode|os|gcc_version/'
  "$REDIS_CLI" -p 6379 INFO memory | awk -F: '/mem_allocator|maxmemory_human|used_memory:|maxmemory_policy/'
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory
  "$REDIS_CLI" -p 6379 CONFIG GET appendonly
  "$REDIS_CLI" -p 6379 CONFIG GET hz
} > results/env.txt

/tmp/miaosha-window -addr 127.0.0.1:6379 -lua "$PWD/lua" -out-dir "$PWD/results" "$@"
