#!/usr/bin/env bash
# 同机 Redis 7.0.15：1 个主库、1 或 2 个从库、3 个 Sentinel。
# 会关掉本机 6379、6380、6381、26379、26380、26381 上的 Redis。不要对有数据的实例运行。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-sentinel

if [[ ! -x /tmp/redis-7015/bin/redis-server ]]; then
  if [[ ! -d /tmp/redis-7.0.15 ]]; then
    curl -fsSL -o /tmp/redis-7.0.15.tar.gz https://download.redis.io/releases/redis-7.0.15.tar.gz
    tar -C /tmp -xzf /tmp/redis-7.0.15.tar.gz
  fi
  make -C /tmp/redis-7.0.15 -j"$(nproc)" PREFIX=/tmp/redis-7015 install
fi
REDIS_SERVER=/tmp/redis-7015/bin/redis-server
REDIS_CLI=/tmp/redis-7015/bin/redis-cli

stop_ports() {
  local port
  for port in 6379 6380 6381 26379 26380 26381; do
    "$REDIS_CLI" -p "$port" shutdown nosave >/dev/null 2>&1 || true
  done
}

trap stop_ports EXIT
stop_ports

go build -o /tmp/miaosha-sentinel-bin .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
} > results/env.txt

/tmp/miaosha-sentinel-bin -scenario all
"$REDIS_SERVER" --version >> results/env.txt
