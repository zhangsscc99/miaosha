#!/usr/bin/env bash
# 同机 3 个 Redis Cluster 主库，外加 1 个没开 cluster 的对照进程。
# 会清掉 /tmp/miaosha-cluster，并关掉 7101–7104 上的 Redis。不要对有数据的实例运行。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results

if [[ ! -x /tmp/redis-7015/bin/redis-server ]]; then
  if [[ ! -d /tmp/redis-7.0.15 ]]; then
    curl -fsSL -o /tmp/redis-7.0.15.tar.gz https://download.redis.io/releases/redis-7.0.15.tar.gz
    tar -C /tmp -xzf /tmp/redis-7.0.15.tar.gz
  fi
  make -C /tmp/redis-7.0.15 -j"$(nproc)" PREFIX=/tmp/redis-7015 install
fi
REDIS_SERVER=/tmp/redis-7015/bin/redis-server
REDIS_CLI=/tmp/redis-7015/bin/redis-cli
BASE=/tmp/miaosha-cluster

stop_ports() {
  local port
  for port in 7101 7102 7103 7104; do
    "$REDIS_CLI" -p "$port" shutdown nosave >/dev/null 2>&1 || true
  done
}

wait_ping() {
  local port="$1"
  local _
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if "$REDIS_CLI" -p "$port" ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.1
  done
  echo "redis :$port did not start" >&2
  return 1
}

write_conf() {
  local port="$1"
  local cluster="$2"
  local dir="$BASE/$port"
  mkdir -p "$dir"
  cat > "$dir/redis.conf" <<EOF
bind 127.0.0.1
port $port
protected-mode yes
daemonize yes
pidfile $dir/redis.pid
logfile $dir/redis.log
dir $dir
dbfilename dump.rdb
save ""
appendonly no
maxmemory 64mb
maxmemory-policy noeviction
timeout 0
tcp-backlog 511
loglevel notice
enable-debug-command local
EOF
  if [[ "$cluster" == "yes" ]]; then
    cat >> "$dir/redis.conf" <<EOF
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
cluster-require-full-coverage yes
EOF
  fi
}

trap stop_ports EXIT
stop_ports
rm -rf "$BASE"
mkdir -p "$BASE"

for port in 7101 7102 7103; do
  write_conf "$port" yes
  "$REDIS_SERVER" "$BASE/$port/redis.conf"
  wait_ping "$port"
done
write_conf 7104 no
"$REDIS_SERVER" "$BASE/7104/redis.conf"
wait_ping 7104

"$REDIS_CLI" --cluster create 127.0.0.1:7101 127.0.0.1:7102 127.0.0.1:7103 --cluster-replicas 0 --cluster-yes \
  > results/create.txt

ok=""
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25; do
  st=$("$REDIS_CLI" -p 7101 CLUSTER INFO | awk -F: '/^cluster_state/{gsub(/\r/,"",$2); print $2}')
  if [[ "$st" == "ok" ]]; then
    ok=yes
    break
  fi
  sleep 0.2
done
if [[ -z "$ok" ]]; then
  echo "cluster did not become ok" >&2
  cat results/create.txt >&2 || true
  exit 1
fi

go build -o /tmp/miaosha-cluster-bin .
export GOMEMLIMIT=256MiB

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
} > results/env.txt

/tmp/miaosha-cluster-bin -redis "$REDIS_SERVER" -confdir "$BASE" -lua "$PWD/lua/stock.lua" -out "$PWD/results" "$@"
"$REDIS_SERVER" --version >> results/env.txt
