#!/usr/bin/env bash
# 同机 Redis Cluster：3 个主库各带 1 个副本。会清掉 /tmp/miaosha-cluster-fo，并关掉 7201–7207 上的 Redis。
# 不要对已经有业务数据的实例运行。
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
BASE=/tmp/miaosha-cluster-fo

stop_ports() {
  local port
  for port in 7201 7202 7203 7204 7205 7206 7207; do
    if [[ -x "$REDIS_CLI" ]]; then
      "$REDIS_CLI" -p "$port" shutdown nosave >/dev/null 2>&1 || true
    fi
  done
  sleep 0.2
  local p
  for port in 7201 7202 7203 7204 7205 7206 7207; do
    p=$(ss -ltnp 2>/dev/null | awk -v p=":$port" '$4 ~ p"$" {print}' || true)
    if [[ -n "$p" ]]; then
      pid=$(echo "$p" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1)
      if [[ -n "$pid" ]]; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
  done
}

wait_ping() {
  local port="$1"
  local _
  for _ in $(seq 1 50); do
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
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 2000
cluster-require-full-coverage yes
repl-diskless-sync yes
repl-diskless-sync-delay 0
EOF
}

trap stop_ports EXIT
stop_ports
rm -rf "$BASE"
mkdir -p "$BASE"

for port in 7201 7202 7203 7204 7205 7206; do
  write_conf "$port"
  "$REDIS_SERVER" "$BASE/$port/redis.conf"
  wait_ping "$port"
done

"$REDIS_CLI" --cluster create \
  127.0.0.1:7201 127.0.0.1:7202 127.0.0.1:7203 \
  127.0.0.1:7204 127.0.0.1:7205 127.0.0.1:7206 \
  --cluster-replicas 1 --cluster-yes > results/create.txt

ok=""
for _ in $(seq 1 50); do
  st=$("$REDIS_CLI" -p 7201 CLUSTER INFO | awk -F: '/^cluster_state/{gsub(/\r/,"",$2); print $2}')
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

go build -o /tmp/miaosha-cluster-fo-bin .
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

/tmp/miaosha-cluster-fo-bin -redis "$REDIS_SERVER" -confdir "$BASE" -out "$PWD/results" "$@"
