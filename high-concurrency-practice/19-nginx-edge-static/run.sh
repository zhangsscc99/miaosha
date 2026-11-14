#!/usr/bin/env bash
# 第 19 章一键复现：起 Redis 6390、Java 上游、nginx，跑完再停掉。
#
#   bash run.sh              # 只起服务，停在那里（Ctrl-C 结束）
#   bash run.sh matrix       # 9 项主对照，3 轮
#   bash run.sh scale        # 32/64/128/256 连接
#   bash run.sh cache        # 冷启动 256 并发，lock on/off
#   bash run.sh stale        # TTL 过期，lock vs stale-while-revalidate
#   bash run.sh limits       # 限流三种 burst
set -euo pipefail
cd "$(dirname "$0")"

MODE="${1:-serve}"
RUNTIME="$PWD/runtime"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk}"
JAVA="$JAVA_HOME/bin/java"

cleanup() {
  if [[ -f "$RUNTIME/logs/nginx.pid" ]]; then
    kill "$(cat "$RUNTIME/logs/nginx.pid")" 2>/dev/null || true
  fi
  if [[ -n "${EDGE_PID:-}" ]]; then
    kill "$EDGE_PID" 2>/dev/null || true
  fi
  if [[ -f "$RUNTIME/redis.pid" ]]; then
    kill "$(cat "$RUNTIME/redis.pid")" 2>/dev/null || true
  fi
}
trap cleanup EXIT

mkdir -p "$RUNTIME/logs" "$RUNTIME/cache" "$RUNTIME/redis" results/runs

# 1. 静态页和 Redis 种子
python3 scripts/gen_data.py
# 2. 一个只给这一章用的 Redis，避免动到 6379
redis-server --port 6390 --maxmemory 256mb --maxmemory-policy allkeys-lru \
  --save '' --appendonly no --daemonize yes \
  --pidfile "$RUNTIME/redis.pid" --dir "$RUNTIME/redis" --logfile "$RUNTIME/redis.log"
sleep 0.5
redis-cli -p 6390 flushall >/dev/null
redis-cli -p 6390 --pipe < "$RUNTIME/seed.resp" >/dev/null

# 3. Java 上游：/dyn 8KB、/api 2.3KB、/stats 和 /delay 控制面
bash scripts/build.sh
"$JAVA" -Xmx512m -cp "target/classes:$(cat target/cp.txt)" com.geekq.lab.edge.EdgeServer \
  --port 18090 --redisPort 6390 > "$RUNTIME/logs/edge.log" 2>&1 &
EDGE_PID=$!
for _ in $(seq 1 50); do
  curl -sf http://127.0.0.1:18090/health >/dev/null 2>&1 && break
  sleep 0.2
done

# 4. nginx。worker_connections 8192 需要把 fd 上限提上去
ulimit -n 32768
nginx -p "$RUNTIME/" -c "$PWD/nginx/nginx.conf"
sleep 0.5
curl -sf -o /dev/null http://127.0.0.1:18080/p1.html || { echo "nginx 静态页没起来" >&2; exit 1; }

echo "services up: nginx 18076/18078-18089, edge 18090, redis 6390, mode=$MODE"
cat > results/env.txt <<EOF
date=$(date -Iseconds)
nginx=$(nginx -v 2>&1)
java=$("$JAVA" -version 2>&1 | head -1)
redis=$(redis-server --version)
uname=$(uname -a)
load=$(uptime)
EOF

case "$MODE" in
  serve) wait ;;
  *) python3 scripts/bench.py "$MODE" ;;
esac
