#!/usr/bin/env bash
# 一键跑完所有对照实验。要求本机已启动 MySQL / Redis / RabbitMQ，且已执行 sql/lab_schema.sql。
# 资源切片：server 1 个 JVM(-Xmx512m)、Redis 1 实例、MySQL 1 实例、RabbitMQ 1 实例，全部本机 8 vCPU。
set -uo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/Cellar/openjdk/27.0.1.0.3/libexec/openjdk.jdk/Contents/Home}"
JAVA="$JAVA_HOME/bin/java"
CP="target/classes:$(cat target/cp.txt)"
PORT="${PORT:-18080}"
BASE="http://127.0.0.1:$PORT"
STOCK="${STOCK:-5000000}"
CONC="${CONC:-64}"
SECS="${SECS:-15}"
WARM="${WARM:-3}"
OUT="${OUT:-results}"
STAMP="$(date +%Y%m%d-%H%M%S)"
RAW="$OUT/raw-$STAMP.jsonl"

mkdir -p "$OUT"
: > "$RAW"
SRV=""

cleanup() {
  if [ -n "$SRV" ] && kill -0 "$SRV" 2>/dev/null; then
    kill "$SRV" 2>/dev/null
    sleep 1
    kill -9 "$SRV" 2>/dev/null
  fi
}
trap cleanup EXIT

wait_health() {
  for _ in $(seq 1 80); do
    if curl -sf -m 1 "$BASE/stats" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

start_server() {
  local mode="$1"
  shift
  "$JAVA" -Xms256m -Xmx512m -cp "$CP" com.geekq.lab.SeckillServer \
    --mode "$mode" --port "$PORT" "$@" > "$OUT/server-$mode.log" 2>&1 &
  SRV=$!
  if ! wait_health; then
    echo "[run_all] server $mode 启动失败："
    tail -30 "$OUT/server-$mode.log"
    exit 1
  fi
}

stop_server() {
  if [ -n "$SRV" ] && kill -0 "$SRV" 2>/dev/null; then
    kill "$SRV" 2>/dev/null
    wait "$SRV" 2>/dev/null
  fi
  SRV=""
  sleep 1
}

run_case() {
  local label="$1" mode="$2" stock="$3" conc="$4" secs="$5"
  shift 5
  echo "[run_all] ==== $label (mode=$mode, stock=$stock, conc=$conc, ${secs}s) ===="
  start_server "$mode" "$@"
  curl -sf "$BASE/admin/reset?stock=$stock" >/dev/null
  redis-cli config resetstat >/dev/null 2>&1 || true
  local q0
  q0=$(mysql -uroot -proot -N -e "show global status" 2>/dev/null | awk '$1=="Com_insert"||$1=="Com_update"||$1=="Com_delete"{s+=$2} END{print s+0}')

  ( while kill -0 "$SRV" 2>/dev/null; do ps -o %cpu=,rss= -p "$SRV"; sleep 1; done ) > "$OUT/ps-$label.txt" &
  local samp=$!

  "$JAVA" -Xms256m -Xmx512m -cp "$CP" com.geekq.lab.LoadGen \
    --url "$BASE/seckill?goodsId=1" --seconds "$secs" --warmup "$WARM" \
    --concurrency "$conc" --label "$label" --out "$OUT/load-$label.json" | tee -a "$RAW"

  local q1
  q1=$(mysql -uroot -proot -N -e "show global status" 2>/dev/null | awk '$1=="Com_insert"||$1=="Com_update"||$1=="Com_delete"{s+=$2} END{print s+0}')
  if [ -n "$q0" ] && [ -n "$q1" ]; then
    local total_secs=$((WARM + secs))
    local dbqps=$(( (q1 - q0) / total_secs ))
    echo "{\"label\":\"$label\",\"mysqlWrites\":$((q1 - q0)),\"mysqlWriteQps\":$dbqps}" | tee -a "$RAW" > "$OUT/mysql-$label.json"
  fi

  sleep 4
  curl -s "$BASE/stats" > "$OUT/stats-$label.json" || true
  redis-cli info commandstats 2>/dev/null | grep -E "cmdstat_(eval|get|set|incr|decr)" > "$OUT/redis-$label.txt" || true

  kill "$samp" 2>/dev/null
  wait "$samp" 2>/dev/null
  stop_server
}

echo "[run_all] stamp=$STAMP java=$($JAVA -version 2>&1 | head -1)"
echo "[run_all] host=$(sysctl -n hw.ncpu) vCPU / $(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 )) GB"

run_case baseline-db        v0 "$STOCK" "$CONC" "$SECS"
run_case redis-sync-db      v1 "$STOCK" "$CONC" "$SECS"
run_case redis-mq           v2 "$STOCK" "$CONC" "$SECS"
run_case localcache-mq     v3 "$STOCK" "$CONC" "$SECS"
run_case ratelimit-v3       v4 "$STOCK" 256 "$SECS" --rate 5000
run_case soldout-db         v0 100 256 10
run_case soldout-localcache v3 100 256 10

echo "[run_all] ==== 正确性：v2 / v3 各 100 库存、3000 并发请求 ===="
for m in v2 v3; do
  start_server "$m"
  "$JAVA" -Xms256m -Xmx512m -cp "$CP" com.geekq.lab.CorrectnessTest "$BASE" 100 3000 128 \
    | tee -a "$RAW"
  stop_server
done

echo "[run_all] ==== 分布式锁 / Lua 微基准 ===="
"$JAVA" -Xms256m -Xmx512m -cp "$CP" com.geekq.lab.LockBench all 8 3000 | tee -a "$RAW" > "$OUT/lockbench.txt"

echo "[run_all] ==== Redis 单机基线 ===="
{
  echo "# Redis 8.10.3, 8 vCPU 本机, 64 clients, 200k requests"
  redis-benchmark -t set,get,incr -n 200000 -c 64 -q
  redis-benchmark -n 200000 -c 64 -q eval "return redis.call('incr', KEYS[1])" 1 bench:stock
} | tee -a "$RAW" > "$OUT/redis-benchmark.txt"

echo "[run_all] done -> $RAW"
