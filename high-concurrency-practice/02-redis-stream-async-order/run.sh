#!/usr/bin/env bash
# 用本机一个 64MB Redis 和本机 SQLite 把第 2 章跑完。不要带生产流量。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-redis-6379

stop_redis() {
  redis-cli -p 6379 shutdown nosave >/dev/null 2>&1 || true
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

export CGO_ENABLED=1
go build -o /tmp/miaosha-streambench .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$(redis-server --version)"
  echo "sqlite=$(sqlite3 --version)"
  echo "nproc=$(nproc)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  echo "--- redis 64mb ---"
  redis-cli INFO server | awk -F: '/redis_version|process_id|tcp_port|os/'
  redis-cli CONFIG GET maxmemory
  redis-cli CONFIG GET appendonly
  redis-cli CONFIG GET stream-node-max-entries
} > results/env.txt

run() {
  local name="$1"
  shift
  echo "=== ${name} ==="
  /tmp/miaosha-streambench -scenario "$name" -out "results/${name}.json" "$@"
}

run atomic-enqueue -workers 64 -stock 100 -users 400 -repeat 10 -sync FULL
run split-loss -workers 32 -stock 100 -users 1000 -sync FULL
run retry -sync FULL
run poison -sync FULL
run maxlen-loss

run enqueue-tps -workers 16 -duration 3s -warmup 500ms -stock 5000000 -maxlen 10000 -out results/enqueue-tps-w16.json
run enqueue-tps -workers 32 -duration 3s -warmup 500ms -stock 5000000 -maxlen 10000
mv results/enqueue-tps.json results/enqueue-tps-w32.json

run xadd-tps -workers 16 -duration 3s -warmup 500ms -maxlen 10000 -out results/xadd-tps-w16.json
run xadd-tps -workers 16 -duration 3s -warmup 500ms -maxlen 10000 -fat 200 -out results/xadd-tps-fat.json

run consume -messages 2000 -consumers 1 -sync FULL -batch 1 -out results/consume-full.json
run consume -messages 8000 -consumers 1 -sync FULL -batch 100 -out results/consume-full-batch.json
run consume -messages 8000 -consumers 1 -sync NORMAL -batch 1 -out results/consume-normal.json
run consume -messages 8000 -consumers 1 -sync OFF -batch 1 -out results/consume-off.json
run consume -messages 4000 -consumers 4 -sync FULL -batch 1 -out results/consume-full-c4.json

run sync-path -workers 4 -stock 5000 -sync FULL -out results/sync-full.json
run async-path -workers 4 -stock 5000 -sync FULL -batch 1 -out results/async-full-batch1.json
run async-path -workers 4 -stock 5000 -sync FULL -batch 100 -out results/async-full.json

run backlog -messages 2000000 -fat 0 -out results/backlog-small.json
run backlog -messages 2000000 -fat 200 -out results/backlog-fat.json

echo "=== aof everysec ==="
redis-cli CONFIG SET appendonly yes >/dev/null
redis-cli CONFIG SET appendfsync everysec >/dev/null
run enqueue-tps -workers 16 -duration 3s -warmup 500ms -stock 5000000 -maxlen 10000 -out results/enqueue-tps-aof.json

stop_redis
echo "--- pass ---"
python3 - <<'PY'
import json, pathlib
root = pathlib.Path("results")
bad = 0
for p in sorted(root.glob("*.json")):
    d = json.loads(p.read_text())
    ok = d.get("pass")
    if not ok:
        bad += 1
    print(f"{p.name:28} pass={ok} qps={d.get('qps') or d.get('producerQps') or 0:.0f} orders={d.get('orders')} entries={d.get('entries')} {d.get('detail','')}")
if bad:
    raise SystemExit(f"{bad} scenario(s) failed")
PY
