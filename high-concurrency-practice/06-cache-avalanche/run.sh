#!/usr/bin/env bash
# 多 key 同时过期。会 FLUSHDB 6379，并在 /tmp 写一个 SQLite 文件。不要对已经有数据的实例运行。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-redis-6379

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

export CGO_ENABLED=1
go test .
go build -o /tmp/miaosha-avalanche .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  echo "ulimit_n=$(ulimit -n)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  "$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|tcp_port|process_id|os|hz/'
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory
  "$REDIS_CLI" -p 6379 CONFIG GET appendonly
  "$REDIS_CLI" -p 6379 CONFIG GET hz
  "$REDIS_CLI" -p 6379 CONFIG GET lazyfree-lazy-expire
  python3 - <<'PY'
import sqlite3
print("sqlite", sqlite3.sqlite_version)
print("compile", [r[0] for r in sqlite3.connect(":memory:").execute("pragma compile_options") if "THREADSAFE" in r[0]])
PY
} > results/env.txt

BIN=/tmp/miaosha-avalanche
run() {
  local file="$1"
  shift
  echo "=== ${file} ==="
  "$BIN" -role super -scenario "$file" -out "results/${file}.json" "$@"
}

# 过期曲线。EXISTS 会把已经到期的 key 惰性删掉，所以这一组测的是「被同时访问」。
run expire-same -note "2000 个 key 同一个 500 毫秒 TTL。每 50 毫秒用 EXISTS 数一次还在的 key。采样本身会触发惰性删除。" \
  -mode expire-same -keys 2000 -instances 0 -sample-ms 50 -window-ms 1500
run expire-jitter -note "2000 个 key 的 TTL 铺在 500 到 2500 毫秒。同样用 EXISTS 采样。看一次采样里最多消失多少。" \
  -mode expire-jitter -keys 2000 -instances 0 -sample-ms 100 -window-ms 4000

# 不访问 key，只看主动过期。PING 不是业务负载。
run expire-active-hz10 -note "10 万个小 key，同一个 500 毫秒 TTL。不 GET。hz=10。每 100 毫秒记 expired_keys、keyspace 和一次 PING。" \
  -mode expire-active-hz10 -keys 100000 -instances 0 -hz 10 -sample-ms 100 -window-ms 3000
run expire-active-hz100 -note "同样 10 万个 key，hz=100。对照主动过期扫得快不快，以及 PING 有没有变慢。" \
  -mode expire-active-hz100 -keys 100000 -instances 0 -hz 100 -sample-ms 100 -window-ms 3000

# 同时未命中。回源是 SQLite 加一段校准过的 CPU。请求数对齐：4 进程 × 800 次。
# 不排队时，冲进数据库的次数会晃。每种池子跑 4 次，最后一次的 JSON 留全量，四次的一行摘要在 race-spread.txt。
: > results/race-spread.txt
echo "mode pool origin wallSec peak slotAvgUs originCpu missP99us maxus" >> results/race-spread.txt
spread() {
  local mode="$1" pool="$2" note="$3"
  echo "=== ${mode} ===" >&2
  "$BIN" -role super -scenario "$mode" -mode "$mode" -note "$note" \
    -keys 200 -repeat 4 -instances 4 -workers 32 -pool "$pool" -service-ms 2 \
    -out "results/${mode}.json" >/tmp/avalanche-line.txt
  python3 - "$mode" <<'PY'
import json, sys
mode = sys.argv[1]
r = json.load(open(f"results/{mode}.json"))
print(r["mode"], r["originPool"], r["originLoads"], round(r["wallSec"], 3), r["peakOrigin"], round(r["originSlotAvgNs"]/1000, 1), round(r["originCpuPercent"], 1), r["missP99us"], r["maxus"])
PY
}
for _ in 1 2 3 4; do
  spread burst-plain 8 "200 个 key，每进程每个 key 4 个请求，4 个进程，没有 singleflight。池 8。回源次数随调度变化，峰值看 100 毫秒桶。" >> results/race-spread.txt
  spread burst-plain-p4 4 "同一批请求，池改成 4。用来和池 8 比峰值和槽内时间，不比某一次的回源次数。" >> results/race-spread.txt
done
for _ in 1 2 3; do
  spread burst-flight 8 "同样 200 个 key。进程内 singleflight，抢到的人回源前再 GET 一次。" >> results/race-spread.txt
done
run burst-flight -note "同样 200 个 key、每进程 4 个并发。进程内 singleflight，回源前再 GET 一次。上面三行是重复，这一次留完整 JSON。" \
  -mode burst-flight -keys 200 -repeat 4 -instances 4 -workers 32 -pool 8 -service-ms 2
python3 - >> results/race-spread.txt <<'PY'
import json
r = json.load(open("results/burst-flight.json"))
print(r["mode"], r["originPool"], r["originLoads"], round(r["wallSec"], 3), r["peakOrigin"], round(r["originSlotAvgNs"]/1000, 1), round(r["originCpuPercent"], 1), r["missP99us"], r["maxus"])
PY
run burst-onekey-plain -note "请求数仍是每进程 800、共 3200。全部打到 1 个 key。没有 singleflight。" \
  -mode burst-onekey-plain -keys 1 -repeat 800 -instances 4 -workers 32 -pool 8 -service-ms 2
run burst-onekey-flight -note "同样 3200 个请求打到 1 个 key。进程内 singleflight。这是击穿，不是雪崩。" \
  -mode burst-onekey-flight -keys 1 -repeat 800 -instances 4 -workers 32 -pool 8 -service-ms 2
run burst-rebuild -note "只有 0 号进程回源，每个 key 一次。另外 3 个进程每 1 毫秒 GET，自己不回源。" \
  -mode burst-rebuild -keys 200 -repeat 4 -instances 4 -workers 32 -pool 8 -service-ms 2

# 先让 4 个进程都 GET 到未命中，再一起回源。这一组比的是设计，不是调度碰巧。
run gate-plain -note "4 个进程共 3200 次已经确认的未命中，没有 singleflight。每个请求都回源。" \
  -mode gate-plain -keys 200 -repeat 4 -instances 4 -workers 32 -pool 8 -service-ms 2
run gate-flight -note "同样 3200 次已确认的未命中。进程内 singleflight，不再二次 GET。每个进程每个 key 回源一次。" \
  -mode gate-flight -keys 200 -repeat 4 -instances 4 -workers 32 -pool 8 -service-ms 2
run gate-onekey-plain -note "3200 次已确认的未命中打到同一个 key，没有 singleflight。" \
  -mode gate-onekey-plain -keys 1 -repeat 800 -instances 4 -workers 32 -pool 8 -service-ms 2
run gate-onekey-flight -note "3200 次已确认的未命中打到同一个 key。进程内 singleflight。这是击穿。" \
  -mode gate-onekey-flight -keys 1 -repeat 800 -instances 4 -workers 32 -pool 8 -service-ms 2
run gate-rebuild -note "已确认未命中之后，只有 0 号进程回源。另外 3 个进程每 1 毫秒 GET。" \
  -mode gate-rebuild -keys 200 -repeat 4 -instances 4 -workers 32 -pool 8 -service-ms 2

# 持续读。1 个进程，避免把「实例倍数」乘进过期形状里。QPS 是紧循环 GET，不是接口容量。
run cold-fixed -note "缓存是空的。未命中后 SET PX 700 毫秒。1 个进程 8 个 worker，单飞，跑 2.5 秒。第一波和后面的波都应该挤在一起。" \
  -mode cold-fixed -keys 200 -instances 1 -workers 8 -pool 8 -service-ms 2 -duration 2.5s
run cold-jitter -note "同样从空缓存开始。回填的 TTL 是 350 到 1047 毫秒，均值约 700。第一波仍是冷启动，后面的波应该散开。" \
  -mode cold-jitter -keys 200 -instances 1 -workers 8 -pool 8 -service-ms 2 -duration 2.5s
run warm-fixed -note "key 已经在 Redis 里。放行时统一 PEXPIRE 700 毫秒。没有冷启动那一波。" \
  -mode warm-fixed -keys 200 -instances 1 -workers 8 -pool 8 -service-ms 2 -duration 2.5s
run warm-jitter -note "放行时按 key 把 PEXPIRE 铺在 350 到 1047 毫秒。均值和固定 TTL 一样。" \
  -mode warm-jitter -keys 200 -instances 1 -workers 8 -pool 8 -service-ms 2 -duration 2.5s

# 本地缓存的截止时间。命中不打 Redis。每 10 毫秒扫一轮，这是采样，不是接口 QPS。
run local-fixed -note "4 个进程、1000 个 key。本地截止时间都是放行后 400 毫秒。过期后每个 key 只回 Redis 一次。" \
  -mode local-fixed -keys 1000 -instances 4 -workers 8 -duration 1.5s
run local-proc -note "每个进程的本地截止时间错开 200 毫秒：400、600、800、1000。key 之间不错开。" \
  -mode local-proc -keys 1000 -instances 4 -workers 8 -duration 1.5s
run local-key -note "每个 key 再加 0 到 399 毫秒，四个进程用同一组偏移，所以同一个 key 仍会一起过期。" \
  -mode local-key -keys 1000 -instances 4 -workers 8 -duration 1.5s

stop_redis
trap - EXIT
echo "ALL PASS"
