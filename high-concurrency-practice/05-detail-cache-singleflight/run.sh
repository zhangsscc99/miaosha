#!/usr/bin/env bash
# 商品详情缓存击穿。会 FLUSHDB 6379，并在 /tmp 写一个 SQLite 文件。不要对已经有数据的实例运行。
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

FIXTURE="$PWD/../../miaosha-v1/src/main/resources/static/goods_detail.htm"
if [[ ! -f "$FIXTURE" ]]; then
  echo "missing fixture $FIXTURE" >&2
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
go build -o /tmp/miaosha-detail .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  echo "ulimit_n=$(ulimit -n)"
  echo "fixture=$FIXTURE"
  echo "fixture_bytes=$(wc -c < "$FIXTURE")"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  "$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|tcp_port|process_id|os/'
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory
  "$REDIS_CLI" -p 6379 CONFIG GET appendonly
  python3 - <<'PY'
import sqlite3
print("sqlite", sqlite3.sqlite_version)
print("compile", [r[0] for r in sqlite3.connect(":memory:").execute("pragma compile_options") if "THREADSAFE" in r[0]])
PY
} > results/env.txt

BIN=/tmp/miaosha-detail
run() {
  local file="$1"
  shift
  echo "=== ${file} ==="
  "$BIN" -role super -scenario "$file" -fixture "$FIXTURE" -lua "$PWD/lua" -out "results/${file}.json" -pool 8 -service-ms 20 "$@"
}

# 热详情已经在 Redis 里。比较「只 GET」和「GET 之后把页面从本进程抄一遍」。
run redis-get -note "缓存命中。1 个进程 16 条连接，预热 0.5 秒，统计 2 秒。只检查长度，不再把 6KB 抄第二遍。" \
  -mode redis-get -instances 1 -workers 16 -duration 2s
run redis-get-32 -note "同样 1 个进程，连接加到 32。看 16 连接是不是还没把 Redis 喂满。" \
  -mode redis-get -instances 1 -workers 32 -duration 2s
run redis-get-64 -note "1 个进程 64 条连接。连接再加，QPS 不一定上去。" \
  -mode redis-get -instances 1 -workers 64 -duration 2s
run redis-get-4 -note "同样的热 key。4 个进程各 8 条连接，统计 2 秒。看一个 key 会不会变成 4 倍。" \
  -mode redis-get -instances 4 -workers 8 -duration 2s
run redis-get-touch -note "命中之后把页面抄进发送缓冲。这是从 Redis 把详情交出去的本机成本。" \
  -mode redis-get-touch -instances 1 -workers 16 -duration 2s

# 详情已经在本进程里。抄同一份字节，不再访问 Redis。
run local-hit -note "进程内已经有这份页面。1 个进程 16 个 goroutine，预热 0.5 秒，统计 2 秒。" \
  -mode local-hit -instances 1 -workers 16 -duration 2s
run local-hit-4 -note "4 个进程各 4 个 goroutine，各自抄自己内存里的页面。用来看这份字节能不能吃满 4 个核。" \
  -mode local-hit -instances 4 -workers 4 -duration 2s

# SQLite 只做 SELECT，不占额外 CPU。这是本机读这一行的上限，不是 MySQL。
run sql-seq -note "1 条连接顺序 SELECT 这一行 2000 次。没有缓存。" \
  -mode sql-fast -instances 1 -workers 1 -attempts 2000
run sql-pool -note "8 条连接一共 4000 次 SELECT。连接池是 8，和上面的顺序读对照。" \
  -mode sql-fast -instances 1 -workers 8 -attempts 4000

# 缓存是空的。4 个进程同时进来。fast 只有 SQL；slow 把连接占住一段实测过的 CPU。
run stampede-fast-a -note "4 个进程各 128 个请求同时未命中。回源只有 SQLite SELECT，然后 SET，TTL 60 秒。快路径的回源次数会晃，所以连跑三次。" \
  -mode stampede-fast -instances 4 -workers 128 -attempts 128
run stampede-fast-b -note "快路径击穿的第二次。和 a、c 一起看范围，不挑最严重的一次当结论。" \
  -mode stampede-fast -instances 4 -workers 128 -attempts 128
run stampede-fast-c -note "快路径击穿的第三次。" \
  -mode stampede-fast -instances 4 -workers 128 -attempts 128
run stampede-slow -note "同样 512 个请求。每次回源占住连接池里的一条，池大小 8。没有 singleflight。" \
  -mode stampede-slow -instances 4 -workers 128 -attempts 128

run singleflight-1 -note "1 个进程 256 个请求同时未命中。进程内 singleflight，回源走慢路径。" \
  -mode singleflight -instances 1 -workers 256 -attempts 256
run singleflight -note "4 个进程各 128 个请求。singleflight 不跨进程。" \
  -mode singleflight -instances 4 -workers 128 -attempts 128

run distlock -note "4 个进程共 512 个请求。SET NX PX 5000，没抢到的人每 1 毫秒 GET 一次，自己不回源。" \
  -mode distlock -instances 4 -workers 128 -attempts 128 -lock-ms 5000
run logical -note "值还在，逻辑过期时间已经是 0。读到旧页面就返回。只有抢到刷新锁的那一个回源。" \
  -mode logical -instances 4 -workers 128 -attempts 128 -lock-ms 5000
run local-logical -note "旧页面在进程内存里。读者不等刷新。每个进程自己刷一次，不再为这一波去打 Redis。" \
  -mode local-logical -instances 4 -workers 128 -attempts 128

run lock-short -note "锁的 PX 只有 2 毫秒，回源是慢路径。持有者还没写完，锁就过期了。" \
  -mode lock-short -instances 4 -workers 32 -attempts 32 -lock-ms 2
run lock-stuck -note "1 个进程里 0 号 worker 拿到没有过期时间的锁之后退出。另外 7 个等 150 毫秒，详情仍然是空的。" \
  -mode lock-stuck -instances 1 -workers 8 -attempts 8 -lock-ms 0

run penetrate -note "商品不存在。未命中不写占位。200 个请求每个都进 SQLite。" \
  -mode penetrate -instances 1 -workers 200 -attempts 200
run negative-race -note "不存在的商品会写入 __NULL__，但是没有 singleflight。第一波仍会有多次回源。之后每个 worker 再 GET 20 次。" \
  -mode negative-race -instances 1 -workers 200 -attempts 200 -follow 20
run negative -note "同样的占位，加上进程内 singleflight。第一波 1 次回源，随后 20 次 GET 不再回源。" \
  -mode negative -instances 1 -workers 200 -attempts 200 -follow 20

stop_redis
trap - EXIT
echo "ALL PASS"
