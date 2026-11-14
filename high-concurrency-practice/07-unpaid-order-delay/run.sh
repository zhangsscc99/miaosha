#!/usr/bin/env bash
# 未支付订单的到期关单。会 FLUSHDB 6379，并在 /tmp 写 SQLite 文件。不要对已经有数据的实例运行。
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
go build -o /tmp/miaosha-delay .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  echo "ulimit_n=$(ulimit -n)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  "$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|tcp_port|process_id|os|hz|mem_allocator/'
  "$REDIS_CLI" -p 6379 INFO memory | awk -F: '/mem_allocator|maxmemory_human/'
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory-policy
  "$REDIS_CLI" -p 6379 CONFIG GET appendonly
  "$REDIS_CLI" -p 6379 CONFIG GET hz
  python3 - <<'PY'
import sqlite3
print("sqlite", sqlite3.sqlite_version)
PY
} > results/env.txt

BIN=/tmp/miaosha-delay
run() {
  local file="$1"
  shift
  echo "=== ${file} ==="
  "$BIN" -scenario "$file" -out "results/${file}.json" "$@"
}

run unsafe -note "32 个连接各自对 2000 张未支付订单做 HGET，看到 UNPAID 就 HSET CLOSED 再 INCR。三次往返，没有原子性。" \
  -mode unsafe -workers 32 -orders 2000
run lua-byid -note "同样 32 个连接、同样 2000 张单。关单改成一次 Lua：不是未支付就只摘掉集合，是未支付才改状态并加 1 件库存。" \
  -mode lua-byid -workers 32 -orders 2000
run pay-race -note "4000 张单。8 个连接支付，8 个连接按同一批 id 关单，两边同时开始。每张单恰好被付一次、被关一次。" \
  -mode pay-race -workers 8 -orders 4000
run replay -note "同一张单连续关两次。第二次不再加库存。" \
  -mode replay
run not-yet -note "500 张单的截止时间在 60 秒后。用当前时间去弹，一张都不应该关。把时间参数拨过截止时间之后再关完。" \
  -mode not-yet -orders 500
run ttl -note "1000 个带 400 毫秒 TTL 的键。等它们过期。没有任何回调去加库存，所以库存保持 0。这不是延迟队列。" \
  -mode ttl -orders 1000
run recover -note "先从有序集合领到处理中列表。id 能被 10 整除的 100 张故意不执行完成脚本，模拟进程停在两次调用之间。再用完成脚本把这 100 张补上。最后把 1 号单再完成一次。" \
  -mode recover -orders 1000
run drain-w1 -note "50000 张已经到期的单，1 个连接用 close_due 往空里弹。这是现有定时任务拿着一把锁、单线程扫的形状。" \
  -mode drain -workers 1 -orders 50000
run drain-w16 -note "同样 50000 张到期单，16 个连接一起弹。脚本本身是原子的，不需要再拿一把全局锁。" \
  -mode drain -workers 16 -orders 50000

run recycle-c1000 -note "基数钉在 1000。脚本比生产多一次 HSET 和一次 ZADD，所以 QPS 是下限。预热 1 秒，统计 3 秒。" \
  -mode recycle -workers 16 -card 1000 -warmup 1s -duration 3s
run recycle-c10000 -note "基数 10000。其余和 recycle-c1000 相同。" \
  -mode recycle -workers 16 -card 10000 -warmup 1s -duration 3s
run recycle-c100000 -note "基数 100000。看有序集合变大以后，单线程还是不是同一档。" \
  -mode recycle -workers 16 -card 100000 -warmup 1s -duration 3s

run zadd-100000 -note "16 个连接各自 ZADD 新订单，一共 10 万个成员。一次往返一条命令，没有流水线。字节数用灌入前后的 used_memory 相减。" \
  -mode zadd -workers 16 -orders 100000
run memory-100000 -note "先只放 10 万个有序集合成员，再给每个订单一条状态。两段 used_memory 分开记。这是存量，不是 QPS。" \
  -mode memory -card 100000

run late-poll-20ms -note "2000 张单，截止时间每 1 毫秒一张，从启动后 250 毫秒开始。1 个线程每 20 毫秒扫一次到期订单。" \
  -mode late-poll -orders 2000 -poll 20ms -step 1ms -workers 1
run late-poll-200ms -note "同样 2000 张单。轮询间隔改成 200 毫秒。" \
  -mode late-poll -orders 2000 -poll 200ms -step 1ms -workers 1
run late-smart -note "同样 2000 张单。先看最早一张的 score，睡到那个时间再关。没有固定轮询间隔。" \
  -mode late-smart -orders 2000 -step 1ms -workers 1

run recycle-aof -note "打开 AOF everysec 之后再跑基数 10000 的循环脚本。和 recycle-c10000 比写放大，不和别的章节的 AOF 比例混用。" \
  -mode recycle -workers 16 -card 10000 -warmup 1s -duration 3s -aof
"$REDIS_CLI" -p 6379 CONFIG SET appendonly no >/dev/null

run sql-50k -note "SQLite 5 万行，其中 2000 行到期。synchronous=FULL。先全表过滤，再建 (status, deadline) 索引。5 次 SELECT 的时间都留下，中位数写在外面。最后一条 UPDATE 把到期行改成已关闭。" \
  -mode sql -rows 50000 -due 2000
run sql-400k -note "同样的库表，放大到 40 万行、5000 行到期。用来看全表扫描随行数怎么变。这是 SQLite，不是 MySQL。" \
  -mode sql -rows 400000 -due 5000

stop_redis
trap - EXIT
echo "ALL PASS"
