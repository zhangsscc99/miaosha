#!/usr/bin/env bash
# 多进程售罄标记和准入额度。会 FLUSHDB 6379。不要对已经有数据的实例运行。
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

go build -o /tmp/miaosha-admit .

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  echo "ulimit_n=$(ulimit -n)"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  "$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|tcp_port|process_id|os/'
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory
  "$REDIS_CLI" -p 6379 CONFIG GET appendonly
} > results/env.txt

BIN=/tmp/miaosha-admit
run() {
  local file="$1"
  local scenario="$2"
  shift 2
  echo "=== ${file} (${scenario}) ==="
  "$BIN" -role super -scenario "$scenario" -lua "$PWD/lua" -out "results/${file}.json" "$@"
}

# 命令计数：成功路径和售罄路径各一次，用来对照 total_commands_processed 里有没有算进 Lua 里的 redis.call。
run one-ok one-ok -note "1 个进程 1 条连接，库存 1，只打 1 次成功。" \
  -instances 1 -workers 1 -attempts 1 -stock 1 -soldout-flag=true
run one-sold one-sold -note "库存已经是 0，只打 1 次售罄。" \
  -instances 1 -workers 1 -attempts 1 -stock 0 -soldout-flag=true

# 每个进程 16 条连接、各 8000 次点击。标记不共享。
run sellout-i1-w16 sellout -note "1 个进程，16 条连接，8000 次点击，进程内售罄标记。" \
  -instances 1 -workers 16 -attempts 8000 -stock 100 -soldout-flag=true
run sellout-i2-w16 sellout -note "2 个进程各自 16 条连接，标记不共享。" \
  -instances 2 -workers 16 -attempts 8000 -stock 100 -soldout-flag=true
run sellout-i4-w16 sellout -note "4 个进程各自 16 条连接，标记不共享。" \
  -instances 4 -workers 16 -attempts 8000 -stock 100 -soldout-flag=true
run sellout-i8-w16 sellout -note "8 个进程各自 16 条连接，标记不共享。" \
  -instances 8 -workers 16 -attempts 8000 -stock 100 -soldout-flag=true
run sellout-i1-w16-noflag sellout-noflag -note "同样 1 个进程 8000 次，没有售罄标记。" \
  -instances 1 -workers 16 -attempts 8000 -stock 100 -soldout-flag=false
run sellout-i8-w16-noflag sellout-noflag -note "8 个进程各 8000 次，没有售罄标记。点击数是 8 倍。" \
  -instances 8 -workers 16 -attempts 8000 -stock 100 -soldout-flag=false

# 总连接数都是 32，总点击都是 8000。看漏量跟着连接走，还是跟着进程数走。
run split-i1-w32 sellout -note "1 个进程 32 条连接，8000 次点击。" \
  -instances 1 -workers 32 -attempts 8000 -stock 100 -soldout-flag=true
run split-i8-w4 sellout -note "8 个进程各 4 条连接，各 1000 次点击。总连接 32，总点击 8000。" \
  -instances 8 -workers 4 -attempts 1000 -stock 100 -soldout-flag=true

# 同样 4 个进程、各 8000 次，连接池从 16 加到 64。
run herd-i4-w64 sellout -note "4 个进程各 64 条连接。在途请求跟着连接池变大。" \
  -instances 4 -workers 64 -attempts 8000 -stock 100 -soldout-flag=true
run sellout-i8-burst2 sellout -note "8 个进程各 16 条连接，但本地突发只有 2。额度每秒 10 万，限制的是同时进 Redis 的数量。" \
  -instances 8 -workers 16 -attempts 8000 -stock 100 -soldout-flag=true -gate local -quota 100000 -burst 2

# 库存一开始就是 0。标记生效后，2 秒窗口里几乎不再进 Redis。
run soldout-hold-i1 soldout-hold -note "库存是 0。1 个进程 16 条连接，标记打开，跑 2 秒。" \
  -instances 1 -workers 16 -duration 2s -stock 0 -soldout-flag=true
run soldout-hold-i8 soldout-hold -note "库存是 0。8 个进程各 16 条连接，标记打开，跑 2 秒。" \
  -instances 8 -workers 16 -duration 2s -stock 0 -soldout-flag=true
run soldout-open-i1 soldout-open -note "库存是 0。1 个进程没有标记，2 秒都打在 Redis 上。" \
  -instances 1 -workers 16 -duration 2s -stock 0 -soldout-flag=false
run soldout-open-i8 soldout-open -note "库存是 0。8 个进程没有标记，仍然是同一个 Redis。" \
  -instances 8 -workers 16 -duration 2s -stock 0 -soldout-flag=false

# 库存远大于窗口，标记帮不上忙。本地额度决定 Redis 看到多少成功扣减。
run flood-open-i1 flood-open -note "库存 500 万。1 个进程 16 条连接，2 秒，不限额度。" \
  -instances 1 -workers 16 -duration 2s -stock 5000000 -soldout-flag=false -gate none
run flood-open-i4 flood-open -note "库存 500 万。4 个进程各 8 条连接，2 秒，不限额度。" \
  -instances 4 -workers 8 -duration 2s -stock 5000000 -soldout-flag=false -gate none
run flood-quota-4 flood-quota -note "4 个进程各 2000 次/秒，突发 100。桶初始是满的。" \
  -instances 4 -workers 8 -duration 3s -stock 5000000 -soldout-flag=true -gate local -quota 2000 -burst 100
run flood-quota-8 flood-quota -note "8 个进程各 1000 次/秒，突发 50。合计额度和 4 进程那组一样。" \
  -instances 8 -workers 4 -duration 3s -stock 5000000 -soldout-flag=true -gate local -quota 1000 -burst 50

run undersell undersell -note "额度合计约 2000 次/秒，库存 5 万，只跑 2 秒，卖不完。" \
  -instances 4 -workers 8 -duration 2s -stock 50000 -soldout-flag=true -gate local -quota 500 -burst 100
run drain drain -note "额度合计约 8000 次/秒，库存 1500，2 秒内卖完，然后标记停住。" \
  -instances 4 -workers 8 -duration 2s -stock 1500 -soldout-flag=true -gate local -quota 2000 -burst 100

run restock-stale restock-stale -note "先卖完 50。标记不清，再补 50。没有探测，补的库存卖不动。" \
  -instances 4 -workers 8 -attempts 2000 -stock 50 -soldout-flag=true -phases 2 -phase2 300ms -restock 50 -probe 0s
run restock-probe restock-probe -note "同样先卖完再补 50。每个进程 20 毫秒探测一次，探测到有货就重新打开。" \
  -instances 4 -workers 8 -attempts 2000 -stock 50 -soldout-flag=true -phases 2 -phase2 500ms -restock 50 -probe 20ms

run bad-instance bad-instance -note "3 个进程各 1000 次/秒，1 个进程不限额度。本地额度挡不住那个进程。" \
  -instances 4 -workers 8 -duration 2s -stock 5000000 -soldout-flag=false -gate local -quota 1000 -burst 100 -bad=true
run redis-gate redis-gate -note "限流放在 Redis 里。窗口 60 秒、上限 2000。被拒绝的请求也进了 Redis。" \
  -instances 1 -workers 16 -duration 2s -stock 0 -soldout-flag=false -gate redis -limit 2000 -window-ms 60000
run local-gate local-gate -note "同样大约 2000 次/秒，额度在进程内。被拒绝的请求不进 Redis。" \
  -instances 1 -workers 16 -duration 2s -stock 0 -soldout-flag=false -gate local -quota 2000 -burst 100

stop_redis
trap - EXIT
echo "ALL PASS"
