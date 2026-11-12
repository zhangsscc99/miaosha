#!/usr/bin/env bash
# 用 cgroup v2 的 cpu.max / cpu.weight / memory.max 测 Kubernetes 限额。
# 不启动 dockerd，也不启动 kubelet。需要能对父 cgroup 执行 sudo。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results /tmp/miaosha-redis-6379 /tmp/miaosha-redis-6380

if [[ "$(getconf CLK_TCK)" != "100" ]]; then
  echo "CLK_TCK is $(getconf CLK_TCK), cpu percent divides by 100" >&2
  exit 1
fi
sudo -n true

if [[ -n "${REDIS_SERVER:-}" ]]; then
  REDIS_SERVER="$(command -v "$REDIS_SERVER" || true)"
elif [[ -x /tmp/redis-7015/bin/redis-server ]]; then
  REDIS_SERVER=/tmp/redis-7015/bin/redis-server
elif command -v redis-server >/dev/null 2>&1; then
  REDIS_SERVER="$(command -v redis-server)"
else
  echo "redis-server not found; build 7.0.15 into /tmp/redis-7015 or set REDIS_SERVER" >&2
  exit 1
fi
REDIS_CLI="${REDIS_CLI:-$(dirname "$REDIS_SERVER")/redis-cli}"

rel="$(awk -F: 'NR==1 {print $3}' /proc/self/cgroup)"
pod="$(dirname "$(dirname "$rel")")"
BASE="/sys/fs/cgroup${pod}/miaosha-cpu"
if [[ ! -f "/sys/fs/cgroup${pod}/cpu.max" ]]; then
  echo "parent cgroup has no cpu.max: /sys/fs/cgroup${pod}" >&2
  exit 1
fi

stop_redis() {
  "$REDIS_CLI" -p 6379 shutdown nosave >/dev/null 2>&1 || true
  "$REDIS_CLI" -p 6380 shutdown nosave >/dev/null 2>&1 || true
  sleep 0.2
}

wait_ping() {
  local port="$1"
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if "$REDIS_CLI" -p "$port" ping 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.2
  done
  echo "redis :$port did not start" >&2
  return 1
}

# 进程挪进 cgroup 之后，已经分配的页不一定跟着走。内存限额要在启动前套上。
# 不用 sudo 启动 Redis：sudo 的 PrivateTmp 写不了这个 /tmp 日志。
start_in_cgroup() {
  local cg="$1"
  local conf="$2"
  local port="$3"
  bash -c "echo \$\$ | sudo tee '$cg/cgroup.procs' >/dev/null; exec '$REDIS_SERVER' '$conf'"
  wait_ping "$port"
}

move_pid() {
  local dest="$1"
  local pid="$2"
  echo "$pid" | sudo tee "$dest/cgroup.procs" >/dev/null
}

cleanup_tree() {
  if [[ -d "$BASE" ]]; then
    # 只挪走我们自己的 redis。cgroup 里如果还有别的进程，rmdir 会失败，脚本继续。
    sudo find "$BASE" -name cgroup.procs -print 2>/dev/null | while read -r procs; do
      while read -r pid; do
        [[ -z "$pid" ]] && continue
        comm="$(cat "/proc/$pid/comm" 2>/dev/null || true)"
        if [[ "$comm" == "redis-server" ]]; then
          sudo kill "$pid" 2>/dev/null || true
        fi
      done < "$procs"
    done
    sleep 0.2
    sudo find "$BASE" -depth -type d -print 2>/dev/null | while read -r d; do
      sudo rmdir "$d" 2>/dev/null || true
    done
  fi
}

trap stop_redis EXIT
stop_redis
cleanup_tree

sudo mkdir -p "$BASE"
echo '+cpu +memory' | sudo tee "$BASE/cgroup.subtree_control" >/dev/null
for name in qmax qw1 q2000 q1000 q500 q200 q500p shared pack-a pack-b mem128 mem256 mem32 sharep; do
  sudo mkdir -p "$BASE/$name"
done
echo '+cpu +memory' | sudo tee "$BASE/sharep/cgroup.subtree_control" >/dev/null
sudo mkdir -p "$BASE/sharep/w250" "$BASE/sharep/w1000"

# kubelet 把 limit 写成 quota/period，默认 period 100000 微秒。单位是微秒。
echo 'max 100000' | sudo tee "$BASE/qmax/cpu.max" >/dev/null
echo 'max 100000' | sudo tee "$BASE/qw1/cpu.max" >/dev/null
echo 1 | sudo tee "$BASE/qw1/cpu.weight" >/dev/null
echo '200000 100000' | sudo tee "$BASE/q2000/cpu.max" >/dev/null
echo '100000 100000' | sudo tee "$BASE/q1000/cpu.max" >/dev/null
echo '50000 100000' | sudo tee "$BASE/q500/cpu.max" >/dev/null
echo '20000 100000' | sudo tee "$BASE/q200/cpu.max" >/dev/null
echo '500000 1000000' | sudo tee "$BASE/q500p/cpu.max" >/dev/null
echo '100000 100000' | sudo tee "$BASE/shared/cpu.max" >/dev/null
echo '100000 100000' | sudo tee "$BASE/pack-a/cpu.max" >/dev/null
echo '100000 100000' | sudo tee "$BASE/pack-b/cpu.max" >/dev/null
echo '100000 100000' | sudo tee "$BASE/sharep/cpu.max" >/dev/null
# kubelet：shares = milliCPU * 1024 / 1000，weight = ((shares - 2) * 9999) / 262142 + 1。
# 250m -> 10，1000m -> 39。
echo 10 | sudo tee "$BASE/sharep/w250/cpu.weight" >/dev/null
echo 39 | sudo tee "$BASE/sharep/w1000/cpu.weight" >/dev/null
echo $((128 * 1024 * 1024)) | sudo tee "$BASE/mem128/memory.max" >/dev/null
echo $((256 * 1024 * 1024)) | sudo tee "$BASE/mem256/memory.max" >/dev/null
echo $((32 * 1024 * 1024)) | sudo tee "$BASE/mem32/memory.max" >/dev/null

if ! cat "$BASE/q500/cpu.max" >/dev/null 2>&1; then
  echo "cpu.max is not readable by this user" >&2
  exit 1
fi

CGO_ENABLED=0 go build -o /tmp/miaosha-cgroup .
BIN=/tmp/miaosha-cgroup

"$REDIS_SERVER" "$PWD/redis-64mb-6379.conf"
wait_ping 6379
PID6379="$(cat /tmp/miaosha-redis-6379.pid)"

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "redis=$("$REDIS_SERVER" --version)"
  echo "nproc=$(nproc)"
  echo "clk_tck=$(getconf CLK_TCK)"
  echo "base=$BASE"
  echo "self_cgroup=$rel"
  echo "pod_cpu_max=$(cat "/sys/fs/cgroup${pod}/cpu.max")"
  echo "pod_cpu_weight=$(cat "/sys/fs/cgroup${pod}/cpu.weight")"
  lscpu | awk -F: '/Model name|CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -2
  echo "--- cpu.max files ---"
  for name in qmax qw1 q2000 q1000 q500 q200 q500p shared pack-a sharep; do
    echo "$name cpu.max=$(cat "$BASE/$name/cpu.max") weight=$(cat "$BASE/$name/cpu.weight")"
  done
  echo "w250 weight=$(cat "$BASE/sharep/w250/cpu.weight")"
  echo "w1000 weight=$(cat "$BASE/sharep/w1000/cpu.weight")"
  echo "mem128=$(cat "$BASE/mem128/memory.max")"
  echo "mem256=$(cat "$BASE/mem256/memory.max")"
  echo "mem32=$(cat "$BASE/mem32/memory.max")"
  "$REDIS_CLI" -p 6379 INFO server | awk -F: '/redis_version|process_id|tcp_port|os|hz|mem_allocator/'
  "$REDIS_CLI" -p 6379 CONFIG GET maxmemory
  "$REDIS_CLI" -p 6379 CONFIG GET appendonly
} > results/env.txt

run() {
  local file="$1"
  shift
  echo "=== ${file} ==="
  "$BIN" -out "results/${file}.json" "$@"
}

move_pid "$BASE/qmax" "$PID6379"
run soldout-max -scenario soldout -cgroup "$BASE/qmax" -workers 16 -duration 4s -warmup 1s \
  -note "cpu.max=max，cpu.weight 保持默认 100。这是这台机器上不限额的售罄读，后面的比例都除以它。"

move_pid "$BASE/qw1" "$PID6379"
run soldout-weight1 -scenario soldout -cgroup "$BASE/qw1" -workers 16 -duration 4s -warmup 1s \
  -note "cpu.weight=1，cpu.max 仍是 max。机器有空闲核时，最小 weight 不应当成限额。"

move_pid "$BASE/q2000" "$PID6379"
run soldout-2000m -scenario soldout -cgroup "$BASE/q2000" -workers 16 -duration 4s -warmup 1s \
  -note "limits.cpu=2，也就是 cpu.max=200000 100000。Redis 只有一个执行线程。"

move_pid "$BASE/q1000" "$PID6379"
run soldout-1000m -scenario soldout -cgroup "$BASE/q1000" -workers 16 -duration 4s -warmup 1s \
  -note "limits.cpu=1，cpu.max=100000 100000。单线程刚好用满这一个核。"

move_pid "$BASE/q500" "$PID6379"
run soldout-500m -scenario soldout -cgroup "$BASE/q500" -workers 16 -duration 4s -warmup 1s \
  -note "limits.cpu=500m，周期 100 毫秒，16 条同步连接。平均大约半个核，周期内的配额用完会被按住。"
run soldout-500m-w128 -scenario soldout -cgroup "$BASE/q500" -workers 128 -duration 4s -warmup 1s \
  -note "同样是 500m、100 毫秒周期，连接加到 128。每次被按住时挂起的请求更多，用来看 p99 会不会被这批等待拖进去。"

move_pid "$BASE/q200" "$PID6379"
run soldout-200m -scenario soldout -cgroup "$BASE/q200" -workers 16 -duration 4s -warmup 1s \
  -note "limits.cpu=200m，周期 100 毫秒。用来看配额更紧时尾巴变成什么样。"

move_pid "$BASE/q500p" "$PID6379"
run soldout-500m-1s -scenario soldout -cgroup "$BASE/q500p" -workers 16 -duration 4s -warmup 1s \
  -note "仍然是 0.5 个核，但周期改成 1 秒：cpu.max=500000 1000000。平均 CPU 和 500m 同一档，周期更长。"

move_pid "$BASE/mem128" "$PID6379"
run soldout-mem128 -scenario soldout -cgroup "$BASE/mem128" -workers 16 -duration 4s -warmup 1s \
  -note "memory.max=128Mi，cpu 不限额。售罄读的工作集应该远小于这个限额，进程不该被杀掉。"

move_pid "$BASE/qmax" "$PID6379"
run success-max -scenario success -cgroup "$BASE/qmax" -workers 16 -duration 4s -warmup 1s -stock 5000000 \
  -note "成功扣减，cpu.max=max。一人一单的集合会变大，用来和只读的售罄路径对照。"

move_pid "$BASE/q2000" "$PID6379"
run success-2000m -scenario success -cgroup "$BASE/q2000" -workers 16 -duration 4s -warmup 1s -stock 5000000 \
  -note "成功扣减，limits.cpu=2。多出来的核不应提高这一条热 key 的 QPS。"

move_pid "$BASE/q500" "$PID6379"
run success-500m -scenario success -cgroup "$BASE/q500" -workers 16 -duration 4s -warmup 1s -stock 5000000 \
  -note "成功扣减，limits.cpu=500m，周期 100 毫秒。"

# 应用和 Redis 塞进同一个 1 核限额。压测进程启动后立刻挪进去，预热有 1 秒。
move_pid "$BASE/shared" "$PID6379"
echo "=== soldout-shared-1000m ==="
"$BIN" -scenario soldout -cgroup "$BASE/shared" -workers 16 -duration 4s -warmup 1s \
  -note "Redis 和压测进程在同一个 cpu.max=100000 100000 里。模拟一个容器里既跑 Redis 又跑业务，限额只有 1 核。" \
  -out results/soldout-shared-1000m.json &
bench_pid=$!
move_pid "$BASE/shared" "$bench_pid"
wait "$bench_pid"

"$REDIS_SERVER" "$PWD/redis-64mb-6380.conf"
wait_ping 6380
PID6380="$(cat /tmp/miaosha-redis-6380.pid)"
move_pid "$BASE/pack-a" "$PID6379"
move_pid "$BASE/pack-b" "$PID6380"
start_ms=$(($(date +%s%3N) + 800))
echo "=== pack two redis, each cpu.max=1 ==="
"$BIN" -scenario soldout -cgroup "$BASE/pack-a" -addr 127.0.0.1:6379 -workers 16 -duration 4s -warmup 1s \
  -start-unix-ms "$start_ms" \
  -note "两个 Redis 同时跑，各自 cpu.max=100000 100000。客户端在限额外面。这是 6379。" \
  -out results/pack-a.json &
p1=$!
"$BIN" -scenario soldout -cgroup "$BASE/pack-b" -addr 127.0.0.1:6380 -workers 16 -duration 4s -warmup 1s \
  -start-unix-ms "$start_ms" \
  -note "两个 Redis 同时跑，各自 cpu.max=100000 100000。客户端在限额外面。这是 6380。" \
  -out results/pack-b.json &
p2=$!
wait "$p1"
wait "$p2"

move_pid "$BASE/sharep/w250" "$PID6379"
move_pid "$BASE/sharep/w1000" "$PID6380"
start_ms=$(($(date +%s%3N) + 800))
echo "=== weight 250m vs 1000m under parent 1 core ==="
"$BIN" -scenario soldout -cgroup "$BASE/sharep/w250" -parent-cgroup "$BASE/sharep" \
  -addr 127.0.0.1:6379 -workers 16 -duration 4s -warmup 1s -start-unix-ms "$start_ms" \
  -note "父 cgroup cpu.max=1 核。这个子 cgroup 的 cpu.weight=10，对应 kubelet 对 250m request 的换算。客户端在父 cgroup 外面。" \
  -out results/weight-250m.json &
p1=$!
"$BIN" -scenario soldout -cgroup "$BASE/sharep/w1000" -parent-cgroup "$BASE/sharep" \
  -addr 127.0.0.1:6380 -workers 16 -duration 4s -warmup 1s -start-unix-ms "$start_ms" \
  -note "同一个 1 核的父 cgroup。cpu.weight=39，对应 kubelet 对 1000m request 的换算。" \
  -out results/weight-1000m.json &
p2=$!
wait "$p1"
wait "$p2"

# 同一对权重再跑 12 秒，看 4 秒的比例是不是没稳住。
move_pid "$BASE/sharep/w250" "$PID6379"
move_pid "$BASE/sharep/w1000" "$PID6380"
start_ms=$(($(date +%s%3N) + 800))
echo "=== weight 12s ==="
"$BIN" -scenario soldout -cgroup "$BASE/sharep/w250" -parent-cgroup "$BASE/sharep" \
  -addr 127.0.0.1:6379 -workers 16 -duration 12s -warmup 1s -start-unix-ms "$start_ms" \
  -note "和 weight-250m 同一对 cgroup，窗口加到 12 秒。weight=10。" \
  -out results/weight-250m-12s.json &
p1=$!
"$BIN" -scenario soldout -cgroup "$BASE/sharep/w1000" -parent-cgroup "$BASE/sharep" \
  -addr 127.0.0.1:6380 -workers 16 -duration 12s -warmup 1s -start-unix-ms "$start_ms" \
  -note "和 weight-1000m 同一对 cgroup，窗口加到 12 秒。weight=39。" \
  -out results/weight-1000m-12s.json &
p2=$!
wait "$p1"
wait "$p2"

# 内存：先让 Redis 自己拒绝。6380 不再参与。这两组在对应 cgroup 里启动，避免老页面留在原来的 cgroup。
"$REDIS_CLI" -p 6380 shutdown nosave >/dev/null 2>&1 || true
"$REDIS_CLI" -p 6379 shutdown nosave >/dev/null 2>&1 || true
start_in_cgroup "$BASE/mem256" "$PWD/redis-64mb-6379.conf" 6379
PID6379="$(cat /tmp/miaosha-redis-6379.pid)"
echo "mem256 pid=$PID6379 cgroup=$(cat /proc/$PID6379/cgroup) current=$(cat "$BASE/mem256/memory.current")"
echo "mem256 current=$(cat "$BASE/mem256/memory.current")" > results/mem-startup.txt
run mem-reject -scenario mem-reject -cgroup "$BASE/mem256" -maxmemory 8388608 -value-bytes $((256 * 1024)) -keys 80 \
  -note "在 memory.max=256Mi 的 cgroup 里启动。Redis maxmemory=8MiB，noeviction。写超之后应返回 OOM，oom_kill 仍是 0。"

"$REDIS_CLI" -p 6379 shutdown nosave >/dev/null 2>&1 || true
start_in_cgroup "$BASE/mem32" "$PWD/redis-64mb-6379.conf" 6379
PID6379="$(cat /tmp/miaosha-redis-6379.pid)"
echo "mem32 pid=$PID6379 cgroup=$(cat /proc/$PID6379/cgroup) current=$(cat "$BASE/mem32/memory.current")"
echo "mem32 current=$(cat "$BASE/mem32/memory.current")" >> results/mem-startup.txt
run mem-kill -scenario mem-kill -cgroup "$BASE/mem32" -maxmemory $((64 * 1024 * 1024)) -value-bytes $((256 * 1024)) -keys 200 \
  -note "在 memory.max=32Mi 的 cgroup 里启动。Redis maxmemory=64MiB。cgroup 比 Redis 自己的上限紧，进程应被杀掉。"

echo "ALL PASS"
