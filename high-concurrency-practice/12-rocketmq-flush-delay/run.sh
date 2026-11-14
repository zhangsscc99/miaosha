#!/usr/bin/env bash
# 拉起一个本机 RocketMQ 5.5.1：NameServer + 一个 broker。测完关掉。
# 不启动 Docker，不启动 Kubernetes，不连接配置文件里的远端主机。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results

if [[ "$(getconf CLK_TCK)" != "100" ]]; then
  echo "CLK_TCK is $(getconf CLK_TCK), cpu percent divides by 100" >&2
  exit 1
fi

RMQ="${ROCKETMQ_HOME:-/tmp/rocketmq-551/rocketmq-all-5.5.1-bin-release}"
if [[ ! -x "$RMQ/bin/mqbroker" ]]; then
  mkdir -p /tmp/rocketmq-551
  curl -fL --retry 3 --retry-delay 2 -o /tmp/rocketmq-all-5.5.1-bin-release.zip \
    https://downloads.apache.org/rocketmq/5.5.1/rocketmq-all-5.5.1-bin-release.zip
  unzip -q -o /tmp/rocketmq-all-5.5.1-bin-release.zip -d /tmp/rocketmq-551
fi

# 发行版脚本写死 -Xms8g -Xmx8g 和 MaxDirectMemorySize=15g。只改 /tmp 里的这一份。
sed -i 's/-Xms8g -Xmx8g/-Xms512m -Xmx512m/' "$RMQ/bin/runbroker.sh"
sed -i 's/-XX:MaxDirectMemorySize=15g/-XX:MaxDirectMemorySize=256m/' "$RMQ/bin/runbroker.sh"
sed -i 's/-Xms4g -Xmx4g/-Xms128m -Xmx128m/g' "$RMQ/bin/runserver.sh"
sed -i 's/G1HeapRegionSize=16m/G1HeapRegionSize=4m/g' "$RMQ/bin/runbroker.sh"
sed -i 's/G1HeapRegionSize=16m/G1HeapRegionSize=4m/g' "$RMQ/bin/runserver.sh"

BASE=/tmp/miaosha-rocketmq
mkdir -p "$BASE/logs"

# 只认 java 命令行。pgrep -f 会匹配到自己，这里不走那条路。
java_pid() {
  local pat="$1" pid cmd
  for pid in /proc/[0-9]*; do
    pid="${pid#/proc/}"
    cmd="$({ tr '\0' ' ' <"/proc/$pid/cmdline"; } 2>/dev/null || true)"
    [[ "$cmd" == *java* && "$cmd" == *"$pat"* ]] || continue
    echo "$pid"
    return 0
  done
  return 1
}

stop_java() {
  local pat="$1" i pid
  for i in $(seq 1 50); do
    pid="$(java_pid "$pat" || true)"
    if [[ -z "$pid" ]]; then
      return 0
    fi
    kill -9 "$pid" >/dev/null 2>&1 || true
    sleep 0.1
  done
  return 1
}

stop_broker() { stop_java "org.apache.rocketmq.broker.BrokerStartup"; }
stop_namesrv() { stop_java "org.apache.rocketmq.namesrv.NamesrvStartup"; }

cleanup() {
  stop_broker || true
  stop_namesrv || true
}
trap cleanup EXIT

cleanup || true

broker_pid() { java_pid "org.apache.rocketmq.broker.BrokerStartup"; }
namesrv_pid() { java_pid "org.apache.rocketmq.namesrv.NamesrvStartup"; }

start_namesrv() {
  nohup "$RMQ/bin/mqnamesrv" >"$BASE/logs/namesrv.out" 2>&1 &
  local i
  for i in $(seq 1 50); do
    if (echo >/dev/tcp/127.0.0.1/9876) >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "namesrv did not listen" >&2
  tail -n 40 "$BASE/logs/namesrv.out" >&2 || true
  tail -n 40 "$HOME/logs/rocketmqlogs/namesrv.log" >&2 || true
  exit 1
}

start_broker() {
  local conf="$1"
  : >"$BASE/logs/broker.out"
  nohup "$RMQ/bin/mqbroker" -c "$conf" >"$BASE/logs/broker.out" 2>&1 &
  local i
  for i in $(seq 1 80); do
    if (echo >/dev/tcp/127.0.0.1/10911) >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "broker did not listen: $conf" >&2
  tail -n 60 "$BASE/logs/broker.out" >&2 || true
  tail -n 80 "$HOME/logs/rocketmqlogs/broker.log" >&2 || true
  exit 1
}

export GOTOOLCHAIN=local
go mod tidy
go build -o /tmp/miaosha-rmqbench .
BIN=/tmp/miaosha-rmqbench

wait_ready() {
  local i
  for i in $(seq 1 8); do
    if "$BIN" -scenario ping -topic ms12_ready -send-timeout 15s -out results/ready.json -broker-pid "$(broker_pid || true)" >/tmp/miaosha-rmq-ready.err 2>&1; then
      if python3 -c 'import json,sys; d=json.load(open("results/ready.json")); sys.exit(0 if d.get("pass") else 1)'; then
        return 0
      fi
    fi
    sleep 0.4
  done
  echo "ping did not pass" >&2
  cat /tmp/miaosha-rmq-ready.err >&2 || true
  cat results/ready.json >&2 || true
  tail -n 40 "$HOME/logs/rocketmqlogs/broker.log" >&2 || true
  exit 1
}

wipe_store() {
  stop_broker
  rm -rf "$BASE/store"
  mkdir -p "$BASE/store"
}

kill_broker_now() {
  local pid
  pid="$(broker_pid || true)"
  if [[ -n "${pid}" ]]; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  stop_broker
}

run() {
  "$BIN" -broker-pid "$(broker_pid || true)" "$@"
}

rm -rf "$BASE/store"
mkdir -p "$BASE/store"
start_namesrv
start_broker "$PWD/broker-async.conf"
wait_ready

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "java=$(java -version 2>&1 | head -n 1)"
  echo "rocketmq_home=$RMQ"
  echo "namesrv_pid=$(namesrv_pid || true)"
  echo "broker_pid=$(broker_pid || true)"
  echo "nproc=$(nproc)"
  echo "--- heap ---"
  grep -nE "Xms|Xmx|MaxDirect|G1HeapRegion" "$RMQ/bin/runbroker.sh" "$RMQ/bin/runserver.sh" || true
  echo "--- broker.conf ---"
  cat broker-async.conf
  echo "--- machine ---"
  lscpu | awk -F: '/Model name|^CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -n 2
  echo "broker_rss_kb=$(awk '/VmRSS/ {print $2}' /proc/$(broker_pid)/status)"
  echo "namesrv_rss_kb=$(awk '/VmRSS/ {print $2}' /proc/$(namesrv_pid)/status)"
} > results/env.txt

run -scenario delay -delay-level 1 -messages 30 -topic ms12_delay1 -out results/delay1.json
run -scenario delay -delay-level 2 -messages 20 -topic ms12_delay2 -out results/delay2.json
run -scenario tag -messages 40 -topic ms12_tag -out results/tag.json
run -scenario assign -queues 1 -consumers 2 -messages 1000 -topic ms12_assign1 -out results/assign-1.json
run -scenario assign -queues 4 -consumers 2 -messages 1000 -topic ms12_assign4 -out results/assign-4.json
run -scenario orderly -messages 32 -topic ms12_orderly -out results/orderly.json
run -scenario concurrent -messages 32 -topic ms12_concurrent -out results/concurrent.json
run -scenario retry -messages 8 -topic ms12_retry -out results/retry.json
run -scenario dedup -topic ms12_dedup -out results/dedup.json
run -scenario ack-early -topic ms12_ack -out results/ack-early.json
run -scenario broadcast -messages 40 -topic ms12_broadcast -out results/broadcast.json
run -scenario tx -topic ms12_tx -out results/tx.json

run -scenario produce -queues 1 -workers 8 -duration 4s -warmup 1s -payload 200 -topic ms12_p1 -out results/produce-q1.json
run -scenario produce -queues 4 -workers 8 -duration 4s -warmup 1s -payload 200 -topic ms12_p4 -out results/produce-q4.json
run -scenario produce -queues 8 -workers 8 -duration 4s -warmup 1s -payload 200 -topic ms12_p8 -out results/produce-q8.json

run -scenario consume -queues 1 -consumers 1 -threads 1 -messages 30000 -payload 200 -topic ms12_c1t1 -out results/consume-q1-t1.json
run -scenario consume -queues 1 -consumers 1 -threads 16 -messages 30000 -payload 200 -topic ms12_c1t16 -out results/consume-q1-t16.json
run -scenario consume -queues 4 -consumers 4 -threads 4 -messages 30000 -payload 200 -topic ms12_c4 -out results/consume-q4-c4.json
run -scenario consume -ids -queues 4 -consumers 4 -threads 4 -messages 30000 -topic ms12_c4_ids -out results/consume-q4-ids.json

run -scenario winners -messages 5000 -payload 200 -topic ms12_winners -out results/winners.json
run -scenario disk -messages 100000 -payload 200 -topic ms12_disk -store "$BASE/store" -out results/disk.json
run -scenario produce -oneway -queues 1 -workers 4 -duration 2s -warmup 0s -payload 200 -topic ms12_oneway -out results/produce-oneway.json

run -scenario namesrv-down -namesrv-pid "$(namesrv_pid)" -topic ms12_ns -out results/namesrv-down.json
start_namesrv
wait_ready

# 默认异步刷盘、不使用堆外缓冲。kill -9 两次，看 mmap 上的消息还在不在。
for i in 1 2; do
  run -scenario seed -messages 100 -payload 200 -queues 1 -topic "ms12_crash_mmap_$i" -out "results/crash-mmap-seed-$i.json"
  kill_broker_now
  start_broker "$PWD/broker-async.conf"
  wait_ready
  run -scenario count -topic "ms12_crash_mmap_$i" -expect 100 -out "results/crash-mmap-kept-$i.json"
done

wipe_store
start_broker "$PWD/broker-pool.conf"
wait_ready
run -scenario seed -messages 100 -payload 200 -queues 1 -send-timeout 15s -topic ms12_crash_pool -out results/crash-pool-seed.json
kill_broker_now
start_broker "$PWD/broker-pool.conf"
wait_ready
run -scenario count -topic ms12_crash_pool -expect-less 100 -out results/crash-pool-kept.json

wipe_store
start_broker "$PWD/broker-sync.conf"
wait_ready
run -scenario seed -messages 100 -payload 200 -queues 1 -send-timeout 10s -topic ms12_crash_sync -out results/crash-sync-seed.json
kill_broker_now
start_broker "$PWD/broker-sync.conf"
wait_ready
run -scenario count -topic ms12_crash_sync -expect 100 -out results/crash-sync-kept.json
run -scenario produce -queues 1 -workers 8 -duration 4s -warmup 1s -payload 200 -send-timeout 10s -topic ms12_sync_q1 -out results/produce-sync-q1.json
run -scenario produce -queues 4 -workers 8 -duration 4s -warmup 1s -payload 200 -send-timeout 10s -topic ms12_sync_q4 -out results/produce-sync-q4.json

python3 - <<'PY'
import json, pathlib, sys
bad = 0
for p in sorted(pathlib.Path("results").glob("*.json")):
    if p.name == "ready.json":
        continue
    d = json.loads(p.read_text())
    if p.name == "consume-q4-c4.json" and d.get("deliveries", 0) > d.get("messages", 0):
        print("ok-extra", p.name, (d.get("detail") or "")[:200])
        continue
    # 堆外缓冲把后续发送挡成 system busy。成功条数小于请求条数就是这组的结果。
    if p.name == "crash-pool-seed.json" and d.get("success", 0) < d.get("messages", 0):
        print("ok-busy", p.name, (d.get("detail") or "")[:200])
        continue
    if not d.get("pass"):
        print("FAIL", p.name, d.get("detail"))
        bad += 1
    else:
        print("ok", p.name, (d.get("detail") or "")[:200])
print("bad", bad)
sys.exit(1 if bad else 0)
PY
