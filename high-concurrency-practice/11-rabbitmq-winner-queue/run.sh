#!/usr/bin/env bash
# 拉起一个本机 RabbitMQ，水位是空节点 RSS 再加 256MiB。测完关掉。
# 不启动 Docker，不启动 RocketMQ，不连接配置文件里的远端主机。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results

if [[ "$(getconf CLK_TCK)" != "100" ]]; then
  echo "CLK_TCK is $(getconf CLK_TCK), cpu percent divides by 100" >&2
  exit 1
fi
if ! command -v erl >/dev/null || ! command -v rabbitmqctl >/dev/null; then
  echo "need Erlang OTP 27 and RabbitMQ 4.4 on PATH; this run used the Team RabbitMQ noble apt repo" >&2
  exit 1
fi

BASE=/tmp/miaosha-rabbitmq
bash broker.sh stop >/dev/null 2>&1 || true
bash broker.sh kill >/dev/null 2>&1 || true
sudo rm -rf "$BASE"
bash broker.sh start

bash broker.sh ctl --formatter json status > results/status-idle.json
python3 - <<'PY'
import json
d = json.load(open("results/status-idle.json"))
rss = d["memory"]["total"]["rss"]
limit = rss + 256 * 1024 * 1024
open("/tmp/miaosha-rmq-wm.txt", "w").write(f"{limit}\n{rss}\n")
print(f"idle_rss={rss} limit={limit}")
PY
limit="$(sed -n '1p' /tmp/miaosha-rmq-wm.txt)"
idle_rss="$(sed -n '2p' /tmp/miaosha-rmq-wm.txt)"
echo "$limit" | sudo tee "$BASE/watermark.bytes" >/dev/null
sudo chmod 644 "$BASE/watermark.bytes"
bash broker.sh ctl set_vm_memory_high_watermark absolute "$limit"
bash broker.sh ctl --formatter json status > results/status-watermark.json

export GOTOOLCHAIN=local
go mod tidy
go build -o /tmp/miaosha-rmqbench .

pid="$(bash broker.sh pid)"
{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "erl=$(erl -noshell -eval 'io:format("~s",[erlang:system_info(system_version)]), halt().')"
  echo "rabbitmq=$(bash broker.sh ctl version | head -n 1)"
  echo "broker_pid=$pid"
  echo "idle_rss_bytes=$idle_rss"
  echo "vm_memory_high_watermark_bytes=$limit"
  echo "headroom_bytes=$((256 * 1024 * 1024))"
  echo "nproc=$(nproc)"
  lscpu | awk -F: '/Model name|^CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -n 2
  python3 - <<'PY'
import json
d=json.load(open("results/status-watermark.json"))
mem=d["memory"]["total"]
print(f"strategy={d.get('vm_memory_calculation_strategy')}")
print(f"limit={d.get('vm_memory_high_watermark_limit')}")
print(f"rss={mem.get('rss')} allocated={mem.get('allocated')} erlang={mem.get('erlang')}")
print(f"erlang_version={d.get('erlang_version')}")
print(f"rabbitmq_version={d.get('rabbitmq_version')}")
print(f"schedulers_line={d.get('erlang_version')}")
PY
  echo "--- schedulers ---"
  bash broker.sh ctl eval 'erlang:system_info(schedulers_online).'
} > results/env.txt

BIN=/tmp/miaosha-rmqbench
run() { "$BIN" "$@"; }

run -scenario unroutable -out results/unroutable.json
run -scenario unroutable-mandatory -out results/unroutable-mandatory.json
run -scenario manual-stall -out results/manual-stall.json
run -scenario auto-return -out results/auto-return.json
run -scenario ack-before-insert -out results/ack-before-insert.json
run -scenario ack-current-prefetch1 -out results/ack-current-prefetch1.json
run -scenario ack-current-prefetch50 -out results/ack-current-prefetch50.json
run -scenario ack-max-prefetch50 -out results/ack-max-prefetch50.json
run -scenario redelivery -out results/redelivery.json
run -scenario dedup -out results/dedup.json
run -scenario reject-publish -out results/reject-publish.json
run -scenario drop-head -out results/drop-head.json
run -scenario competing -out results/competing.json
run -scenario fanout -out results/fanout.json
run -scenario requeue -out results/requeue.json
run -scenario dlx -out results/dlx.json
run -scenario retry -out results/retry.json

# 非持久队列在 4.4 上默认被拒绝。这一组是持久队列、非持久消息、不等 confirm。
run -scenario produce -queue ms11-p-transient -durable=true -persistent=false -confirms=false \
  -workers 8 -duration 4s -warmup 1s -payload 200 -out results/produce-transient-noconfirm.json
run -scenario produce -queue ms11-p-drain -durable=true -persistent=true -confirms=true -drain \
  -workers 8 -prefetch 250 -duration 4s -warmup 1s -payload 200 -out results/produce-persist-drain.json
run -scenario produce -queue ms11-p-q4 -queues 4 -durable=true -persistent=true -confirms=true -drain \
  -workers 8 -prefetch 250 -duration 4s -warmup 1s -payload 200 -out results/produce-4q-drain.json
run -scenario produce -queue ms11-p-persist -durable=true -persistent=true -confirms=true \
  -workers 8 -duration 4s -warmup 0s -payload 200 -out results/produce-persist-nodrain.json
run -scenario produce -queue ms11-p-quorum -quorum -persistent=true -confirms=true -drain \
  -workers 8 -prefetch 250 -duration 4s -warmup 1s -payload 200 -out results/produce-quorum-drain.json
# 同一水位下 quorum 会告警。再把水位临时放到 2GiB，看单节点 quorum 在不流控时的确认速率。
bash broker.sh ctl set_vm_memory_high_watermark absolute 2147483648
run -scenario produce -queue ms11-p-quorum-2g -quorum -persistent=true -confirms=true -drain \
  -workers 8 -prefetch 250 -duration 4s -warmup 1s -payload 200 -out results/produce-quorum-drain-2g.json
bash broker.sh ctl set_vm_memory_high_watermark absolute "$limit"

run -scenario consume -queue ms11-c-p1c1 -messages 100000 -workers 4 -consumers 1 -prefetch 1 \
  -payload 200 -out results/consume-prefetch1-c1.json
run -scenario consume -queue ms11-c-p1c10 -messages 100000 -workers 4 -consumers 10 -prefetch 1 \
  -payload 200 -out results/consume-prefetch1-c10.json
run -scenario consume -queue ms11-c-p250c1 -messages 100000 -workers 4 -consumers 1 -prefetch 250 \
  -payload 200 -out results/consume-prefetch250-c1.json
run -scenario consume -queue ms11-c-p250c4 -messages 100000 -workers 4 -consumers 4 -prefetch 250 \
  -payload 200 -out results/consume-prefetch250-c4.json

run -scenario winners -messages 5000 -workers 4 -prefetch 250 -payload 200 -out results/winners.json
run -scenario disk -messages 100000 -workers 4 -payload 200 -out results/disk.json

run -scenario wipe -out results/wipe.json
run -scenario restart-persistent -out results/restart-persistent.json
run -scenario restart-transient -out results/restart-transient.json
run -scenario restart-undurable -out results/restart-undurable.json
run -scenario restart-quorum -out results/restart-quorum.json

# 经典队列把正文放在磁盘上，256MiB 余量不一定在 20 秒里触顶。给到 90 秒。
bash broker.sh kill
bash broker.sh start
run -scenario alarm -workers 4 -payload 200 -duration 90s -out results/alarm.json

python3 - <<'PY'
import json, pathlib, sys
bad = 0
for p in sorted(pathlib.Path("results").glob("*.json")):
    if p.name.startswith("status-"):
        continue
    d = json.loads(p.read_text())
    if d.get("scenario") == "wipe":
        print("ok", p.name)
        continue
    if not d.get("pass"):
        print("FAIL", p.name, d.get("detail"))
        bad += 1
    else:
        print("ok", p.name, (d.get("detail") or "")[:180])
print("bad", bad)
sys.exit(1 if bad else 0)
PY

bash broker.sh stop >/dev/null 2>&1 || true
