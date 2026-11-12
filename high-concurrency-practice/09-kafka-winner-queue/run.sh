#!/usr/bin/env bash
# 拉起一个 256MB 堆的 Kafka 4.2.2（KRaft，broker 和 controller 在同一个进程），跑完关掉。
# 不启动 RabbitMQ，不启动 RocketMQ，不启动 dockerd。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results

if [[ "$(getconf CLK_TCK)" != "100" ]]; then
  echo "CLK_TCK is $(getconf CLK_TCK), cpu percent divides by 100" >&2
  exit 1
fi

KAFKA_HOME="${KAFKA_HOME:-/tmp/kafka-422/kafka_2.13-4.2.2}"
if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
  mkdir -p /tmp/kafka-422
  curl -fL --retry 3 --retry-delay 2 -o /tmp/kafka_2.13-4.2.2.tgz \
    https://downloads.apache.org/kafka/4.2.2/kafka_2.13-4.2.2.tgz
  tar -xzf /tmp/kafka_2.13-4.2.2.tgz -C /tmp/kafka-422
fi

DATA=/tmp/miaosha-kafka-data
LOG=/tmp/miaosha-kafka-runlogs
PIDFILE=/tmp/miaosha-kafka.pid

stop_kafka() {
  if [[ -f "$PIDFILE" ]]; then
    kill "$(cat "$PIDFILE")" >/dev/null 2>&1 || true
  fi
  # 配置文件路径写在命令行里，用来只停这一章的进程。
  if pgrep -f "kafka.Kafka ${PWD}/server.properties" >/dev/null 2>&1; then
    pkill -f "kafka.Kafka ${PWD}/server.properties" >/dev/null 2>&1 || true
  fi
  sleep 0.4
}

trap stop_kafka EXIT
stop_kafka
rm -rf "$DATA" "$LOG"
mkdir -p "$LOG"

CID="$(KAFKA_HEAP_OPTS='-Xms64m -Xmx128m' "$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
KAFKA_HEAP_OPTS='-Xms64m -Xmx128m' "$KAFKA_HOME/bin/kafka-storage.sh" format \
  -c "$PWD/server.properties" -t "$CID" --standalone

# 启动脚本不设 KAFKA_HEAP_OPTS 时会要 1G。这里钉死 256MB。
# 不设 JMX 端口。KAFKA_JMX_OPTS 覆盖脚本里默认打开的无认证 JMX。
export KAFKA_HEAP_OPTS='-Xms256m -Xmx256m'
export KAFKA_JMX_OPTS='-Dcom.sun.management.jmxremote=false'
export LOG_DIR="$LOG"
"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$PWD/server.properties"

ready=0
for _ in $(seq 1 40); do
  if KAFKA_HEAP_OPTS='-Xms64m -Xmx128m' "$KAFKA_HOME/bin/kafka-topics.sh" \
      --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if [[ "$ready" != 1 ]]; then
  echo "kafka did not start" >&2
  tail -n 40 "$LOG/kafkaServer.out" >&2 || true
  exit 1
fi
pid="$(pgrep -f "kafka.Kafka ${PWD}/server.properties" | head -n 1)"
if [[ -z "${pid}" ]]; then
  echo "kafka pid not found" >&2
  exit 1
fi
echo "$pid" > "$PIDFILE"

export GOTOOLCHAIN=local
go build -o /tmp/miaosha-kafkabench .
BIN=/tmp/miaosha-kafkabench
KB="$KAFKA_HOME/bin"

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go=$(go version)"
  echo "java=$(java -version 2>&1 | head -n 1)"
  echo "kafka_home=$KAFKA_HOME"
  echo "cluster_id=$CID"
  echo "broker_pid=$pid"
  echo "nproc=$(nproc)"
  lscpu | awk -F: '/Model name|^CPU\(s\)|Thread\(s\) per core|BogoMIPS/ {gsub(/^ +/,"",$2); print}'
  free -h | head -n 2
  echo "--- broker cmdline ---"
  echo "java $KAFKA_HEAP_OPTS -Dcom.sun.management.jmxremote=false kafka.Kafka $PWD/server.properties"
  echo "classpath_jars=$(tr '\0' '\n' < "/proc/${pid}/cmdline" | awk '/^-cp$/{getline; n=split($0,a,":"); print n; exit}')"
  echo "--- heap ---"
  echo "$KAFKA_HEAP_OPTS"
} > results/env.txt

run() {
  "$BIN" -kafka-bin "$KB" -broker-pid "$pid" -log-dir "$DATA" "$@"
}

run -scenario dedup -topic ms-dedup -out results/dedup.json
run -scenario loss -topic ms-loss -out results/loss.json
run -scenario redelivery -topic ms-redelivery -out results/redelivery.json
run -scenario assign -topic ms-assign-1 -partitions 1 -messages 1000 -out results/assign-1.json
run -scenario assign -topic ms-assign-4 -partitions 4 -messages 1000 -out results/assign-4.json

run -scenario produce -topic ms-p1-linger5 -partitions 1 -acks 1 -linger-ms 5 -compression none \
  -workers 8 -duration 4s -warmup 1s -payload fixed200 -out results/produce-p1-linger5.json
run -scenario produce -topic ms-p1-linger0 -partitions 1 -acks 1 -linger-ms 0 -compression none \
  -workers 8 -duration 4s -warmup 1s -payload fixed200 -out results/produce-p1-linger0.json
run -scenario produce -topic ms-p6-linger5 -partitions 6 -acks 1 -linger-ms 5 -compression none \
  -workers 8 -duration 4s -warmup 1s -payload fixed200 -out results/produce-p6-linger5.json
run -scenario produce -topic ms-p1-all -partitions 1 -acks all -idempotent -linger-ms 5 -compression none \
  -workers 8 -duration 4s -warmup 1s -payload fixed200 -out results/produce-p1-acks-all.json
run -scenario produce -topic ms-p1-lz4 -partitions 1 -acks 1 -linger-ms 5 -compression lz4 \
  -workers 8 -duration 4s -warmup 1s -payload fixed200 -out results/produce-p1-lz4.json
run -scenario produce -topic ms-p1-json -partitions 1 -acks 1 -linger-ms 5 -compression none \
  -workers 8 -duration 4s -warmup 1s -payload json -out results/produce-p1-json.json
run -scenario produce -topic ms-p1-ack0 -partitions 1 -acks 0 -linger-ms 0 -compression none \
  -workers 8 -duration 2s -warmup 1s -payload fixed200 -out results/produce-p1-ack0.json

run -scenario winners -topic ms-win-linger5 -acks 1 -linger-ms 5 -compression none \
  -workers 4 -messages 5000 -payload fixed200 -out results/winners-linger5.json
run -scenario winners -topic ms-win-linger0 -acks 1 -linger-ms 0 -compression none \
  -workers 4 -messages 5000 -payload fixed200 -out results/winners-linger0.json
run -scenario disk -topic ms-disk -messages 100000 -payload fixed200 -compression none -out results/disk.json
run -scenario disk -topic ms-disk-lz4 -messages 100000 -payload fixed200 -compression lz4 -out results/disk-lz4.json

ticks() {
  awk '{print $14+$15}' "/proc/$1/stat"
}

java_produce() {
  local name="$1"
  local topic="$2"
  local parts="$3"
  local records="$4"
  local props="$5"
  KAFKA_HEAP_OPTS='-Xms64m -Xmx128m' "$KB/kafka-topics.sh" \
    --bootstrap-server 127.0.0.1:9092 --create --topic "$topic" \
    --partitions "$parts" --replication-factor 1 >/dev/null
  # 第一次把 JIT 和主题元数据走一遍，数字丢掉。
  KAFKA_HEAP_OPTS='-Xms128m -Xmx512m' "$KB/kafka-producer-perf-test.sh" \
    --topic "$topic" --num-records 80000 --record-size 200 --throughput -1 \
    --producer-props $props >/dev/null
  local out="/tmp/miaosha-java-${name}.txt"
  local t0 n0
  t0="$(ticks "$pid")"
  n0="$(date +%s%N)"
  KAFKA_HEAP_OPTS='-Xms128m -Xmx512m' "$KB/kafka-producer-perf-test.sh" \
    --topic "$topic" --num-records "$records" --record-size 200 --throughput -1 \
    --producer-props $props > "$out"
  local t1 n1
  t1="$(ticks "$pid")"
  n1="$(date +%s%N)"
  local rss
  rss="$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status")"
  python3 - "$name" "$topic" "$parts" "$records" "$props" "$out" "$t0" "$t1" "$n0" "$n1" "$rss" <<'PY'
import json, sys
name, topic, parts, records, props, path, t0, t1, n0, n1, rss = sys.argv[1:]
text = open(path, encoding="utf-8", errors="replace").read().strip().splitlines()
line = text[-1] if text else ""
# 400000 records sent, 123456.7 records/sec (12.34 MB/sec), 1.2 ms avg latency, 40 ms max latency, 1 ms 50th, 2 ms 95th, 3 ms 99th, 5 ms 99.9th.
parts_line = line.replace(",", " ").split()
def after(token):
    if token not in parts_line:
        return None
    i = parts_line.index(token)
    return float(parts_line[i - 1])
qps = after("records/sec")
mb = after("MB/sec")
avg = after("ms")
# avg is the first "ms"; take by position instead
import re
m = re.search(
    r"([\d.]+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, ([\d.]+) ms 50th, ([\d.]+) ms 95th, ([\d.]+) ms 99th, ([\d.]+) ms 99.9th",
    line,
)
wall = (int(n1) - int(n0)) / 1e9
cpu = (float(t1) - float(t0)) / 100 / wall * 100 if wall > 0 else 0
doc = {
    "scenario": "java-produce",
    "name": name,
    "note": "官方 ProducerPerformance。百分位是工具打印的整数毫秒，不是直方图桶。堆是客户端 512MB，broker 仍是 256MB。",
    "pass": False,
    "detail": line,
    "topic": topic,
    "partitions": int(parts),
    "records": int(records),
    "props": props,
    "elapsedSec": wall,
    "brokerCpuPercent": cpu,
    "brokerRssKB": int(rss),
}
if m:
    doc.update({
        "sent": float(m.group(1)),
        "qps": float(m.group(2)),
        "mbPerSec": float(m.group(3)),
        "avgMs": float(m.group(4)),
        "maxMs": float(m.group(5)),
        "p50ms": float(m.group(6)),
        "p95ms": float(m.group(7)),
        "p99ms": float(m.group(8)),
        "p999ms": float(m.group(9)),
    })
    doc["pass"] = doc["sent"] == float(records) and doc["qps"] > 0
open(f"results/java-produce-{name}.json", "w").write(json.dumps(doc, indent=2) + "\n")
if not doc["pass"]:
    sys.exit(1)
PY
}

PROPS_ACK1="bootstrap.servers=127.0.0.1:9092 acks=1 linger.ms=5 batch.size=65536 compression.type=none enable.idempotence=false"
PROPS_ALL="bootstrap.servers=127.0.0.1:9092 acks=all linger.ms=5 batch.size=65536 compression.type=none enable.idempotence=true"
java_produce p1-linger5 j-p1 1 400000 "$PROPS_ACK1"
java_produce p6-linger5 j-p6 6 400000 "$PROPS_ACK1"
java_produce p1-acks-all j-all 1 400000 "$PROPS_ALL"

java_consume() {
  local name="$1"
  local topic="$2"
  local messages="$3"
  local out="/tmp/miaosha-java-consume-${name}.txt"
  local t0 n0
  t0="$(ticks "$pid")"
  n0="$(date +%s%N)"
  KAFKA_HEAP_OPTS='-Xms128m -Xmx512m' "$KB/kafka-consumer-perf-test.sh" \
    --bootstrap-server 127.0.0.1:9092 --topic "$topic" --messages "$messages" \
    --timeout 60000 > "$out"
  local t1 n1
  t1="$(ticks "$pid")"
  n1="$(date +%s%N)"
  local rss
  rss="$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status")"
  python3 - "$name" "$topic" "$messages" "$out" "$t0" "$t1" "$n0" "$n1" "$rss" <<'PY'
import json, sys
name, topic, messages, path, t0, t1, n0, n1, rss = sys.argv[1:]
lines = [ln for ln in open(path, encoding="utf-8", errors="replace").read().splitlines() if ln.strip()]
header, data = lines[0], lines[-1]
# start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
cols = [c.strip() for c in data.split(",")]
wall = (int(n1) - int(n0)) / 1e9
cpu = (float(t1) - float(t0)) / 100 / wall * 100 if wall > 0 else 0
doc = {
    "scenario": "java-consume",
    "name": name,
    "note": "官方 ConsumerPerformance。fetch.nMsg.sec 不含再平衡。读的是上面那次生产留下的日志。",
    "pass": False,
    "detail": data,
    "header": header,
    "topic": topic,
    "messages": int(messages),
    "elapsedSec": wall,
    "brokerCpuPercent": cpu,
    "brokerRssKB": int(rss),
}
if len(cols) >= 10:
    doc.update({
        "mb": float(cols[2]),
        "mbPerSec": float(cols[3]),
        "records": float(cols[4]),
        "qps": float(cols[5]),
        "rebalanceMs": float(cols[6]),
        "fetchMs": float(cols[7]),
        "fetchMbPerSec": float(cols[8]),
        "fetchQps": float(cols[9]),
    })
    doc["pass"] = doc["records"] == float(messages) and doc["fetchQps"] > 0
open(f"results/java-consume-{name}.json", "w").write(json.dumps(doc, indent=2) + "\n")
if not doc["pass"]:
    sys.exit(1)
PY
}

java_consume p1 j-p1 480000
java_consume p6 j-p6 480000

python3 - <<'PY'
import json, re
text = open("/tmp/miaosha-kafka-runlogs/kafkaServer-gc.log", encoding="utf-8", errors="replace").read()
rows = re.findall(r"Pause (Young|Full|Mixed|Remark|Cleanup).*? ([0-9.]+)ms", text)
def kind_ms(name):
    return [float(ms) for kind, ms in rows if kind == name]
young, full = kind_ms("Young"), kind_ms("Full")
pauses = [float(ms) for _, ms in rows]
def pct(xs, p):
    if not xs:
        return 0
    xs = sorted(xs)
    i = min(len(xs) - 1, int((len(xs) - 1) * p))
    return xs[i]
doc = {
    "scenario": "gc",
    "note": "整个 broker 进程这一次运行的停顿，不是某一组 4 秒窗口单独的。Young 的分位只在 Young 里面排。",
    "pauses": len(pauses),
    "young": len(young),
    "remark": sum(1 for k, _ in rows if k == "Remark"),
    "cleanup": sum(1 for k, _ in rows if k == "Cleanup"),
    "full": len(full),
    "maxPauseMs": max(pauses) if pauses else 0,
    "maxYoungMs": max(young) if young else 0,
    "youngP50Ms": pct(young, 0.50),
    "youngP99Ms": pct(young, 0.99),
    "maxFullMs": max(full) if full else 0,
    "fullOver10ms": sum(1 for x in full if x >= 10),
    "pass": True,
}
open("results/gc.json", "w").write(json.dumps(doc, indent=2) + "\n")
print(f"gc pauses={doc['pauses']} maxMs={doc['maxPauseMs']}")
PY

echo "broker_rss_end_kb=$(awk '/VmRSS:/ {print $2}' /proc/${pid}/status)" >> results/env.txt
echo "data_du_logical=$(du -sb "$DATA" | awk '{print $1}')" >> results/env.txt
echo "data_du_alloc=$(du -s --block-size=1 "$DATA" | awk '{print $1}')" >> results/env.txt
echo "done"
