#!/usr/bin/env bash
# 用 3 个 JVM 进程模拟 K8s 里的 3 个 Pod，验证"本地批发库存(v5) + 水平扩容"。
# 压测客户端按轮询直连 3 个 Pod（等价于 L4 Service）；正确性阶段额外挂一个 L7 轮询代理验证入口转发。
set -uo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/Cellar/openjdk/27.0.1.0.3/libexec/openjdk.jdk/Contents/Home}"
JAVA="$JAVA_HOME/bin/java"
CP="target/classes:$(cat target/cp.txt)"
OUT="results/cluster"
mkdir -p "$OUT"
PORTS=(18081 18082 18083)
LB_PORT=18090
LB="http://127.0.0.1:$LB_PORT"
URLS="http://127.0.0.1:18081/seckill?goodsId=1,http://127.0.0.1:18082/seckill?goodsId=1,http://127.0.0.1:18083/seckill?goodsId=1"
PIDS=()

cleanup() {
  if [ ${#PIDS[@]} -gt 0 ]; then
    for p in "${PIDS[@]}"; do
      kill "$p" 2>/dev/null
    done
    sleep 1
    for p in "${PIDS[@]}"; do
      kill -9 "$p" 2>/dev/null
    done
  fi
}
trap cleanup EXIT

start_cluster() {
  local chunk="$1" with_lb="${2:-yes}"
  for port in "${PORTS[@]}"; do
    "$JAVA" -Xms192m -Xmx384m -cp "$CP" com.geekq.lab.SeckillServer \
      --mode v5 --port "$port" --chunk "$chunk" > "$OUT/pod-$port.log" 2>&1 &
    PIDS+=($!)
  done
  if [ "$with_lb" = "yes" ]; then
    "$JAVA" -Xms128m -Xmx256m -cp "$CP" com.geekq.lab.LbProxy \
      --port "$LB_PORT" --backends "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083" \
      > "$OUT/lb.log" 2>&1 &
    PIDS+=($!)
  fi

  for _ in $(seq 1 100); do
    curl -sf -m 1 "http://127.0.0.1:18081/stats" >/dev/null 2>&1 && return 0
    sleep 0.3
  done
  echo "[cluster] 启动失败"
  tail -20 "$OUT"/pod-*.log
  return 1
}

stop_cluster() {
  for p in "${PIDS[@]}"; do
    kill "$p" 2>/dev/null
  done
  sleep 2
  for p in "${PIDS[@]}"; do
    kill -9 "$p" 2>/dev/null
  done
  PIDS=()
  sleep 1
}

echo "[cluster] 3 pods，chunk=2000，库存 3,000,000"
start_cluster 2000 no
curl -sf "http://127.0.0.1:18081/admin/reset?stock=3000000" >/dev/null

( while kill -0 "${PIDS[0]}" 2>/dev/null; do ps -o %cpu=,rss= -p "${PIDS[0]}"; sleep 1; done ) > "$OUT/ps-pod1.txt" &
SAMP=$!
"$JAVA" -Xms256m -Xmx512m -cp "$CP" com.geekq.lab.LoadGen \
  --urls "$URLS" --seconds 15 --warmup 3 --concurrency "${CONC:-192}" \
  --label cluster-3pods --out "$OUT/load-cluster.json" | tee "$OUT/raw.txt"
kill "$SAMP" 2>/dev/null
sleep 4

for port in "${PORTS[@]}"; do
  curl -s "http://127.0.0.1:$port/stats" > "$OUT/pod-$port.json"
done

python3 - "$OUT" <<'PY' | tee "$OUT/summary.json"
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])
tot = {"accepted": 0, "published": 0, "persisted": 0, "localLeft": 0, "redisLeft": 0}
for p in sorted(out.glob("pod-1808*.json")):
    d = json.loads(p.read_text())
    tot["accepted"] += d["metrics"]["accepted"]
    tot["published"] += d["mq"]["published"]
    tot["persisted"] += d["mq"]["persisted"]
    tot["localLeft"] += d.get("localStock", 0) or 0
    tot["redisLeft"] = max(tot["redisLeft"], d.get("redisStock", 0) or 0)
print(json.dumps(tot))
PY
stop_cluster

echo "[cluster] 正确性：3 pods + L7 代理，chunk=10，库存 100，3000 请求"
start_cluster 10 yes
"$JAVA" -Xms256m -Xmx512m -cp "$CP" com.geekq.lab.CorrectnessTest "$LB" 100 3000 128 lenient \
  | tee "$OUT/correctness.txt"
stop_cluster

echo "[cluster] done -> $OUT"
