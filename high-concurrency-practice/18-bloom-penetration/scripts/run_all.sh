#!/usr/bin/env bash
# 第 18 章全量实验：5 种防穿透写法 × 2 种流量形状 × 3 轮。
# 会在本机 6381 起一个 Redis（256MB），在 miaosha_bloom 库建 goods 表，会 TRUNCATE 它。
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -z "${JAVA_HOME:-}" ]]; then
  for c in /opt/homebrew/opt/openjdk /opt/homebrew/opt/openjdk@21 /Library/Java/JavaVirtualMachines/*/Contents/Home; do
    if [[ -x "$c/bin/java" ]]; then JAVA_HOME="$c"; break; fi
  done
fi
export PATH="$JAVA_HOME/bin:$PATH"
CP="target/classes:$(cat target/cp.txt)"

SECONDS_RUN="${SECONDS_RUN:-10}"
CONCURRENCY="${CONCURRENCY:-64}"
GOODS="${GOODS:-50000}"
ROUNDS="${ROUNDS:-3}"
PORT="${PORT:-18090}"
mkdir -p results

if ! redis-cli -p 6381 ping >/dev/null 2>&1; then
  redis-server redis-256mb.conf > results/redis-6381.log 2>&1 &
  for _ in $(seq 1 20); do redis-cli -p 6381 ping >/dev/null 2>&1 && break; sleep 0.2; done
fi

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host=$(uname -srm)"
  echo "cpu=$(sysctl -n hw.ncpu)"
  echo "mem_bytes=$(sysctl -n hw.memsize)"
  echo "loadavg=$(sysctl -n vm.loadavg)"
  echo "java=$(java -version 2>&1 | head -1)"
  echo "redis=$(redis-cli -p 6381 info server | tr -d '\r' | grep redis_version)"
  echo "mysql=$(mysql -uroot -proot -N -e 'select version()' 2>/dev/null)"
  echo "goods=$GOODS concurrency=$CONCURRENCY seconds=$SECONDS_RUN rounds=$ROUNDS"
  echo "redis_maxmemory=$(redis-cli -p 6381 config get maxmemory | tail -1)"
} > results/env.txt

stats() {
  curl -sf "http://127.0.0.1:$PORT/stats"
}

run_mode() {
  local mode="$1"
  echo "=== mode=$mode ==="
  redis-cli -p 6381 flushdb >/dev/null
  java -Xmx512m -cp "$CP" com.geekq.lab.bloom.PenetrationServer \
      --mode "$mode" --port "$PORT" --goods "$GOODS" > "results/server-$mode.log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$PORT/stats" >/dev/null 2>&1 && break
    sleep 0.3
  done
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "server for $mode died:" >&2
    tail -20 "results/server-$mode.log" >&2
    return 1
  fi

  for round in $(seq 1 "$ROUNDS"); do
    for wl in scan replay; do
      stats > "results/stats-$mode-$wl-r$round-before.json"
      local extra=""
      if [[ "$wl" == "scan" ]]; then
        extra="--seqOffset $((round * 1000000))"
      fi
      java -Xmx1g -cp "$CP" com.geekq.lab.bloom.LoadGen \
          --url "http://127.0.0.1:$PORT" --workload "$wl" --label "$mode" \
          --seconds "$SECONDS_RUN" --warmup 3 --concurrency "$CONCURRENCY" --goods "$GOODS" \
          $extra --out "results/e2e-$mode-$wl-r$round.json" > /dev/null
      stats > "results/stats-$mode-$wl-r$round-after.json"
      python3 - "$mode" "$wl" "$round" <<'PY'
import json,sys
mode,wl,rnd=sys.argv[1],sys.argv[2],sys.argv[3]
e=json.load(open(f"results/e2e-{mode}-{wl}-r{rnd}.json"))
b=json.load(open(f"results/stats-{mode}-{wl}-r{rnd}-before.json"))
a=json.load(open(f"results/stats-{mode}-{wl}-r{rnd}-after.json"))
d={"mode":mode,"workload":wl,"round":int(rnd),"requests":a["requests"]-b["requests"]}
for k in ["dbQueries","redisCmds","bloomRejects","nullHits","ok200","notFound","errors"]:
    d[k]=a[k]-b[k]
d["bloomBytes"]=a["bloomBytes"]; d["fpTheory"]=a["fpTheory"]
json.dump(d,open(f"results/stats-{mode}-{wl}-r{rnd}.json","w"),indent=1)
r=d["requests"]
print(f"  r{rnd} {wl:6s} rps={e['rps']:>7} p50={e['latency']['p50Us']:>6}us p99={e['latency']['p99Us']:>7}us "
      f"db={d['dbQueries']:>7} ({100*d['dbQueries']/max(1,r):5.2f}%/req) "
      f"redis={d['redisCmds']:>7} rej={d['bloomRejects']:>7} nullHit={d['nullHits']:>6} "
      f"correct={e['correct']} err={d['errors']}")
PY
    done
  done
  {
    echo "mode=$mode"
    echo "  used_memory=$(redis-cli -p 6381 info memory | tr -d '\r' | awk -F: '/used_memory_human/{print $2}')"
    echo "  keys=$(redis-cli -p 6381 dbsize)"
    echo "  $(redis-cli -p 6381 info stats | tr -d '\r' | grep -E '^evicted_keys')"
  } >> results/e2e-summary.txt
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  sleep 0.5
}

for m in none nullcache bloom bloomredis bloomnull; do
  run_mode "$m"
done

echo "done"
