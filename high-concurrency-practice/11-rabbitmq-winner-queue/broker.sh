#!/usr/bin/env bash
# 以 rabbitmq 用户启动这一章自己的节点。deb 包装过的 /usr/sbin/rabbitmqctl 只允许 root 或 rabbitmq。
set -euo pipefail

BASE="${MIAOSHA_RMQ_BASE:-/tmp/miaosha-rabbitmq}"
NODENAME="${MIAOSHA_RMQ_NODENAME:-miaosha@localhost}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CTL=(sudo -n -u rabbitmq env HOME=/var/lib/rabbitmq RABBITMQ_NODENAME="$NODENAME" RABBITMQ_MNESIA_BASE="$BASE/mnesia" RABBITMQ_LOG_BASE="$BASE/log" RABBITMQ_PID_FILE="$BASE/rabbit.pid" RABBITMQ_CONFIG_FILE="$BASE/rabbitmq" RABBITMQ_ENABLED_PLUGINS_FILE="$BASE/enabled_plugins")

cmd="${1:-}"
shift || true

case "$cmd" in
  start)
    sudo mkdir -p "$BASE/mnesia" "$BASE/log"
    # 目录属于 rabbitmq，普通用户写不进去。
    tmpconf="$(mktemp)"
    cp "$HERE/rabbitmq.conf" "$tmpconf"
    sudo cp "$tmpconf" "$BASE/rabbitmq.conf"
    rm -f "$tmpconf"
    printf '[].\n' | sudo tee "$BASE/enabled_plugins" >/dev/null
    sudo chown -R rabbitmq:rabbitmq "$BASE"
    if [[ -f "$BASE/rabbit.pid" ]] && kill -0 "$(cat "$BASE/rabbit.pid")" 2>/dev/null; then
      echo "already running pid=$(cat "$BASE/rabbit.pid")"
      exit 0
    fi
    sudo rm -f "$BASE/rabbit.pid" || true
    "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmq-server -detached
    # ping 只说明 Erlang 进程已经注册。应用起来之前 status 会失败。
    ok=0
    for _ in $(seq 1 80); do
      if "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmqctl --timeout 5 ping >/dev/null 2>&1; then
        ok=1
        break
      fi
      sleep 0.25
    done
    if [[ "$ok" != 1 ]]; then
      echo "rabbitmq did not start" >&2
      tail -n 50 "$BASE/log/"* >&2 || true
      exit 1
    fi
    if ! "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmqctl --timeout 60 await_startup; then
      echo "rabbitmq app did not start" >&2
      tail -n 50 "$BASE/log/"* >&2 || true
      exit 1
    fi
    if [[ -f "$BASE/watermark.bytes" ]]; then
      "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmqctl set_vm_memory_high_watermark absolute "$(cat "$BASE/watermark.bytes")"
    fi
    ;;
  stop)
    "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmqctl --timeout 60 shutdown || true
    sleep 0.3
    ;;
  kill)
    pid=""
    if [[ -f "$BASE/rabbit.pid" ]]; then
      pid="$(cat "$BASE/rabbit.pid" || true)"
    fi
    if [[ -z "${pid}" ]]; then
      pid="$(pgrep -u rabbitmq -f beam.smp | head -n 1 || true)"
    fi
    if [[ -n "${pid}" ]]; then
      sudo kill -9 "$pid" || true
      for _ in $(seq 1 50); do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
    fi
    sudo rm -f "$BASE/rabbit.pid" || true
    # 端口松开再交给 start。这台机器没有 ss。
    for _ in $(seq 1 50); do
      if ! python3 -c 'import socket,sys; s=socket.socket(); s.settimeout(0.2); r=s.connect_ex(("127.0.0.1",5672)); s.close(); sys.exit(0 if r==0 else 1)'; then
        break
      fi
      sleep 0.1
    done
    ;;
  ctl)
    "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmqctl --timeout 30 "$@"
    ;;
  diag)
    "${CTL[@]}" /usr/lib/rabbitmq/bin/rabbitmq-diagnostics --timeout 30 "$@"
    ;;
  pid)
    if [[ -f "$BASE/rabbit.pid" ]]; then
      cat "$BASE/rabbit.pid"
      exit 0
    fi
    pgrep -u rabbitmq -f beam.smp | head -n 1
    ;;
  du)
    app="$(sudo -n du -sb "$BASE/mnesia" | awk '{print $1}')"
    alloc="$(sudo -n du -sB1 "$BASE/mnesia" | awk '{print $1}')"
    echo "apparent=$app"
    echo "allocated=$alloc"
    ;;
  *)
    echo "usage: broker.sh start|stop|kill|ctl|diag|pid|du" >&2
    exit 2
    ;;
esac
