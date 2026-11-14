#!/usr/bin/env python3
"""第 19 章压测驱动：静态化、gzip、keepalive、open_file_cache、sendfile、边缘缓存、限流。

服务由 run.sh 拉起：nginx 监听 18078/18080-18089，EdgeServer 18090，Redis 6390。
所有数字写 results/，一组一个 JSON。用法：

    python3 scripts/bench.py pilot     # 每项 3 秒，先看量级
    python3 scripts/bench.py matrix    # 3 轮主对照
    python3 scripts/bench.py cache     # 缓存击穿：lock on/off × 3 轮
    python3 scripts/bench.py limits    # 限流三种 burst × 2 轮
"""
import json
import os
import random
import re
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(BASE, "results")
GOODS = 1000
JAVA = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk") + "/bin/java"
CP = os.path.join(BASE, "target", "cp.txt")


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def cpu_seconds(pid):
    out = sh("ps -o time= -p %d" % pid)
    if not out:
        return None
    try:
        parts = [float(x) for x in out.strip().split(":")]
    except ValueError:
        return None
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return parts[0]


def nginx_pids():
    try:
        master = int(open(os.path.join(BASE, "runtime", "logs", "nginx.pid")).read().strip())
    except Exception:
        return []
    kids = [int(x) for x in sh("pgrep -P %d" % master).split()]
    return [master] + kids


def edge_pid():
    out = sh("pgrep -f 'com.geekq.lab.edge.EdgeServer'")
    return int(out.split()[0]) if out else None


def get_json(url, host="127.0.0.1", port=18090, path="/stats", timeout=3):
    import http.client
    c = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        c.request("GET", path)
        r = c.getresponse()
        body = r.read()
        return json.loads(body) if body else {}
    finally:
        c.close()


def set_delay(ms):
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", 18090, timeout=3)
    try:
        c.request("GET", "/delay?ms=%d" % ms)
        c.getresponse().read()
    finally:
        c.close()


def run_load(label, url, prefix, suffix, conc=64, secs=5, warmup=2, gzip=False,
             header=None, rnd=1, goods=GOODS, fixed_id=None):
    out = os.path.join(RESULTS, "runs", "%s-r%d.json" % (label, rnd))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    cp = open(CP).read().strip()
    cmd = [JAVA, "-Xmx2g", "-cp", "target/classes:" + cp,
           "com.geekq.lab.edge.LoadGen",
           "--url", url, "--prefix", prefix, "--suffix", suffix,
           "--seconds", str(secs), "--warmup", str(warmup),
           "--concurrency", str(conc), "--goods", str(goods),
           "--label", label, "--out", out]
    if gzip:
        cmd += ["--gzip", "true"]
    if header:
        cmd += ["--header", header]
    if fixed_id is not None:
        cmd += ["--fixedId", str(fixed_id)]
    ng_pids = nginx_pids()
    edge = edge_pid()
    server_pids = sorted(set(ng_pids + ([edge] if edge else [])))
    for suffix_f in (".start", ".end"):
        try:
            os.remove(out + suffix_f)
        except FileNotFoundError:
            pass
    p = subprocess.Popen(cmd, cwd=BASE, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, text=True)
    deadline = time.time() + 30
    while not os.path.exists(out + ".start") and time.time() < deadline:
        time.sleep(0.05)
    before = get_json("")
    c0 = {pid: cpu_seconds(pid) or 0.0 for pid in server_pids}
    cli0 = cpu_seconds(p.pid) or 0.0
    t0 = time.time()
    while not os.path.exists(out + ".end") and time.time() - t0 < secs + 15:
        time.sleep(0.05)
    wall = time.time() - t0
    c1 = {pid: cpu_seconds(pid) or 0.0 for pid in server_pids}
    cli1 = cpu_seconds(p.pid) or 0.0
    stdout, _ = p.communicate(timeout=60)
    after = get_json("")

    line = ""
    for ln in stdout.strip().splitlines()[::-1]:
        if ln.startswith("{"):
            line = ln
            break
    data = json.loads(line) if line else {}

    def delta(k):
        return max(0, int(after.get(k, 0)) - int(before.get(k, 0)))

    ng = sum(c1[x] - c0[x] for x in ng_pids if x in c0)
    edge_delta = (c1.get(edge, 0.0) - c0.get(edge, 0.0)) if edge else 0.0
    rec = {
        "label": label,
        "round": rnd,
        "url": url + prefix + "{id}" + suffix,
        "concurrency": conc,
        "seconds": round(wall, 2),
        "rps": data.get("rps"),
        "requests": data.get("requests"),
        "ok200": data.get("ok200"),
        "unavailable": data.get("unavailable"),
        "other": data.get("other"),
        "latency": data.get("latency"),
        "bodyBytes": data.get("bodyBytes"),
        "contentLengthBytes": data.get("contentLengthBytes"),
        "headerCounts": data.get("headerCounts"),
        "edgePageDelta": delta("page"),
        "edgeApiDelta": delta("api"),
        "edgeRedisDelta": delta("redis"),
        "loadavg": round(os.getloadavg()[0], 2),
        "cpu": {
            "nginxPct": round(ng / wall * 100, 1) if wall else None,
            "edgePct": round(edge_delta / wall * 100, 1) if wall else None,
            "clientPct": round((cli1 - cli0) / wall * 100, 1) if wall else None,
        },
    }
    return rec


def warm_cache(n=GOODS, port=18084):
    import http.client

    def one(i):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            c.request("GET", "/dyn?id=%d" % i)
            c.getresponse().read()
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(one, range(1, n + 1)))


def warm_path(port, path):
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=8)
    try:
        c.request("GET", path)
        c.getresponse().read()
    finally:
        c.close()


def burst(port, n=256, gid=None, delay_ms=20, rnd=1):
    import http.client
    set_delay(delay_ms)
    gid = gid or random.randint(1, GOODS)
    tag = "%d%d" % (int(time.time() * 1000), rnd)
    path = "/dyn?id=%d&b=%s" % (gid, tag)
    before = get_json("")
    lat = []
    codes = Counter()
    cache = Counter()
    barrier = threading.Barrier(n)

    def one(i):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
        try:
            barrier.wait()
            t = time.perf_counter()
            c.request("GET", path)
            r = c.getresponse()
            r.read()
            lat.append((time.perf_counter() - t) * 1000)
            codes[r.status] += 1
            cache[r.getheader("X-Cache") or "none"] += 1
        except Exception:
            codes["err"] += 1
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(one, range(n)))
    after = get_json("")
    lat.sort()
    rec = {
        "label": "cache-lock-%s" % ("on" if port == 18084 else "off"),
        "round": rnd,
        "port": port,
        "concurrency": n,
        "delayMs": delay_ms,
        "elapsedMs": round(max(lat), 1) if lat else None,
        "p50Ms": round(statistics.median(lat), 1) if lat else None,
        "p99Ms": round(lat[int(len(lat) * 0.99) - 1], 1) if lat else None,
        "codes": dict(codes),
        "cacheStatus": dict(cache),
        "originPageRequests": max(0, after.get("page", 0) - before.get("page", 0)),
        "originRedis": max(0, after.get("redis", 0) - before.get("redis", 0)),
    }
    return rec


def burst_java(port, n=256, gid=None, delay_ms=20, rnd=1, path=None):
    """一个 Java 进程用 CyclicBarrier 对齐 N 个虚拟线程，每个线程只发一次；延迟由 Java 直方图记录。"""
    set_delay(delay_ms)
    tag = "%d%d" % (int(time.time() * 1000), rnd)
    if path is None:
        gid = gid or random.randint(1, GOODS)
        path = "/dyn?id=%d&b=%s" % (gid, tag)
    cp = open(CP).read().strip()
    cmd = [JAVA, "-Xmx2g", "-cp", "target/classes:" + cp, "com.geekq.lab.edge.Burst",
           "--port", str(port), "--n", str(n),
           "--path", path]
    p = subprocess.run(cmd, cwd=BASE, capture_output=True, text=True, timeout=90)
    line = ""
    for ln in p.stdout.strip().splitlines()[::-1]:
        if ln.startswith("{"):
            line = ln
            break
    data = json.loads(line) if line else {}
    return {
        "label": "cache-lock-%s" % ("on" if port == 18084 else "off"),
        "round": rnd,
        "port": port,
        "concurrency": n,
        "delayMs": delay_ms,
        "wallMs": data.get("wallMs"),
        "latency": data.get("latency"),
        "codes": data.get("codes"),
        "cacheStatus": data.get("cache"),
        "originPageRequests": data.get("originPage"),
        "originRedis": data.get("originRedis"),
    }


def block_rate():
    """限流测试之间让令牌桶恢复。"""
    time.sleep(2.2)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "pilot"
    os.makedirs(os.path.join(RESULTS, "runs"), exist_ok=True)
    rows = []
    if mode in ("pilot", "matrix"):
        rounds = 1 if mode == "pilot" else 3
        secs = 3 if mode == "pilot" else 3
        matrix = [
            ("static-baseline", "http://127.0.0.1:18080", "/p", ".html", {}),
            ("static-gzip", "http://127.0.0.1:18081", "/p", ".html", {"gzip": True}),
            ("static-nokeepalive", "http://127.0.0.1:18082", "/p", ".html", {}),
            ("static-openfilecache", "http://127.0.0.1:18078", "/p", ".html", {}),
            ("static-sendfile-off", "http://127.0.0.1:18089", "/p", ".html", {}),
            ("dyn-via-nginx", "http://127.0.0.1:18083", "/dyn?id=", "", {}),
            ("dyn-direct", "http://127.0.0.1:18090", "/dyn?id=", "", {}),
            ("api-via-nginx", "http://127.0.0.1:18083", "/api?id=", "", {}),
            ("edgecache-steady", "http://127.0.0.1:18084", "/dyn?id=", "", {"header": "X-Cache"}),
        ]
        set_delay(0)
        for rnd in range(1, rounds + 1):
            order = matrix[:]
            random.shuffle(order)
            for label, url, prefix, suffix, kw in order:
                if label == "edgecache-steady":
                    warm_cache()
                rec = run_load(label, url, prefix, suffix, conc=32, secs=secs,
                               warmup=2, rnd=rnd, **kw)
                rows.append(rec)
                print(json.dumps(rec, ensure_ascii=False))
    elif mode == "scale":
        set_delay(0)
        cases = [
            ("static-c32", "http://127.0.0.1:18080", "/p", ".html", 32),
            ("static-c64", "http://127.0.0.1:18080", "/p", ".html", 64),
            ("static-c128", "http://127.0.0.1:18080", "/p", ".html", 128),
            ("static-c256", "http://127.0.0.1:18080", "/p", ".html", 256),
            ("dyn-c32", "http://127.0.0.1:18083", "/dyn?id=", "", 32),
            ("dyn-c64", "http://127.0.0.1:18083", "/dyn?id=", "", 64),
            ("dyn-c128", "http://127.0.0.1:18083", "/dyn?id=", "", 128),
        ]
        for rnd in (1, 2):
            for label, url, prefix, suffix, conc in cases:
                rec = run_load(label, url, prefix, suffix, conc=conc, secs=4,
                               warmup=2, rnd=rnd)
                rows.append(rec)
                print(json.dumps(rec, ensure_ascii=False))
            time.sleep(1)
    elif mode == "cache":
        for rnd in range(1, 4):
            rec = burst_java(18084, n=256, delay_ms=20, rnd=rnd)
            rows.append(rec)
            print(json.dumps(rec, ensure_ascii=False))
            time.sleep(1)
            rec = burst_java(18085, n=256, delay_ms=20, rnd=rnd)
            rows.append(rec)
            print(json.dumps(rec, ensure_ascii=False))
            time.sleep(2)
        set_delay(0)
    elif mode == "stale":
        for rnd in range(1, 4):
            for port, label in ((18084, "expired-lock"), (18076, "expired-stale")):
                gid = random.randint(1, GOODS)
                path = "/dyn?id=%d&s=%d%d" % (gid, int(time.time()), rnd)
                warm_path(port, path)
                time.sleep(6.0)
                rec = burst_java(port, n=256, delay_ms=20, rnd=rnd, path=path)
                rec["label"] = label
                rows.append(rec)
                print(json.dumps(rec, ensure_ascii=False))
                time.sleep(1)
        set_delay(0)
    elif mode == "limits":
        block_rate()
        for rnd in range(1, 3):
            for label, port in [("limit-burst1", 18086), ("limit-burst1000-nodelay", 18087),
                                ("limit-burst1000-queue", 18088)]:
                block_rate()
                rec = run_load(label, "http://127.0.0.1:%d" % port, "/dyn?id=", "",
                               conc=64, secs=3, warmup=2, rnd=rnd)
                rows.append(rec)
                print(json.dumps(rec, ensure_ascii=False))
    out = os.path.join(RESULTS, "bench-%s.json" % mode)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"mode": mode, "rows": rows}, f, ensure_ascii=False, indent=2)
    print("written", out)


if __name__ == "__main__":
    main()
