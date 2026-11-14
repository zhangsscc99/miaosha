#!/usr/bin/env python3
"""汇总多次实验结果：读 results/**/raw-*.jsonl，输出中位数/区间与每次明细。"""
import glob
import json
import pathlib
import re
import statistics

ROOT = pathlib.Path(__file__).resolve().parent.parent
RES = ROOT / "results"

RUN_NAMES = {
    "20261114-124351": "第 1 轮",
    "20261114-124802": "第 2 轮",
    "20261114-125142": "第 3 轮",
}

LABELS = [
    ("baseline-db", "v0 DB 直接扣减 + 同步落库"),
    ("redis-sync-db", "v1 Redis Lua + 同步落库"),
    ("redis-mq", "v2 Redis Lua + MQ 异步"),
    ("localcache-mq", "v3 本地库存 + MQ 异步"),
    ("ratelimit-v3", "v4 v3 + 入口限流 5000/s"),
    ("soldout-db", "售罄后 v0（DB 空转）"),
    ("soldout-localcache", "售罄后 v3（本地标记）"),
]


def parse_raws():
    runs = {}
    for path in sorted(glob.glob(str(RES / "**" / "raw-*.jsonl"), recursive=True)):
        m = re.search(r"raw-(\d{8}-\d{6})", path)
        if not m:
            continue
        run = RUN_NAMES.get(m.group(1), m.group(1))
        runs.setdefault(run, {"loads": {}, "mysql": {}, "correctness": [], "lock": []})
        for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("[loadgen] "):
                line = line[len("[loadgen] "):]
            elif line.startswith("[correctness] "):
                line = line[len("[correctness] "):]
            elif line.startswith("[lockbench] "):
                try:
                    runs[run]["lock"].append(json.loads(line.split(" ", 1)[1]))
                except json.JSONDecodeError:
                    pass
                continue
            if not line.startswith("{"):
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "qps" in d and "label" in d:
                runs[run]["loads"][d["label"]] = d
            elif "mysqlWrites" in d:
                runs[run]["mysql"][d["label"]] = d
            elif "pass" in d:
                runs[run]["correctness"].append(d)
    return runs


def ps_stats(label, run_dir):
    path = RES / run_dir / f"ps-{label}.txt"
    if not path.exists():
        path = RES / f"ps-{label}.txt"
    if not path.exists():
        return None, None
    cpu, rss = [], []
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                cpu.append(float(parts[0]))
                rss.append(float(parts[1]))
            except ValueError:
                pass
    if not cpu:
        return None, None
    return statistics.mean(cpu), max(rss) / 1024.0


def main():
    runs = parse_raws()
    out = ["# 压测结果汇总", ""]
    out.append(f"共 {len(runs)} 轮，同一台 8 vCPU / 16 GB macOS 主机；服务端、压测客户端、"
               "Redis、MySQL、RabbitMQ 全部跑在本机，且机器上还有其它后台进程。")
    out.append("因此绝对值偏保守、轮次间波动明显，**对照比例比绝对值更有参考意义**。")
    out.append("")

    out += ["## 各场景 QPS（三轮）", "",
            "| 场景 | 第1轮 | 第2轮 | 第3轮 | 中位数 | 相对 v0 |",
            "|---|---:|---:|---:|---:|---:|"]
    medians = {}
    for label, desc in LABELS:
        vals = []
        for run in ["第 1 轮", "第 2 轮", "第 3 轮"]:
            d = runs.get(run, {}).get("loads", {}).get(label)
            vals.append(d["qps"] if d else None)
        present = [v for v in vals if v]
        med = statistics.median(present) if present else 0
        medians[label] = med
        base = medians.get("baseline-db") or 1
        ratio = f"{med / base:.1f}×" if label != "baseline-db" else "-"
        cells = " | ".join(f"{v:,}" if v else "-" for v in vals)
        out.append(f"| {desc} | {cells} | {med:,.0f} | {ratio} |")

    out += ["", "> 售罄场景的 QPS 为 0（没有成功订单），应看“总 RPS”（约 5.8k–21.6k），"
            "以及数据库写入是否归零。", ""]

    out += ["## 每轮延迟明细", "",
            "| 轮次 | 场景 | 成功 QPS | 总 RPS | p50(ms) | p99(ms) | p999(ms) | 错误 |",
            "|---|---|---:|---:|---:|---:|---:|---:|"]
    for run in ["第 1 轮", "第 2 轮", "第 3 轮"]:
        for label, desc in LABELS:
            d = runs.get(run, {}).get("loads", {}).get(label)
            if not d:
                continue
            out.append(f"| {run} | {desc} | {d['qps']:,} | {d['rps']:,} | {d['p50Ms']:.2f} | "
                       f"{d['p99Ms']:.2f} | {d['p999Ms']:.2f} | {d['errors']:,} |")

    out += ["", "## 服务端资源（第 2/3 轮采样）", "",
            "| 场景 | 平均 CPU% | 峰值 RSS(MB) |", "|---|---:|---:|"]
    for label, desc in LABELS[:5]:
        cpu, rss = ps_stats(label, "final")
        if cpu is None:
            cpu, rss = ps_stats(label, "")
        if cpu is not None:
            out.append(f"| {desc} | {cpu:.0f} | {rss:.0f} |")

    out += ["", "## MySQL 写入语句数（第 3 轮）", "",
            "| 场景 | 写语句数 | 写语句/s |", "|---|---:|---:|"]
    for label, desc in LABELS:
        d = runs.get("第 3 轮", {}).get("mysql", {}).get(label)
        if d:
            out.append(f"| {desc} | {d['mysqlWrites']:,} | {d['mysqlWriteQps']:,} |")

    out += ["", "## 正确性验证", ""]
    seen = set()
    for run, data in runs.items():
        for c in data["correctness"]:
            key = (run, c.get("accepted"), c.get("dbOrders"))
            if key in seen:
                continue
            seen.add(key)
            out.append(f"- {run}: 库存 {c['stock']}，请求 {c['requests']}，成功 {c['accepted']}，"
                       f"DB 订单 {c['dbOrders']}，对账后 DB 库存 {c['dbStock']}，"
                       f"Redis/本地余量 {c.get('redisStock')}/{c.get('localStock')}，"
                       f"**{'通过' if c['pass'] else '失败'}**")

    lock_path = RES / "lockbench.txt"
    if lock_path.exists():
        out += ["", "## 锁 / Lua 微基准（8 线程 × 3000 次）", "", "```"]
        out += lock_path.read_text().strip().splitlines()[:3]
        out.append("```")

    rb = RES / "redis-benchmark.txt"
    if rb.exists():
        out += ["", "## Redis 单机基线（64 客户端 / 20 万请求）", "", "```"]
        txt = rb.read_text()
        lines = [ln.strip() for ln in txt.splitlines() if "requests per second" in ln]
        out += [re.sub(r"\s+", " ", ln) for ln in lines]
        out.append("```")

    cluster = RES / "cluster"
    if (cluster / "load-cluster.json").exists():
        d = json.loads((cluster / "load-cluster.json").read_text())
        s = json.loads((cluster / "summary.json").read_text()) if (cluster / "summary.json").exists() else {}
        out += ["", "## 3 Pod 集群实验（本机 3 进程 + 轮询直连）", "",
                f"- 成功 QPS：{d['qps']:,}，p50 {d['p50Ms']:.1f} ms，p99 {d['p99Ms']:.1f} ms，"
                f"错误 {d['errors']}",
                f"- 受理 {s.get('accepted', 0):,}，MQ 落库 {s.get('persisted', 0):,}，"
                f"本地剩余 {s.get('localLeft', 0):,}，Redis 剩余 {s.get('redisLeft', 0):,}",
                "- 说明：同机还有浏览器/IDE 争抢 CPU，聚合吞吐不能当作线性扩容证据；"
                "该实验用于验证多 Pod 批发库存的正确性（100 库存 / 3000 请求恰好 100 单）。"]
        cpath = cluster / "correctness.txt"
        if cpath.exists():
            out += ["", "```", cpath.read_text().strip(), "```"]

    target = RES / "summary.md"
    target.write_text("\n".join(out) + "\n", encoding="utf-8")
    print("\n".join(out))


if __name__ == "__main__":
    main()
