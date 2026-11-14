#!/usr/bin/env python3
"""把 results/ 下的压测 JSON 汇总成 Markdown 表格，供文档直接引用。"""
import json
import pathlib
import statistics
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
RES = ROOT / "results"


def load_json_line(path: pathlib.Path):
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    try:
        return json.loads(text.splitlines()[-1])
    except json.JSONDecodeError:
        return None


def ps_stats(path: pathlib.Path):
    if not path.exists():
        return (None, None)
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
        return (None, None)
    return (statistics.mean(cpu), max(rss) / 1024.0)


def main():
    labels = [
        ("baseline-db", "v0 DB 直接扣减 + 同步落库"),
        ("redis-sync-db", "v1 Redis Lua + 同步落库"),
        ("redis-mq", "v2 Redis Lua + MQ 异步"),
        ("localcache-mq", "v3 本地库存 + MQ 异步"),
        ("ratelimit-v3", "v4 v3 + 入口限流 5000/s"),
        ("soldout-db", "售罄后 v0（DB 空转）"),
        ("soldout-localcache", "售罄后 v3（本地标记）"),
    ]
    rows = []
    for label, desc in labels:
        d = load_json_line(RES / f"load-{label}.json")
        s = load_json_line(RES / f"stats-{label}.json")
        if not d:
            continue
        cpu, rss = ps_stats(RES / f"ps-{label}.txt")
        persisted = None
        if s and "mq" in s:
            persisted = s["mq"].get("persisted")
        rows.append({
            "label": label,
            "desc": desc,
            "qps": d["qps"],
            "rps": d["rps"],
            "p50": d["p50Ms"],
            "p99": d["p99Ms"],
            "p999": d["p999Ms"],
            "limited": d["limited"],
            "errors": d["errors"],
            "cpu": cpu,
            "rss": rss,
            "persisted": persisted,
        })

    out = ["# 压测结果汇总（自动生成）", ""]
    out.append("| 场景 | 成功 QPS | 总 RPS | p50(ms) | p99(ms) | p999(ms) | 429 | 错误 | 服务端 CPU% | RSS(MB) | MQ 落库 |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        cpu = f"{r['cpu']:.0f}" if r["cpu"] is not None else "-"
        rss = f"{r['rss']:.0f}" if r["rss"] is not None else "-"
        persisted = str(r["persisted"]) if r["persisted"] is not None else "-"
        out.append(
            f"| {r['desc']} | {r['qps']:,} | {r['rps']:,} | {r['p50']:.2f} | {r['p99']:.2f} | "
            f"{r['p999']:.2f} | {r['limited']:,} | {r['errors']:,} | {cpu} | {rss} | {persisted} |"
        )

    other = RES / "lockbench.txt"
    if other.exists():
        out += ["", "## 锁 / Lua 微基准", "", "```", other.read_text().strip(), "```"]

    rb = RES / "redis-benchmark.txt"
    if rb.exists():
        out += ["", "## Redis 单机基线", "", "```", rb.read_text().strip(), "```"]

    target = RES / "summary.md"
    target.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(target)
    print("\n".join(out))


if __name__ == "__main__":
    sys.exit(main())
