#!/usr/bin/env python3
"""第 18 章的容量/成本模型。

输入全部来自 results/ 里实测出来的 JSON：
- 各模式的 RPS（3 轮取中位数）
- 各模式每次请求打到 MySQL 的比例（dbQueries / requests）
- 共享布隆单分片的 ops/s（redis-bloom.json）
- 布隆的字节/元素、空值占位的字节/key

价格口径和第 17 章一致（公开刊例价的量级，只做相对比较）：
应用 4vCPU/8GB ¥400/月，Redis 4vCPU/8GB ¥500/月，MySQL 8vCPU/32GB ¥3,000/月。
"""
import json
import glob
import math
import statistics

PRICE = {
    "app": 400.0,
    "redis": 500.0,
    "mysql": 3000.0,
}
MYSQL_QPS_PER_PRIMARY = 4000.0   # 第 17 章实测的单主库单行写入/查询量级
HEADROOM = 1.5                    # 滚动发布、故障域、突发
TIERS = [100_000, 1_000_000, 10_000_000]
MODES = ["none", "nullcache", "bloom", "bloomredis", "bloomnull"]


def load(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def med(values):
    return statistics.median(values) if values else 0.0


def collect():
    out = {}
    for mode in MODES:
        for wl in ("scan", "replay"):
            rps, ratios = [], []
            for f in glob.glob(f"results/e2e-{mode}-{wl}-r*.json"):
                rnd = f.split("-r")[-1].split(".")[0]
                e = load(f)
                s = load(f"results/stats-{mode}-{wl}-r{rnd}.json")
                if not e or not s:
                    continue
                rps.append(e["rps"])
                if s["requests"]:
                    ratios.append(s["dbQueries"] / s["requests"])
            out[(mode, wl)] = {
                "rps": med(rps),
                "rounds": len(rps),
                "rpsAll": rps,
                "dbPerReq": med(ratios),
                # 冷启动（第 1 轮）的落库比例：TTL 到期、新 id 第一次访问时就是这一档
                "dbPerReqCold": max(ratios) if ratios else 0.0,
            }
    return out


def main():
    data = collect()
    rb = load("results/redis-bloom.json", {})
    bp = load("results/bloom-params.json", {})
    shared_ops = rb.get("luaSingle", {}).get("opsPerSec", 156000)

    configs = {c["bitsPerKey"]: c for c in bp.get("configs", []) if c["keys"] == 1_000_000}
    bytes_per_key = configs.get(10, {}).get("bytesPerKey", 1.25)
    fp10 = configs.get(10, {}).get("fpRateMeasured", 0.0082)

    lines = []
    def p(s=""):
        print(s)
        lines.append(s)

    p("# 第 18 章容量与成本模型（由实测数据生成）")
    p()
    p(f"生成时间：{__import__('datetime').datetime.now().isoformat(timespec='seconds')}")
    p()
    p("## 1. 实测输入")
    p()
    p("| 模式 | 流量 | RPS（3 轮中位） | 每请求落库（中位） | 冷启动落库 | 轮次 |")
    p("|---|---|---:|---:|---:|---:|")
    for mode in MODES:
        for wl in ("scan", "replay"):
            d = data[(mode, wl)]
            p(f"| {mode} | {wl} | {d['rps']:,.0f} | {d['dbPerReq']*100:.2f}% | "
              f"{d['dbPerReqCold']*100:.2f}% | {d['rounds']} |")
    p()
    p(f"共享布隆单分片（一次 EVALSHA 查 k 个位）：**{shared_ops:,.0f} ops/s**，"
      f"{rb.get('luaSingle',{}).get('latency',{}).get('p99Us','?')} µs p99。")
    p(f"进程内布隆：**{bp.get('singleThread',{}).get('nsPerLookup','?')} ns/次**，"
      f"8 线程合计 **{bp.get('multiThread',{}).get('opsPerSec',0):,.0f} ops/s**。")
    p(f"10 位/元素：**{bytes_per_key:.2f} 字节/key**，实测误判率 **{fp10*100:.2f}%**"
      f"（理论 {configs.get(10,{}).get('fpRateTheory',0)*100:.2f}%）。")
    p()

    # ---- 2. 资源 ----
    p("## 2. 10 万 / 100 万 / 1000 万 QPS 需要多少机器")
    p()
    p("口径：应用 Pod 数 = 峰值 QPS ÷ 本章实测单 Pod RPS × 余量 1.5；")
    p("MySQL 主库 = 峰值 QPS × 每请求落库比例 ÷ 4,000 × 余量 1.5；")
    p("共享布隆的 Redis 分片 = 峰值 QPS ÷ 单分片 ops/s × 余量 1.5（进程内布隆不占 Redis）。")
    p()
    p("| 峰值 QPS | 模式 | App Pod | MySQL 主库 | Redis 分片 | 月成本 |")
    p("|---:|---|---:|---:|---:|---:|")
    table = {}
    for tier in TIERS:
        for mode in MODES:
            rps = data[(mode, "scan")]["rps"]
            db = data[(mode, "scan")]["dbPerReqCold"]
            pods = max(1, math.ceil(tier / max(rps, 1) * HEADROOM))
            dbs = 0 if db == 0 else max(1, math.ceil(tier * db / MYSQL_QPS_PER_PRIMARY * HEADROOM))
            redis = 0
            if mode == "bloomredis":
                redis = max(1, math.ceil(tier / shared_ops * HEADROOM))
            cost = pods * PRICE["app"] + dbs * PRICE["mysql"] + redis * PRICE["redis"]
            table[(tier, mode)] = (pods, dbs, redis, cost)
            p(f"| {tier:,} | {mode} | {pods:,} | {dbs:,} | {redis:,} | ¥{cost:,.0f} |")
    p()

    p("## 3. 省在哪：布隆 vs 无防护 vs 空值占位")
    p()
    p("| 峰值 QPS | 无防护 | 空值占位 | 进程内布隆 | 共享布隆 | 布隆比无防护省 |")
    p("|---:|---:|---:|---:|---:|---:|")
    for tier in TIERS:
        a = table[(tier, "none")][3]
        b = table[(tier, "nullcache")][3]
        c = table[(tier, "bloom")][3]
        d = table[(tier, "bloomredis")][3]
        p(f"| {tier:,} | ¥{a:,.0f} | ¥{b:,.0f} | ¥{c:,.0f} | ¥{d:,.0f} | "
          f"¥{a-c:,.0f}（{(1-c/a)*100:.0f}%） |")
    p()

    # ---- 4. 内存 ----
    p("## 4. 1 亿个 id 的布隆 vs 空值占位")
    p()
    null_bytes = 64
    p("| id 数 | 布隆 10 位/元素 | 空值占位（64 字节/key） | 差值 |")
    p("|---|---:|---:|---:|")
    for n in (100_000_000, 1_000_000_000):
        b = n * bytes_per_key / (1024 ** 3)
        nb = n * null_bytes / (1024 ** 3)
        p(f"| {n:,} | {b:.2f} GB | {nb:.2f} GB | {nb-b:.2f} GB |")
    p()
    p(f"按 1 亿个 id：布隆 {100_000_000*bytes_per_key/1024**2:.0f} MB，"
      f"空值占位 {100_000_000*null_bytes/1024**3:.1f} GB。")
    p()

    # ---- 5. 误判的代价 ----
    p("## 5. 误判率就是钱：0.82% 的误判在 1000 万 QPS 上是多少")
    p()
    p("| 误判率 | 1000 万 QPS 时漏进存储的 QPS | 需要的 MySQL 主库（4,000/s） | 月成本 |")
    p("|---:|---:|---:|---:|")
    for fpr in (0.01, fp10, 0.001):
        leak = 10_000_000 * fpr
        dbs = math.ceil(leak / MYSQL_QPS_PER_PRIMARY * HEADROOM)
        p(f"| {fpr*100:.2f}% | {leak:,.0f} | {dbs:,} | ¥{dbs*PRICE['mysql']:,.0f} |")
    p()
    p("误判只是「多查一次库」，不会把不存在的商品卖出去。真正的成本是误判率乘上峰值 QPS。")
    lines_txt = "\n".join(lines) + "\n"
    with open("results/cost-model.txt", "w") as f:
        f.write(lines_txt)
    print("\nwritten to results/cost-model.txt")


if __name__ == "__main__":
    main()
