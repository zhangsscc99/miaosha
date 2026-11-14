#!/usr/bin/env python3
"""第 19 章的容量和成本表。所有输入都是本机实测的单请求 CPU，不是厂商标称值。

口径：
  - 4 核 8G ECS 按 940 元/月，1GB Redis 社区版按 151.2 元/月（和第 1、17 章一致）。
  - 单请求 CPU 用「进程 CPU% / QPS」从实测算出来；4 核按 100% 可用，不再加余量。
  - 页面 8,106 字节，gzip 后 1,906 字节（实测）。
  - CDN 流量按 0.2 元/GB 的目录价，只算流量，不含请求数费用。
  - 边缘缓存和静态页的回源按 1000 个商品、2 秒 TTL 算：1,000 / 2 = 500 次/秒。
"""
import math

PAGE = 8106
GZIP = 1906
STATIC_US = 29.6      # 静态页：nginx 120.0% / 40,538 QPS
DYN_US = 97.0         # 动态页：nginx 58.1% + edge 116.6% / 18,016 QPS
CACHE_US = 40.4       # 边缘缓存命中：nginx 95.5% + edge 8.5% / 25,758 QPS
GZIP_US = 59.3        # 静态 + gzip：nginx 187.0% / 31,541 QPS
OPENFILE_US = 14.8    # open_file_cache：nginx 50.4% / 37,251 QPS

ECS = 940.0
REDIS = 151.2
CDN_GB = 0.2
CORES = 4
REDIS_QPS = 100_000
GOODS = 1000
TTL = 2
ORIGIN = GOODS / TTL


def machines(qps, us):
    return max(1, math.ceil(qps * us / 1e6 / CORES))


def money(qps):
    dyn_m = machines(qps, DYN_US)
    sta_m = machines(qps, STATIC_US)
    cache_m = machines(qps, CACHE_US)
    gzip_m = machines(qps, GZIP_US)
    ofc_m = machines(qps, OPENFILE_US)
    dyn_r = math.ceil(qps / REDIS_QPS)
    return {
        "qps": qps,
        "dyn_m": dyn_m,
        "sta_m": sta_m,
        "cache_m": cache_m,
        "gzip_m": gzip_m,
        "ofc_m": ofc_m,
        "dyn_r": dyn_r,
        "dyn_cost": dyn_m * ECS + dyn_r * REDIS,
        "sta_cost": sta_m * ECS,
        "cache_cost": cache_m * ECS + REDIS,
        "gzip_cost": gzip_m * ECS,
        "ofc_cost": ofc_m * ECS,
    }


rows = [money(q) for q in (100_000, 1_000_000, 10_000_000)]

out = []
out.append("# 第 19 章容量和成本（由 scripts/cost_model.py 生成）\n")
out.append("单请求 CPU（实测）：静态 %.1f µs，动态 %.1f µs，边缘缓存命中 %.1f µs，"
           "gzip %.1f µs，open_file_cache %.1f µs。\n" % (STATIC_US, DYN_US, CACHE_US, GZIP_US, OPENFILE_US))
out.append("| 点击 QPS | 全动态：4c8g | 全动态：Redis 分片 | 全动态月费 | 静态页：4c8g | 静态页月费 | 省 |")
out.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
for r in rows:
    out.append("| {qps:,} | {dyn_m} | {dyn_r} | ¥{dyn_cost:,.0f} | {sta_m} | ¥{sta_cost:,.0f} | ¥{save:,.0f} |".format(
        qps=r["qps"], dyn_m=r["dyn_m"], dyn_r=r["dyn_r"], dyn_cost=r["dyn_cost"],
        sta_m=r["sta_m"], sta_cost=r["sta_cost"], save=r["dyn_cost"] - r["sta_cost"]))
out.append("")
out.append("| 点击 QPS | 边缘缓存：4c8g | 边缘缓存月费 | 回源 QPS | 静态 + open_file_cache：4c8g | 月费 |")
out.append("| ---: | ---: | ---: | ---: | ---: | ---: |")
for r in rows:
    out.append("| {qps:,} | {cache_m} | ¥{cache_cost:,.0f} | {origin} | {ofc_m} | ¥{ofc_cost:,.0f} |".format(
        qps=r["qps"], cache_m=r["cache_m"], cache_cost=r["cache_cost"], origin=ORIGIN,
        ofc_m=r["ofc_m"], ofc_cost=r["ofc_cost"]))
out.append("")
out.append("## 带宽\n")
out.append("| 点击 QPS | 原始 | gzip | 原始 1 小时流量 | gzip 1 小时流量 | 原始 1 小时 CDN | gzip 1 小时 CDN | 省 |")
out.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
for q in (100_000, 1_000_000, 10_000_000):
    g1 = q * PAGE / 1e9
    g2 = q * GZIP / 1e9
    tb1 = g1 * 3600 / 1000
    tb2 = g2 * 3600 / 1000
    c1 = tb1 * 1000 * CDN_GB
    c2 = tb2 * 1000 * CDN_GB
    out.append("| {q:,} | {g1:.1f} GB/s / {gb1:.0f} Gbps | {g2:.1f} GB/s / {gb2:.0f} Gbps | {tb1:,.0f} TB | {tb2:,.0f} TB | ¥{c1:,.0f} | ¥{c2:,.0f} | ¥{cs:,.0f} |".format(
        q=q, g1=g1, gb1=g1 * 8, g2=g2, gb2=g2 * 8, tb1=tb1, tb2=tb2, c1=c1, c2=c2, cs=c1 - c2))
out.append("")
out.append("## gzip 的 CPU 账\n")
out.append("| 点击 QPS | 不压缩机器 | gzip 机器 | 多买 | 多付/月 | 1 小时省流量费 | 划算吗 |")
out.append("| ---: | ---: | ---: | ---: | ---: | ---: | --- |")
for r in rows:
    extra = r["gzip_m"] - r["sta_m"]
    g1 = r["qps"] * PAGE / 1e9
    g2 = r["qps"] * GZIP / 1e9
    save = (g1 - g2) * 3600 / 1000 * 1000 * CDN_GB
    out.append("| {q:,} | {a} | {b} | {e} | ¥{m:,.0f} | ¥{s:,.0f} | {v} |".format(
        q=r["qps"], a=r["sta_m"], b=r["gzip_m"], e=extra, m=extra * ECS, s=save,
        v="活动超过 %.1f 小时才回本" % (extra * ECS / save) if save > 0 else "-"))
out.append("")
print("\n".join(out))
import os
p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results", "cost-model.md")
open(p, "w", encoding="utf-8").write("\n".join(out) + "\n")
