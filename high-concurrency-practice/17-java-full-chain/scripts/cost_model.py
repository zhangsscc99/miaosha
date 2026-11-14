# -*- coding: utf-8 -*-
"""容量与成本模型：所有价格/容量假设集中在此，改一处即可重算（文档引用其输出）。"""
import math

PRICE = dict(app=400, redis=500, mq=500, mysql=3000, base=1000)  # 元/月，包年包月折算


def plan(qps, per_node, headroom=1.5):
    """按 50% 余量规划节点数，至少 1 台。"""
    return max(1, math.ceil(qps / per_node * headroom))


print("方案定义：A=DB 同步扣减(v0)，B=Redis Lua+MQ(v2)，C=本地库存+MQ(v3)")
print("单机规划吞吐：A 4,000 QPS/4c8g（受 MySQL 单行写限制）；B 10,000；C 20,000")
print("价格：app/redis/mq 4c8g=¥400/500/500 月，MySQL 8c32g=¥3,000 月，负载均衡+监控=¥1,000 月")
print()
header = f"{'QPS':>12} | {'A pods':>6} {'A DB':>5} {'A 月成本':>10} | {'B pods':>6} {'Redis':>5} {'B 月成本':>9} | {'C pods':>6} {'C 月成本':>9} | C省A | C省B"
print(header)
print("-" * len(header))
for qps in (100_000, 1_000_000, 10_000_000):
    a_pods = plan(qps, 4000)
    a_db = max(1, math.ceil(qps / 4000))
    a = a_pods * PRICE['app'] + a_db * PRICE['mysql'] + PRICE['base']

    b_pods = plan(qps, 10000)
    b_redis = max(1, math.ceil(qps / 100_000)) * 3
    b = b_pods * PRICE['app'] + b_redis * PRICE['redis'] + 3 * PRICE['mq'] + 2 * PRICE['mysql'] + PRICE['base']

    c_pods = plan(qps, 20000)
    c = c_pods * PRICE['app'] + 3 * PRICE['redis'] + 3 * PRICE['mq'] + 2 * PRICE['mysql'] + PRICE['base']

    print(f"{qps:>12,} | {a_pods:>6} {a_db:>5} {a:>10,} | {b_pods:>6} {b_redis:>5} {b:>9,} | "
          f"{c_pods:>6} {c:>9,} | {1-c/a:>4.0%} | {1-c/b:>4.0%}")

print()
print("边缘拦截后（CDN/边缘节点挡住 90%~95%，只回源剩余请求，大促持续 10 分钟）：")
for qps, absorb in ((100_000, 0.90), (1_000_000, 0.90), (10_000_000, 0.95)):
    origin = int(qps * (1 - absorb))
    pods = plan(origin, 20000)
    edge = qps * 600 * 0.5 / 1_000_000  # 600 秒 × ¥0.5/百万请求
    print(f"  峰值 {qps:>11,} QPS -> 回源 {origin:>9,} QPS, C 方案 {pods:>2} 个 pod, 边缘费用约 ¥{edge:,.0f}/次(10min)")

print()
print("10 分钟大促的弹性成本（仅算新增 app pod，¥400/月 ≈ ¥0.556/小时）：")
for qps, absorb in ((100_000, 0.90), (1_000_000, 0.90), (10_000_000, 0.95)):
    origin = int(qps * (1 - absorb))
    pods = plan(origin, 20000)
    print(f"  {qps:>11,} QPS: {pods:>2} pods × 10min ≈ ¥{pods*400/730*10/60:,.1f}")
