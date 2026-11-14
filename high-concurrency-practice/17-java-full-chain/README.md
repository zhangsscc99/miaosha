# 第 17 章：Java 全链路高并发实践（可复现实验 + 面试技术报告）

这个目录不是 PPT 架构，而是一套**能在本机跑起来、有真实压测数据**的高并发实验：
从"数据库直接扣库存"开始，逐层加上 Redis Lua、本地库存缓存、消息队列异步下单、限流与售罄标记，
每一层都有独立的对照实验、资源采样和中文文档。

> 所有数字都来自本章目录 的真实运行（3 轮，取中位数与最佳值），
> 不是拍脑袋的"理论上支持百万 QPS"。压测环境、口径、波动原因在
> [`docs/01-压测方法与资源基线.md`](docs/01-压测方法与资源基线.md) 里写清楚了。

## 一分钟看结论

同一台 8 vCPU / 16 GB 机器、同一个 512 MB 堆的服务进程、同样的 64 并发：

| 方案 | 成功 QPS（中位 / 最佳） | p50 | p99 | 每单 DB 写语句 | 每单 Redis 调用 |
|---|---:|---:|---:|---:|---:|
| v0 DB 直接扣减 + 同步落库 | 4,721 / 4,784 | 12.8 ms | 24.9 ms | 2.0 | 0 |
| v1 Redis Lua + 同步落库 | 17,473 / 18,622 | 2.7 ms | 12.1 ms | 1.0 | 1 |
| v2 Redis Lua + MQ 异步 | 19,389 / 24,044 | 2.3 ms | 8.7 ms | 0.012 | 1 |
| v3 本地库存 + MQ 异步 | 19,405 / **31,460** | **1.8 ms** | **6.3 ms** | **0.008** | **0** |
| v4 v3 + 入口限流 5,000/s | 5,013（被限流值） | 6.2 ms | 47.1 ms | — | 0 |

关键不是"31,460"这个绝对值（本机还有浏览器、IDE 等后台进程，绝对值偏保守），而是四个比例：

- **DB 换成 Redis Lua**：QPS ×3.9，p50 从 12.8 ms 降到 2.7 ms；
- **同步落库换成 MQ 异步 + 批量写**：每单 DB 写语句从 1.0 降到 0.012（**约 80 倍**），
  MySQL 写压力从 7,841 条/s 降到 111 条/s；
- **Redis 扣减换成本地库存 + 售罄标记**：热路径 Redis 调用降为 **0**，
  售罄后拒绝能力从 1.29 万 RPS 提到 4.38 万 RPS，且 MySQL 写入归零；
- **分布式锁换 Lua**：同一热点 key，分布式锁 1.31 万 ops/s，Lua 14.34 万 ops/s（**11 倍**），
  p99 从 12 ms 降到 0.17 ms。

正确性：100 件库存、3,000 个不同用户并发抢，v2/v3 都恰好生成 100 个订单，
DB 库存对账后为 0，**不超卖、不少卖、0 错误**。3 Pod 集群模式同样通过。

## 目录

```
17-java-full-chain/
├── docs/
│   ├── 01-压测方法与资源基线.md        # 环境、口径、v0 基线、为什么轮次间会波动
│   ├── 02-本地库存与售罄标记.md        # 分段计数、库存批发、Pod 配额、对账
│   ├── 03-Redis与Lua原子扣减.md        # EVAL/EVALSHA、Cluster 热 key、容量换算
│   ├── 04-分布式锁的代价.md            # 锁 vs Lua vs 本地锁，什么时候才用锁
│   ├── 05-MQ异步下单与削峰.md          # RabbitMQ 拓扑、确认、批量落库、幂等
│   ├── 06-限流降级与售罄.md            # 令牌桶、分层限流、售罄页、快速失败
│   ├── 07-数据库层优化.md              # 批量写、唯一索引幂等、分库分表、连接池
│   ├── 08-K8s部署与弹性扩容.md         # 清单、HPA、优雅停机、冷启动、容量
│   ├── 09-容量与成本模型.md            # 10万/100万/1000万 QPS 的资源与账单
│   └── 10-面试技术报告.md              # 一份可直接讲 45 分钟的主报告
├── src/main/java/com/geekq/lab/        # 实验服务、压测客户端、锁微基准、正确性验证
├── deploy/k8s/                         # Deployment / Service / HPA / PDB 参考清单
├── scripts/                            # 构建、全量实验、集群实验、报告与成本模型
├── sql/lab_schema.sql                  # 独立实验库 miaosha_lab
└── results/                            # 压测原始数据、资源采样、自动汇总
```

## 快速复现（macOS / Linux）

前置：JDK 21+、Maven、MySQL、Redis、RabbitMQ 都已在本机启动。

```bash
cd high-concurrency-practice/17-java-full-chain
mysql -uroot -proot < sql/lab_schema.sql   # 建 miaosha_lab 库（不影响原业务库）
bash scripts/build.sh                      # 编译 + 生成依赖 classpath
bash scripts/run_all.sh                    # 5 种模式 × 15s + 售罄 + 正确性 + 锁基准
bash scripts/run_cluster.sh                # 3 进程模拟 3 Pod + L7 代理
python3 scripts/report.py                  # 生成 results/summary.md
python3 scripts/cost_model.py              # 容量/成本模型
```

只想手动跑一档：

```bash
java -Xmx512m -cp "target/classes:$(cat target/cp.txt)" \
  com.geekq.lab.SeckillServer --mode v3 --port 18080
curl "http://127.0.0.1:18080/admin/reset?stock=100000"
java -Xmx512m -cp "target/classes:$(cat target/cp.txt)" \
  com.geekq.lab.LoadGen --url "http://127.0.0.1:18080/seckill?goodsId=1" \
  --seconds 15 --warmup 3 --concurrency 64
```

## 实验边界（先看这里，避免误读）

- **单机、无网络延迟**：客户端、服务、Redis、MySQL、RabbitMQ 全在同一台 8 核笔记本上，
  服务端 CPU 采样只有 1~1.6 核，说明瓶颈常常在压测客户端或共享资源，绝对值不代表生产容量。
- **3 轮波动明显**（v3：13.7k ~ 31.5k）：机器上还跑着浏览器、IDE 等进程，
  轮次间 CPU 争抢不同。文档同时给出中位数、最佳值和波动原因，**比例比绝对值可靠**。
- **K8s 只做了 3 进程等价模拟**，没有在真实集群 `apply`（本机没有 Docker）；
  清单是参考实现，HPA/滚动发布的行为按 K8s 语义推导，文档中已标注。
- **没有做 TLS、网关鉴权、风控、真实支付**：这些会显著增加延迟，但不改变本文的优化排序。

## 这个项目回答的面试问题

1. 秒杀为什么不能直接 `update stock = stock - 1`？瓶颈到底在哪一行？
2. Redis 扣库存为什么必须用 Lua？和分布式锁差多少？
3. 本地缓存库存怎么保证不超卖？Pod 挂了、发布新版本怎么办？
4. MQ 异步下单，消息丢了/重复了怎么办？用户看到"排队中"之后呢？
5. 10 万、100 万、1000 万 QPS 分别需要多少机器、多少钱？钱花在哪一层？
6. K8s HPA 为什么扩不出来？JVM 冷启动和本地库存怎么配合？
7. 为什么说"限流不是可选项"，限流阈值又该怎么定？

答案都在文档里，主报告在 [`docs/10-面试技术报告.md`](docs/10-面试技术报告.md)。
