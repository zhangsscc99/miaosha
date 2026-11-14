# 秒杀高并发实践

这个目录给面试和学习用。每一章都在这台机器上用很小的资源跑过，原始数字在对应章节的 `results/*.json`。没跑过的技术不写进「已经验证」的结论。

推送分支：`Yuting/modify`。

| 章节 | 状态 | 这一章回答的问题 |
| --- | --- | --- |
| [01 Redis Lua 原子扣库存](01-redis-lua-atomic-stock/README.md) | 2026-11-12 已实测 | 怎样不超卖，为什么热 key 上不要用分布式锁，售罄流量怎样不把 Redis 和数据库打满，扩到 10 万 / 100 万 / 1000 万点击 QPS 要买什么 |
| [02 Redis Stream 异步落库](02-redis-stream-async-order/README.md) | 2026-11-12 已实测 | 胜出的人怎样入队再落库，重复投递和失败重试怎样不多单、不占着队列，点击加到 1000 万 QPS 时消息集群要不要跟着买 |
| [03 成功扣减拆到两个 Redis 进程](03-redis-stock-shard/README.md) | 2026-11-12 已实测 | 同一个商品的成功扣减怎样超过一个核，为什么同进程拆 key 没用，取模和搬库存会把账做成什么样 |
| [04 多实例售罄标记和准入额度](04-multi-instance-admission/README.md) | 2026-11-12 已实测 | 多台应用的售罄标记互不相通时漏进 Redis 多少次，本地额度怎样把总和留在一个分片里，限流放在 Redis 里为什么省不了核 |
| [05 商品详情缓存击穿和 singleflight](05-detail-cache-singleflight/README.md) | 2026-11-12 已实测 | 详情页过期时有多少请求打进数据库，singleflight、分布式锁和逻辑过期各收成几次，热 key 的 GET 为什么不能靠加进程变快 |
| [06 多 key 同时过期](06-cache-avalanche/README.md) | 2026-11-12 已实测 | 一批 key 一起过期时回源是多少次，TTL 抖动和单个重建者各能压住什么，singleflight 为什么收不掉雪崩 |
| [07 未支付订单到期关单](07-unpaid-order-delay/README.md) | 2026-11-12 已实测 | 超时关单怎样不多加库存，键 TTL 为什么不是延迟队列，一个连接和 16 个连接差多少，每个点击都建单并留 2 小时要买多少 Redis |
| [08 cgroup 的 CPU 和内存限额](08-cgroup-cpu-limit/README.md) | 2026-11-12 已实测 | Kubernetes 的 limit 和 request 写进 cpu.max、cpu.weight、memory.max 之后，一个热 key 的 QPS 和尾巴怎么变，500m 为什么省不下核，进程被 cgroup 杀掉和 Redis 自己返回 OOM 差在哪 |
| [09 一个 256MB 堆的 Kafka](09-kafka-winner-queue/README.md) | 2026-11-12 已实测 | 胜出者进一个 Kafka 进程之后，位移先提交会丢多少，一个分区为什么喂不满两个消费者，按点击把 200 字节都写进日志要几台机器 |
| [10 固定窗口的跨秒双倍额度](10-fixed-window-boundary/README.md) | 2026-11-12 已实测 | 固定窗口在秒边界为什么会放出两倍，滑动计数和令牌桶各守住哪一种形状，拦截器的 GET 再写会放进多少次，扩到 10 万 / 100 万 / 1000 万时下游和限流器各要多买什么 |
| [11 本机一个 RabbitMQ 4.4.0](11-rabbitmq-winner-queue/README.md) | 2026-11-12 已实测 | 胜出者进一个 RabbitMQ 进程之后，manual 和不确认会留下多少未确认消息，prefetch 1 为什么喂不满，confirm 要等多少，单节点 quorum 在 kill -9 之后还剩几条，扩到 10 万 / 100 万 / 1000 万点击时队列和云上按条计费各要什么 |
| [12 本机一个 RocketMQ 5.5.1](12-rocketmq-flush-delay/README.md) | 2026-11-12 已实测 | 胜出者进一个 RocketMQ 进程之后，异步刷盘和同步刷盘差多少，延迟级别的存储时间为什么不能当等待，一个队列为什么喂不满两个消费者，扩到 10 万 / 100 万 / 1000 万点击时自建和按条计费各要什么 |
| [13 同机 Redis 主从和 WAIT](13-redis-replica-wait/README.md) | 2026-11-12 已实测 | 一个热 key 的从库能不能分担成功扣减，WAIT 比异步复制慢多少，从库断开和 lag 各放进多少写，kill -9 之后主库和从库各剩多少，同机一起读是几倍 |
| [14 同机 Redis Sentinel](14-redis-sentinel-failover/README.md) | 2026-11-12 已实测 | 主库被 kill -9 以后从库多久开始应答、地址多久改口，quorum 和多数派差在哪，旧主库还活着时已经返回的写入会丢掉多少，每个点击都问地址要多买多少核 |
| [15 同机 Redis Cluster 的槽](15-redis-cluster-slot/README.md) | 2026-11-12 已实测 | 三个主库为什么没有把一个热 key 变成三倍，MOVED 和 ASK 差在哪，槽迁到一半时 Lua 会怎样，一个主库被杀掉之后活着的槽还能不能读，扩到 10 万 / 100 万 / 1000 万时分片和空转的主库各是什么账 |
| [16 同机 Redis Cluster 的副本和切换](16-redis-cluster-failover/README.md) | 2026-11-12 已实测 | 副本为什么没有把热 key 的写变成两倍，手动切换、FORCE 和 TAKEOVER 各丢掉多少已经返回的递增，多数派不够时为什么不会自动切换，扩到 10 万 / 100 万 / 1000 万时主库和副本各是一笔什么账 |
| [17 Java 全链路：MySQL + Redis + RabbitMQ](17-java-full-chain/README.md) | 2026-11-14 已实测 | 把前 16 章的单点结论拼成一条完整 Java 链路：DB 直接扣减只有 4,721 QPS，换 Redis Lua 到 1.7 万，换本地库存 + MQ 批量落库到 3.1 万（p50 1.8 ms），每单 DB 写从 2 条降到 0.008 条；限流、售罄标记、对账、3 Pod 批发库存、10 万 / 100 万 / 1000 万 QPS 的机器和月费都在里面。含 10 篇中文文档和面试报告 |
| [18 缓存穿透和布隆过滤器](18-bloom-penetration/README.md) | 2026-11-14 已实测 | 请求的 id 根本不存在时，空值占位在扫号下为什么是负优化（RPS -10%，落库仍 90%），布隆怎样把落库从 100% 压到 10.74%，进程内 21.85 ns/次和共享位图 15.7 万/秒差在哪，1000 万商品只要 12.5MB、1 亿只要 119MB，10 万 / 100 万 / 1000 万 QPS 省 84%~86% 的机器和月费 |
| 19 | 还没做 | 真的第二台机器、DLedger 还没跑。这台机器没有 dockerd，也没有 kubelet。第 16 章的副本和第 15 章的槽都在同一台机器上；第 17 章的 3 个 Pod 也是同一台机器上的 3 个 JVM 进程 |

面试时先看 [技术报告](REPORT.md)，需要复现再进章节目录执行 `bash run.sh`。
