# 秒杀高并发实践

这个目录给面试和学习用。每一章都在这台机器上用很小的资源跑过，原始数字在对应章节的 `results/*.json`。没跑过的技术不写进「已经验证」的结论。

推送分支：`yuting`。

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
| 09 消息队列进程 | 还没做 | RabbitMQ、Kafka、RocketMQ 的进程还没跑。dockerd 和 kubelet 也没跑，第 8 章只写了它们会写进内核的那三个文件 |

面试时先看 [技术报告](REPORT.md)，需要复现再进章节目录执行 `bash run.sh`。
