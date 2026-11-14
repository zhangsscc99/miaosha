# 第 19 章原始结果

日期 2026-11-14，8 核 macOS 26.5.2，nginx 1.31.8（2 worker），Redis 8.10.3（6390，256MB），
Java 上游 512MB 堆、虚拟线程，压测客户端和所有服务在同一台机器。

| 文件 | 内容 | 什么时候跑的 |
| --- | --- | --- |
| `bench-pilot.json` | 9 项主对照，64 连接，每项 3 秒 | 22:55。本章正文的主表 |
| `bench-scale.json` | 静态页和动态页各 32/64/128/256 连接，每档 2 轮 4 秒 | 22:58。正文「连接数」表 |
| `bench-cache.json` | 冷启动 256 并发，cache lock on/off × 3 轮，注入 20ms 回源 | 23:05 |
| `bench-stale.json` | TTL 过期后 256 并发，cache lock vs stale-while-revalidate × 3 轮 | 23:09 |
| `bench-limits.json` | burst=1 / burst=1000 nodelay / burst=1000 排队，2 轮 | 23:26 |
| `bench-matrix.json` | 主对照的复跑，3 轮，每项 3 秒 | 23:25。压测时 load 17~31，静态页掉到 1.0 万；**只用来说明负载敏感性，不用于正文的绝对值** |
| `cost-model.md` | `scripts/cost_model.py` 生成的容量和成本表 | 由正文参数计算 |

## 读法

- 所有 RPS 都是**同一台机器上的客户端实测**，客户端自己占 300%~450% CPU。静态页 3.8~4.5 万 RPS 是客户端先到瓶颈，不是 nginx 的上限。
- `cpu.nginxPct` 是 nginx master + 两个 worker 在测量窗内的 CPU 时间 / 墙钟；`cpu.edgePct` 是 Java 上游；`cpu.clientPct` 是压测 JVM。macOS 的百分比以单核为 100%，可以超过 100。
- `latency.p50Us`/`p99Us` 是 Java 直方图的桶上界，单位微秒。
- `edgePageDelta`/`edgeRedisDelta` 是测量窗前后 `/stats` 的差值，用来算「多少个请求真的到了上游 / Redis」。
- `bench-cache` 和 `bench-stale` 没有 RPS 字段：它们是一发齐射，不是持续压测；看 `originPageRequests`、`cacheStatus` 和 `latency`。
- `loadavg` 只有后几轮才记录；`bench-pilot` 和 `bench-scale` 跑得早，没有记录 load，正文把这一点写进了边界。

## 复现

```bash
cd high-concurrency-practice/19-nginx-edge-static
bash run.sh pilot     # pilot 模式（1 轮 3 秒）
bash run.sh matrix    # 3 轮 3 秒，会带 loadavg
bash run.sh scale
bash run.sh cache
bash run.sh stale
bash run.sh limits
python3 scripts/cost_model.py
```

`run.sh` 会生成 `runtime/static/p1..p1000.html`、起 Redis 6390、Java 18090、nginx 18076/18078-18089，跑完停掉。
