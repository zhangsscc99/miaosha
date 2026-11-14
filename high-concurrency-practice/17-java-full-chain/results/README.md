# results 说明

| 文件 | 内容 |
|---|---|
| `raw-*.jsonl` | 每轮实验的原始输出（loadgen / mysql / correctness / lockbench） |
| `load-<场景>.json` | 最近一轮该场景的压测结果（QPS、分位、错误数） |
| `stats-<场景>.json` | 服务端 `/stats`：请求数、成功/售罄/限流、MQ 投递与落库 |
| `ps-<场景>.txt` | 每秒服务端 CPU%（`ps %cpu`）与 RSS（KB） |
| `mysql-<场景>.json` | 该场景 `Com_insert + Com_update + Com_delete` 增量与每秒写语句 |
| `redis-<场景>.txt` | `INFO commandstats`，看每单 Redis 调用次数 |
| `server-*.log` | 服务进程日志 |
| `final/` | 第 3 轮的完整文件（三轮对比见 `summary.md`） |
| `cluster/` | 3 进程 Pod 模拟：聚合压测、各 Pod 统计、正确性 |
| `lockbench.txt` | 本地锁 / Redis 锁 / Lua 微基准 |
| `redis-benchmark.txt` | Redis 单机 SET/GET/INCR/EVAL 上限 |
| `cost-model.txt` | 容量与成本模型输出 |
| `summary.md` | 自动汇总（`python3 scripts/report.py`） |

## 读数注意

- 本机 8 vCPU 同时跑服务、客户端、Redis、MySQL、RabbitMQ，**绝对值偏保守**；
- 三轮之间 CPU 争抢不同，v3 在 13.7k~31.5k 之间波动，**比例比绝对值可靠**；
- `ps %cpu` 是进程累计 CPU 时间/已运行时间，不能精确到毫秒级，但足以判断瓶颈在应用还是下游；
- 售罄场景的成功 QPS 为 0，要看 `rps`（总请求）和 `mysql` 写入是否归零。

## 重新生成

```bash
bash scripts/run_all.sh        # 会覆盖顶层 load-/stats-/ps-/mysql- 文件，并追加 raw-*.jsonl
bash scripts/run_cluster.sh    # 覆盖 cluster/
python3 scripts/report.py      # 重新生成 summary.md
```
