# K8s 参考清单

> 这些 YAML 没有在本机 `apply`（实验机没有 Docker/K8s），是**按生产语义编写的参考实现**，
> 用于说明资源、探针、扩缩容、优雅停机和发布的配置方式。
> 应用层的水平扩容行为已用 `scripts/run_cluster.sh`（3 个 JVM 进程 + 代理）做过等价验证。

## 文件

| 文件 | 内容 |
|---|---|
| `deployment.yaml` | Deployment（4 副本、requests/limits、三种探针、preStop、拓扑分散）+ Service + PDB |
| `hpa.yaml` | HPA：CPU 60% + 自定义 QPS 指标，扩容快、缩容慢 |
| `configmap-ingress.yaml` | 配置 + Nginx Ingress 限流注解 |

## 上手步骤

```bash
# 1. 前置：MySQL / Redis / RabbitMQ 可用（云托管或 Operator）
# 2. 构建镜像
mvn -q package && docker build -t registry.example.com/miaosha-seckill:1.0.0 .
docker push registry.example.com/miaosha-seckill:1.0.0

# 3. 部署
kubectl apply -f deploy/k8s/configmap-ingress.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/hpa.yaml

# 4. 验证
kubectl rollout status deploy/seckill-api
kubectl get hpa seckill-api -w
```

## 需要按环境修改

- 镜像地址、Ingress host；
- MySQL/Redis/MQ 的服务名与凭据（生产用 Secret，不要放 ConfigMap）；
- `requests/limits`：按 08 篇的"单 Pod 2 万 QPS / 4 核"模型调整；
- HPA 的 QPS 阈值：与网关限流阈值、下游容量对齐；
- 副本数下限：至少跨 3 个可用区，每区 2 个。

## 明确没做的

- 没有 NetworkPolicy、ServiceMesh、mTLS；
- 没有 Prometheus/Grafana 的具体部署；
- 没有镜像构建的 Dockerfile（原项目是多模块 Spring Boot，需另建）。
  这些属于平台侧，和本文的高并发实验无关，写进来反而稀释重点。
