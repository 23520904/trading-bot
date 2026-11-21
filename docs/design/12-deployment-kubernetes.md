# 12 Deployment (Kubernetes)

> Generated from the .drawio file - do not edit by hand.

## Elements


**Group: Kubernetes cluster (managed K8s, node pools: general / data (local NVMe) / batch (spot, optional GPU))**

- ns/ingress-system - • ingress-nginx: Deployment x2, L4 LoadBalancer - - • cert-manager: Deployment x1 (Let's Encrypt) - - • external-dns: Deployment x1
- ns/app - • web-dashboard: Deployment x2 (static) - - • api-gateway: Deployment 2-6, HPA cpu 70% - - • query-api: Deployment 2-6, HPA cpu + p95 - - • command-api: Deployment x2 - - • realtime-gateway: Deployment 2-8, KEDA on ws_connections - - • keycloak: Deployment x2 (DB on cloudnative-pg)
- ns/streaming - • kafka: Strimzi, 3 brokers KRaft, RF=3, PVC 500Gi each, rack-aware - - • schema-registry: Deployment x2 - - • Flink Kubernetes Operator -> FlinkDeployments: candle-aggregator (p=4), feature-engine (p=2), lake-archiver (p=2); RocksDB state, checkpoints to MinIO every 30 s
- ns/data - • postgresql: CloudNativePG 3 instances (1 primary + 2 replicas), WAL archive to MinIO, PITR - - • clickhouse: Altinity operator, 1 shard x 2 replicas + 3 Keeper, NVMe PVC - - • redis: Sentinel x3, cache semantics - - • minio: operator, 4 nodes EC:2, versioning on cip-lake
- ns/ingest - • market-collector: StatefulSet, 1 pod per symbol shard (<= 25 symbols), Lease leader election - - • historical-ingestor: CronJob daily + on-demand Job - - • data-validator: Deployment 2-12, KEDA on Kafka lag - - • market-normalizer: Deployment 2-12, KEDA - - • dlq-replayer: Deployment x1 - - • data-quality: Deployment x2
- ns/trading - • strategy-engine: Deployment x2 (consumer-group partition assignment) - - • paper-execution: Deployment 2-6, KEDA - - • portfolio-service: Deployment x2 (partition by account) - - • risk-analytics: Deployment 1-2
- ns/ml-research - • model-inference: Deployment x2 - - • model-monitoring: Deployment x1 - - • model-training: Job / CronJob (GPU pool optional) - - • backtesting: KEDA ScaledJob on research.jobs.v1 - - • pairs-research: Job - - • mlflow: Deployment x2 (PG backend, MinIO artifacts)
- ns/batch - • airflow: Helm chart, KubernetesExecutor - - • spark-operator: SparkApplications parquet-compaction, candle-backfill, offline-features - - • node pool: batch (spot)
- ns/observability - • otel-collector: DaemonSet agent + Deployment gateway - - • prometheus (kube-prometheus-stack) + alertmanager - - • grafana - - • loki (S3 chunks) - - • tempo (S3)
- ns/platform - • argocd (GitOps), argo-rollouts - - • external-secrets -> Vault / KMS - - • kyverno (policy: signed images, no latest tag) - - • linkerd (mTLS) - - • default-deny NetworkPolicies

## Connections (from -> to : label)

- ns/ingress-system -> ns/app : HTTP 8080 (sync)
- ns/app -> ns/streaming : Kafka 9093 mTLS (async/Kafka)
- ns/streaming -> ns/data : RESP / HTTP / S3 (sync)

## Tables, notes and text blocks

Deployment view - Kubernetes
One cluster, 1 region / 3 zones (A1) - namespaces = trust and ownership zones - GitOps with Argo CD - details of policies below

NetworkPolicy allow-list (default deny)

| From -> To | Port / proto | Purpose |
|---|---|---|
| internet -> ingress-system | 443 TLS | users |
| ingress-system -> app | 8080 HTTP | api-gateway, realtime-gateway, web-dashboard only |
| app -> data | 5432, 8123, 6379 | PG, ClickHouse, Redis: query-api, command-api, realtime-gateway, api-gateway (Redis rate limit) |
| app -> streaming | 9093 mTLS | command-api produce; realtime-gateway consume |
| ingest / trading / ml-research -> streaming | 9093 mTLS, 8081 | Kafka + Schema Registry, ACL per service principal |
| streaming (Flink) -> data | 6379, 8123, 9000 | Redis, ClickHouse, MinIO |
| batch, ml-research -> data | 9000, 8123, 5432 | MinIO, ClickHouse, PG (mlflow DB) |
| * -> observability | 4317 OTLP | telemetry |
| egress allow-list (443) | ingest -> Coinbase only; observability -> Slack/PagerDuty; CI -> GitHub | everything else denied |

Environments

| Env | Description |
|---|---|
| local | docker-compose: 1 Kafka KRaft, Schema Registry, ClickHouse, PG, Redis, MinIO, Keycloak; Flink mini-cluster; 1 symbol, live or recorded-frame replay. |
| dev | small K8s, 1 replica per service, 3 symbols, Argo CD auto-sync from main. |
| staging | prod-like at ~50% size, 20 symbols, nightly load test (k6 + Kafka producer perf: 5k msg/s for 30 min). |
| prod | full size, 100 symbols, manual approval, Argo Rollouts progressive delivery. |

CI/CD pipeline (GitHub Actions + Argo CD)

| # | Stage | Detail |
|---|---|---|
| 1 | lint + types + unit | ruff, mypy, pytest, Flink MiniCluster tests |
| 2 | contract checks | Avro compatibility vs Schema Registry, OpenAPI diff (oasdiff), AsyncAPI lint |
| 3 | integration | testcontainers: Kafka, PG, ClickHouse, Redis, MinIO; recorded Coinbase frames |
| 4 | build + supply chain | container build, SBOM (syft), Trivy (block HIGH), cosign sign, push GHCR |
| 5 | migrations dry-run | alembic (per PG schema), clickhouse-migrate |
| 6 | deploy dev | Argo CD auto-sync |
| 7 | staging gate | smoke tests + load test + parity test (offline vs online features) |
| 8 | deploy prod | manual approval, Argo Rollouts canary, auto-rollback on SLO burn |
