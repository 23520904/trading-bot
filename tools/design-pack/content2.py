from lib import *

NS = [
    [('ingress-system', ['ingress-nginx: Deployment x2, L4 LoadBalancer', 'cert-manager: Deployment x1 (Let\'s Encrypt)', 'external-dns: Deployment x1'], 'grey'),
     ('app', ['web-dashboard: Deployment x2 (static)', 'api-gateway: Deployment 2-6, HPA cpu 70%', 'query-api: Deployment 2-6, HPA cpu + p95',
              'command-api: Deployment x2', 'realtime-gateway: Deployment 2-8, KEDA on ws_connections', 'keycloak: Deployment x2 (DB on cloudnative-pg)'], 'blue'),
     ('streaming', ['kafka: Strimzi, 3 brokers KRaft, RF=3, PVC 500Gi each, rack-aware', 'schema-registry: Deployment x2',
                    'Flink Kubernetes Operator -> FlinkDeployments: candle-aggregator (p=4), feature-engine (p=2), lake-archiver (p=2); RocksDB state, checkpoints to MinIO every 30 s'], 'purple'),
     ('data', ['postgresql: CloudNativePG 3 instances (1 primary + 2 replicas), WAL archive to MinIO, PITR', 'clickhouse: Altinity operator, 1 shard x 2 replicas + 3 Keeper, NVMe PVC',
               'redis: Sentinel x3, cache semantics', 'minio: operator, 4 nodes EC:2, versioning on cip-lake'], 'green')],
    [('ingest', ['market-collector: StatefulSet, 1 pod per symbol shard (<= 25 symbols), Lease leader election', 'historical-ingestor: CronJob daily + on-demand Job',
                 'data-validator: Deployment 2-12, KEDA on Kafka lag', 'market-normalizer: Deployment 2-12, KEDA', 'dlq-replayer: Deployment x1', 'data-quality: Deployment x2'], 'blue'),
     ('trading', ['strategy-engine: Deployment x2 (consumer-group partition assignment)', 'paper-execution: Deployment 2-6, KEDA', 'portfolio-service: Deployment x2 (partition by account)',
                  'risk-analytics: Deployment 1-2'], 'blue'),
     ('ml-research', ['model-inference: Deployment x2', 'model-monitoring: Deployment x1', 'model-training: Job / CronJob (GPU pool optional)', 'backtesting: KEDA ScaledJob on research.jobs.v1',
                      'pairs-research: Job', 'mlflow: Deployment x2 (PG backend, MinIO artifacts)'], 'blue'),
     ('batch', ['airflow: Helm chart, KubernetesExecutor', 'spark-operator: SparkApplications parquet-compaction, candle-backfill, offline-features', 'node pool: batch (spot)'], 'orange')],
    [('observability', ['otel-collector: DaemonSet agent + Deployment gateway', 'prometheus (kube-prometheus-stack) + alertmanager', 'grafana', 'loki (S3 chunks)', 'tempo (S3)'], 'grey'),
     ('platform', ['argocd (GitOps), argo-rollouts', 'external-secrets -> Vault / KMS', 'kyverno (policy: signed images, no latest tag)', 'linkerd (mTLS)', 'default-deny NetworkPolicies'], 'grey')],
]

NETPOL = [
    ('internet -> ingress-system', '443 TLS', 'users'),
    ('ingress-system -> app', '8080 HTTP', 'api-gateway, realtime-gateway, web-dashboard only'),
    ('app -> data', '5432, 8123, 6379', 'PG, ClickHouse, Redis: query-api, command-api, realtime-gateway, api-gateway (Redis rate limit)'),
    ('app -> streaming', '9093 mTLS', 'command-api produce; realtime-gateway consume'),
    ('ingest / trading / ml-research -> streaming', '9093 mTLS, 8081', 'Kafka + Schema Registry, ACL per service principal'),
    ('streaming (Flink) -> data', '6379, 8123, 9000', 'Redis, ClickHouse, MinIO'),
    ('batch, ml-research -> data', '9000, 8123, 5432', 'MinIO, ClickHouse, PG (mlflow DB)'),
    ('* -> observability', '4317 OTLP', 'telemetry'),
    ('egress allow-list (443)', 'ingest -> Coinbase only; observability -> Slack/PagerDuty; CI -> GitHub', 'everything else denied'),
]
ENVS = [
    ('local', 'docker-compose: 1 Kafka KRaft, Schema Registry, ClickHouse, PG, Redis, MinIO, Keycloak; Flink mini-cluster; 1 symbol, live or recorded-frame replay.'),
    ('dev', 'small K8s, 1 replica per service, 3 symbols, Argo CD auto-sync from main.'),
    ('staging', 'prod-like at ~50% size, 20 symbols, nightly load test (k6 + Kafka producer perf: 5k msg/s for 30 min).'),
    ('prod', 'full size, 100 symbols, manual approval, Argo Rollouts progressive delivery.'),
]
CICD = [
    ('1', 'lint + types + unit', 'ruff, mypy, pytest, Flink MiniCluster tests'),
    ('2', 'contract checks', 'Avro compatibility vs Schema Registry, OpenAPI diff (oasdiff), AsyncAPI lint'),
    ('3', 'integration', 'testcontainers: Kafka, PG, ClickHouse, Redis, MinIO; recorded Coinbase frames'),
    ('4', 'build + supply chain', 'container build, SBOM (syft), Trivy (block HIGH), cosign sign, push GHCR'),
    ('5', 'migrations dry-run', 'alembic (per PG schema), clickhouse-migrate'),
    ('6', 'deploy dev', 'Argo CD auto-sync'),
    ('7', 'staging gate', 'smoke tests + load test + parity test (offline vs online features)'),
    ('8', 'deploy prod', 'manual approval, Argo Rollouts canary, auto-rollback on SLO burn'),
]


def p_deploy_tables(p, y):
    y = tbl(p, 50, y, 1150, 'NetworkPolicy allow-list (default deny)', ['From -> To', 'Port / proto', 'Purpose'], NETPOL, [3, 2, 5])
    tbl(p, 1250, 130 + 0, 1150, 'Environments', ['Env', 'Description'], ENVS, [1, 6]) if False else None
    y2 = tbl(p, 50, y, 1150, 'Environments', ['Env', 'Description'], ENVS, [1, 6])
    tbl(p, 50, y2, 1150, 'CI/CD pipeline (GitHub Actions + Argo CD)', ['#', 'Stage', 'Detail'], CICD, [1, 3, 8])


SLO = [
    ('Pipeline freshness', 'tick -> candle p95 <= 1 s', 'cip_pipeline_e2e_seconds', 'page if p95 > 2 s for 5 min'),
    ('Dashboard latency', 'tick -> WS update p95 <= 500 ms', 'cip_ws_e2e_seconds', 'warn > 1 s'),
    ('Completeness', 'raw -> normalized >= 99.99% (excluding DLQ)', 'per-symbol per-minute count compare', 'ticket'),
    ('DLQ rate', '<= 0.1% of raw (5 min)', 'invalid_total / frames_total', 'warn 1%, page 5%'),
    ('Feed liveness', 'no symbol silent > 30 s', 'cip_ingest_last_event_age_seconds', 'page'),
    ('API', '99.9% availability (30 d); p95 <= 300 ms (<= 5k rows)', 'http_server_duration', 'burn-rate 2% / 1 h'),
    ('Consumer lag', '< 10k msgs or < 30 s', 'kafka_consumergroup_lag', 'warn after 5 min'),
    ('Paper trading', 'signal -> order p95 <= 1 s', 'cip_signal_to_order_seconds', 'warn'),
    ('Model drift', 'PSI < 0.2 per feature', 'cip_model_psi{model,feature}', 'ticket + retrain job'),
]
OBS = [
    ('Metrics', 'name = cip_<domain>_<name>_<unit>; labels service, symbol (<= 100), tf, topic; no high-cardinality labels (ids).'),
    ('Logs', 'JSON to stdout: ts, level, service, trace_id, span_id, event, symbol; collected by OTel -> Loki.'),
    ('Traces', 'W3C traceparent in HTTP and Kafka headers; head sampling 10% + 100% errors (tail sampling in collector).'),
]
SEC = [
    ('Edge TLS', 'cert-manager, TLS 1.3, HSTS.'),
    ('AuthN', 'Keycloak OIDC code + PKCE; JWT RS256; access 10 min, refresh 8 h; JWKS cached 10 min in api-gateway and realtime-gateway.'),
    ('AuthZ', 'scope -> route in api-gateway; resource checks in services; admin actions written to audit.events.'),
    ('Service-to-service', 'Linkerd mTLS; default-deny NetworkPolicies.'),
    ('Kafka', 'mTLS + ACL per service principal (produce / consume only own topics); Schema Registry writes restricted to CI.'),
    ('Datastores', 'one DB role per service (least privilege), read-only roles for cross-owner reads; secrets via External Secrets -> Vault, rotated 90 d.'),
    ('Secrets', 'none in Git; Coinbase public channels need none (A5).'),
    ('Supply chain', 'SBOM, Trivy gate, cosign signature verified by Kyverno.'),
    ('Data classes', 'market data = public; positions / PnL (paper) = internal; user identity = confidential.'),
]
NFR = [
    ('Throughput', '5,000 msg/s sustained, 20,000 burst (x4 headroom)', 'A2; load test at M6'),
    ('Latency', 'see SLO table', 'A3'),
    ('Availability', 'read path 99.9%; ingestion 99.5% (single region)', 'proposed'),
    ('Durability', 'RPO <= 1 min Kafka (RF=3), <= 5 min PG (WAL), MinIO EC; RTO <= 30 min (Argo CD resync + restore)', 'proposed; DR drill at M6'),
    ('Scalability', '100 -> 500 symbols: partitions 12 -> 48 and Flink parallelism x4 (planned re-key)', 'proposed'),
    ('Consistency', 'eventual across views; per-symbol ordering; ClickHouse dedupe after merge (use argMax / FINAL)', 'ADR-05'),
    ('Cost', 'TBD - budget unknown; first estimate after M2 load test', 'A10'),
]


def p_cross_tables(p, y):
    y = tbl(p, 30, y, 1900, 'SLI / SLO and alerts', ['SLI', 'SLO (proposed)', 'Measurement', 'Alert'], SLO, [2, 4, 4, 3])
    y = tbl(p, 30, y, 1900, 'Telemetry conventions', ['Signal', 'Rule'], OBS, [1, 10])
    y = tbl(p, 30, y, 1900, 'Security controls', ['Control', 'Implementation'], SEC, [1, 10])
    tbl(p, 30, y, 1900, 'Non-functional requirements (all values proposed)', ['Attribute', 'Target', 'Basis'], NFR, [2, 8, 3])


FAIL = [
    ('Coinbase WS disconnect / sequence gap', 'missing ticks', 'gap-detector; feed-liveness alert', 'auto reconnect with backoff, re-subscribe, BackfillRequest via ops.commands -> REST backfill; dedupe absorbs overlap'),
    ('Duplicates (reconnect, replay, at-least-once)', 'double counting', 'dup-rate metric', 'event_id / trade_id dedupe (Flink 10 min state; ClickHouse ReplacingMergeTree); idempotent consumers'),
    ('Out-of-order / late data', 'wrong candles', 'late-events counter', 'watermark 2 s, allowedLateness 30 s; later -> market.late.v1; Spark candle-backfill corrects (updated_at version)'),
    ('Schema drift / unknown fields', 'validator rejects', 'invalid_total by reason', 'DLQ + Schema Registry compatibility; alert; fix then replay'),
    ('Poison pill / bad deploy', 'consumer crash loop', 'restarts, lag', 'DLQ on deserialization error; Argo Rollouts rollback; 3 retries then DLQ'),
    ('Backpressure / consumer lag', 'stale data', 'consumer lag', 'KEDA scale-out up to partition count; Flink backpressure metrics; gateway drops oldest ticker updates'),
    ('Kafka broker loss', 'leader failover', 'under-replicated partitions', 'RF=3, min.insync=2, rack-aware; idempotent producers retry'),
    ('Flink job / checkpoint failure', 'candles stop', 'job status, checkpoint age', 'auto restart from last checkpoint (30 s interval); EOS sinks; alert if checkpoint age > 2 min'),
    ('ClickHouse outage', 'queries fail; ingestion lag in Kafka', 'up, query errors', 'Kafka keeps 7 d -> catch-up; query-api returns 503 or Redis snapshot; 2 replicas'),
    ('Redis outage', 'snapshots / features missing', 'health', 'strategy-engine pauses (no features -> no signals); WS clients resync after recovery; Flink jobs re-warm keys'),
    ('PostgreSQL failover', 'brief write errors', 'CNPG events', 'retry with backoff; idempotent writes; read-only <= 30 s'),
    ('Clock skew', 'wrong timestamps', 'ts-skew histogram', 'chrony; event_ts from exchange; validator rejects > 5 s in future'),
    ('Model drift / bad model', 'bad signals', 'PSI / KS, PnL deviation', 'alert; move alias champion back; strategy-engine ignores stale predictions'),
    ('Strategy runaway (paper)', 'exposure beyond limits', 'risk metrics', 'risk.alerts.v1 -> paper-execution halts; admin toggle via command-api'),
    ('Coinbase rate limit / outage', 'slow backfill', 'HTTP 429', 'token bucket 10 rps, honor Retry-After; resumable cursor'),
]
RISK = [
    ('R1', 'PyFlink maturity / performance', 'M / H', 'benchmark in M1; fallback to Java for hot jobs'),
    ('R2', 'Ops load: Kafka + Flink + Spark + ClickHouse with small team', 'H / H', 'operators, runbooks, staged rollout M0-M6; drop Spark if possible (ADR-01)'),
    ('R3', 'Training-serving skew', 'M / H', 'shared cip-core spec; parity test offline vs online in CI'),
    ('R4', 'Backtest look-ahead / overfitting', 'H / M', 'point-in-time data API, walk-forward, embargo; strategy-core forbids future access'),
    ('R5', 'Coinbase feed / API changes', 'M / M', 'adapter layer + schema versioning; contract tests on recorded fixtures'),
    ('R6', 'ReplacingMergeTree dedupe semantics', 'M / M', 'argMax / FINAL in views; tests'),
    ('R7', 'WebSocket fan-out scale', 'L / M', 'load test 1k clients; Redis fan-out tier (ADR-06)'),
    ('R8', 'Cost overrun', 'M / M', 'A10; scale jobs to zero; spot pool for batch'),
]


def p_fail(p):
    title(p, 'Failure modes & risk register', 'Each failure has detection and mitigation owned by a service spec (pages 15-16)')
    y = tbl(p, 30, 90, 1900, 'Failure modes', ['Failure', 'Impact', 'Detection', 'Mitigation'], FAIL, [3, 2, 3, 8])
    tbl(p, 30, y, 1900, 'Risk register (likelihood / impact)', ['ID', 'Risk', 'L / I', 'Mitigation'], RISK, [1, 5, 1, 8])


SPEC_H = ['Service', 'Responsibility, inputs -> outputs', 'Owned data', 'Stack, scaling, state', 'Idempotency, retry', 'Tests, definition of done']
SPECS = [
    ('market-collector', 'Coinbase WSS -> raw frames. Out: market.raw.v1, ops.commands (BackfillRequest)', 'PG collector.subscriptions', 'Python 3.12, asyncio, websockets, confluent-kafka; StatefulSet, 1 pod per shard; K8s Lease', 'event_id UUIDv7; idempotent producer; reconnect 1-30 s + jitter', 'Recorded-frame replay, chaos disconnect. DoD: 0 gaps after 60 s outage'),
    ('historical-ingestor', 'REST backfill. In: BackfillRequest, schedule. Out: market.raw.backfill.v1', 'PG collector.backfill_cursors', 'Python httpx + tenacity; CronJob / Job; cursor in PG', 'resumable by cursor; 429 -> Retry-After; downstream dedupe', 'VCR cassettes. DoD: 30 d BTC-USD < 15 min'),
    ('data-validator', 'Gate: Avro decode + rules (price > 0, size > 0, ts skew, symbol whitelist, seq monotonic). In: raw, raw.backfill. Out: validated, dlq', '-', 'Python, confluent-kafka, fastavro; Deployment 2-12, KEDA on lag; stateless', 'manual commit after produce; poison -> DLQ at once; transient retry 3x', 'Property tests (hypothesis). DoD: invalid never reaches validated'),
    ('market-normalizer', 'Canonical model (BTC-USD, Decimal, UTC us, side enum). In: validated. Out: normalized, dlq', '-', 'Python; Deployment 2-12, KEDA; stateless', 'deterministic mapping => replay-safe', 'Golden files per channel. DoD: 100% fields mapped'),
    ('dlq-replayer', 'Archive dead letters; replay on command. In: dlq, ops.commands. Out: MinIO cip-dlq, raw.backfill', 'MinIO cip-dlq/', 'Python; Deployment x1', 'event_id preserved, ingest_mode=replay', 'testcontainers. DoD: replay 10k msgs, 0 duplicates downstream'),
    ('data-quality', 'Windowed checks: gaps, staleness, dup-rate, price spikes (z > 8), volume anomalies. In: normalized, late. Out: incidents, metrics', 'PG dq.*', 'Python; Deployment x2 (partitioned); 1-min windows', 'incident key (symbol, check, window) upsert', 'Fault injection. DoD: detects 1-min gap within 90 s'),
    ('candle-aggregator', 'Candles 1m..1d (OHLCV, VWAP, count). In: normalized. Out: candles, late, Redis snap:*', 'Redis snap:candle:*, snap:last:*', 'Flink 1.19, RocksDB, p=4, checkpoint 30 s', 'dedupe trade_id 10 min; EOS Kafka txn; Redis SET idempotent', 'MiniCluster + out-of-order fixtures. DoD: equals Spark recompute (parity)'),
    ('feature-engine', 'Online features from candles. In: candles. Out: features.online.v1, Redis feat:*', 'Redis feat:*', 'Flink, p=2, spec from cip-core', 'EOS; HSET idempotent; feature_set_version', 'Offline vs online parity. DoD: max abs diff < 1e-9'),
    ('lake-archiver', 'Normalized trades -> Parquet in MinIO. In: normalized', 'MinIO cip-lake/normalized', 'Flink FileSink, Parquet + zstd, hourly roll, p=2', 'exactly-once via checkpoint-committed files', 'Read-back test. DoD: row count = Kafka offsets'),
    ('spark-batch', 'Compaction, candle-backfill, offline-features. In: MinIO, CH. Out: MinIO curated, CH candles', 'MinIO cip-lake/curated, features', 'Spark 3.5 on K8s operator; spot executors', 'overwrite partitions (idempotent); run_id logged', 'Small-data local tests. DoD: backfill = real-time candles within tolerance'),
    ('airflow', 'Orchestrates batch DAGs and retrains. Out: SparkApplications, K8s Jobs, research.jobs.v1', 'own metadata DB', 'Airflow 2 Helm, KubernetesExecutor', 'idempotent tasks; retries 3; SLA alerts', 'DAG integrity tests. DoD: backfill DAG re-run is a no-op'),
    ('strategy-engine', 'Runs strategies per bar. In: candles, predictions, Redis feat:*, PG strategies / pairs, ops.commands. Out: signals.v1', 'PG trading.strategies, trading.signals', 'Python, strategy-core; Deployment x2 (partition assignment)', 'signal_id = hash(strategy, symbol, bar_ts); idempotent publish', 'Deterministic replay: same candles => same signals. DoD: parity with backtest'),
    ('paper-execution', 'Simulated fills (fees, slippage, latency) + pre-trade risk check. In: signals, risk.alerts. Out: orders.v1, fills.v1', 'PG trading.orders, fills', 'Python; Deployment 2-6 (KEDA); ExecutionPort impl', 'orders.signal_id UNIQUE; TX write then publish (outbox)', 'Simulator unit tests. DoD: duplicate signal => 1 order'),
    ('portfolio-service', 'Positions, avg price, realized / unrealized PnL. In: fills, snap:candle. Out: positions.v1', 'PG trading.positions, pnl_daily', 'Python; Deployment x2 (partition by account)', 'fill_id dedupe; UPSERT idempotent', 'Ledger invariants (property tests). DoD: sum of fills = position'),
    ('risk-analytics', 'Exposure, VaR95, drawdown; limit breach alerts. In: positions. Out: risk.alerts.v1, PG risk.*', 'PG risk.*', 'Python, numpy / pandas; Deployment 1-2', 'alert key (account, rule_id, window)', 'Scenario tests. DoD: breach => alert <= 1 s'),
    ('model-inference', 'Online scoring. In: features.online.v1, MLflow alias. Out: predictions.v1', '- (model cache)', 'Python, MLflow client, ONNX / sklearn; Deployment x2; hot-swap', 'idempotent by (symbol, ts, model_version)', 'Golden predictions. DoD: p95 < 50 ms per event'),
    ('model-training', 'Train, evaluate, register. In: MinIO features, research.jobs.v1. Out: MLflow runs / models', 'MLflow (cip-ml)', 'Python (sklearn / xgboost / PyTorch); K8s Job, GPU optional', 'seeded; data snapshot id logged', 'Reproducibility test. DoD: same data + seed => same metrics'),
    ('model-monitoring', 'Drift (PSI / KS) and performance decay. In: features, predictions. Out: metrics, research.jobs.v1 (retrain)', '-', 'Python; Deployment x1', 'windowed; alert dedupe in Alertmanager', 'Synthetic shift. DoD: alert fires on PSI > 0.2'),
    ('mlflow', 'Tracking + model registry', 'PG db mlflow; MinIO cip-ml', 'MLflow 2; Deployment x2', '-', 'Smoke test. DoD: registry meets API SLO'),
    ('pairs-research', 'Cointegration (Engle-Granger, Johansen), hedge ratio, half-life. In: CH candles, research.jobs.v1', 'PG research.pair_candidates', 'Python, statsmodels; K8s Job', 'upsert by job_id', 'Synthetic cointegrated series. DoD: 20 symbols < 5 min'),
    ('backtesting', 'Event-driven backtests via strategy-core + simulator. In: CH / MinIO data, research.jobs.v1', 'PG research.backtest_runs; MinIO cip-research', 'Python; KEDA ScaledJob', 'run_id = hash(strategy_version, params, data_snapshot)', 'Golden-run regression, no look-ahead test. DoD: reproducible'),
    ('api-gateway', 'AuthN/Z, routing, rate limit. In: HTTPS. Out: HTTP to query-api / command-api', 'Redis ratelimit:*', 'FastAPI + authlib; Deployment 2-6, HPA; stateless', 'stateless', 'OpenAPI contract tests. DoD: 401 / 403 / 429 covered'),
    ('query-api', 'Read side: candles, ticks, features, trading, results. Out: JSON', '- (RO roles on CH / PG / Redis)', 'FastAPI, clickhouse-connect, asyncpg, redis-py; Deployment 2-6, HPA', 'GET idempotent; ETag / Cache-Control', 'Schemathesis. DoD: p95 <= 300 ms'),
    ('command-api', 'Write side: jobs, strategy toggle, DLQ replay; audit', 'PG research.jobs, audit.events', 'FastAPI; Deployment x2', 'Idempotency-Key; TX + outbox publish', 'Contract + authZ tests. DoD: every command audited'),
    ('realtime-gateway', 'WSS fan-out, snapshot + delta. In: Kafka candles / signals / positions, Redis snap:*', '-', 'Starlette WS, aiokafka, redis-py; Deployment 2-8, HPA on connections', 'per-channel seq; resync on gap', 'Load test 1k clients. DoD: server-side p95 <= 100 ms'),
    ('web-dashboard', 'UI: charts, research, paper trading, reconnect UX', '-', 'React 18, TypeScript, Vite, lightweight-charts, TanStack Query; nginx static', '-', 'Playwright e2e. DoD: reconnect and resync tested'),
]
WID = [2, 6, 3, 5, 4, 5]


def p_specs(p, part):
    half = len(SPECS) // 2
    rows = SPECS[:half] if part == 1 else SPECS[half:]
    title(p, f'Service spec cards ({part}/2)', 'One row = one deployable container - implement against: topic catalog (06), contracts (07), data ownership (08) - DoD = definition of done')
    tbl(p, 30, 90, 2000, 'Specs', SPEC_H, rows, WID)


REPO = """cip/
|-- libs/        cip-core (domain types, candle & feature specs, Avro codegen), cip-kafka (producer/consumer: idempotency, DLQ, tracing),
|                cip-obs (OTel, logging, metrics), cip-auth (JWT/JWKS), strategy-core (strategy API + execution simulator)
|-- schemas/     avro/*.avsc, openapi/*.yaml, asyncapi/ws.yaml          (CI: compatibility checks)
|-- services/    market-collector, historical-ingestor, data-validator, market-normalizer, dlq-replayer, data-quality,
|                strategy-engine, paper-execution, portfolio-service, risk-analytics, model-inference, model-monitoring,
|                api-gateway, query-api, command-api, realtime-gateway
|-- jobs/        flink/{candle-aggregator, feature-engine, lake-archiver}, spark/{parquet-compaction, candle-backfill, offline-features},
|                ml/model-training, research/{pairs-research, backtesting}, airflow/dags
|-- web/         dashboard (React + Vite + TypeScript)
|-- db/          postgres/migrations (alembic per schema), clickhouse/migrations
|-- deploy/      helm/ (per service), kustomize/{dev,staging,prod}, argocd/
|-- tools/       docker-compose.yml (local), seed + replay scripts, load tests (k6, kafka-producer-perf)
`-- docs/        this design pack (.drawio), ADRs"""
ROAD = [
    ('M0 Walking skeleton (1-2 wk)',
     'docker-compose; collector -> raw -> validator -> normalizer -> ClickHouse ticks; ClickHouse ingests ticks ONLY via Kafka-engine MV (AvroConfluent; no custom sink scripts); query-api /v1/ticks; dashboard line chart; 1 symbol; CI skeleton + contract checks',
     'live tick visible in UI p95 <= 2 s; gap detected, logged, metric emitted; invalid message lands in DLQ. Acceptance rule: unit tests alone never satisfy a milestone; integration evidence is required (testcontainers: Kafka + Schema Registry + ClickHouse).'),
    ('M1 Candles & realtime',
     'Flink candle-aggregator; candles in ClickHouse; realtime-gateway (snapshot + delta); Keycloak + api-gateway; DLQ replay. Proposed milestone assignments (needs maintainer approval): collector.subscriptions + last_seq checkpoint = M1; gap-detector publishing BackfillRequest = M1.',
     'tick -> UI p95 <= 500 ms; WS reconnect resumes without gaps; replay 10k DLQ messages without duplicates; gap-detector publishes BackfillRequest to ops.commands.v1'),
    ('M2 Platform & observability',
     'K8s dev via Argo CD; OTel -> Grafana stack; feature-engine; data-quality; KEDA. Proposed milestone assignments (needs maintainer approval): K8s Lease leader election = M2.',
     'SLO dashboards live; chaos: kill broker / Flink TaskManager => recovery < 2 min, no data loss'),
    ('M3 Lake & batch',
     'lake-archiver; historical-ingestor; Spark compaction / backfill; Airflow; candle correction. Proposed milestone assignments (needs maintainer approval): backfill execution = M3.',
     '30-day BTC/ETH backfill < 15 min; recomputed candles equal real-time within tolerance (parity test)'),
    ('M4 Quant research', 'strategy-core; backtesting; pairs-research; results UI; command-api jobs', 'backtest reproducible (same run_id inputs => same metrics); pairs report for 20 symbols < 5 min'),
    ('M5 Paper trading', 'strategy-engine, paper-execution, portfolio-service, risk-analytics, kill-switch', 'signal -> position p95 <= 1 s; duplicate signal => single order; risk breach halts new orders <= 1 s'),
    ('M6 ML & hardening', 'model-training, MLflow, model-inference, model-monitoring; load test 5k msg/s; DR drill; security review', 'drift alert fires on synthetic shift; model rollback < 5 min; RPO / RTO drill passes'),
]


def p_repo(p):
    title(p, 'Repository layout & roadmap', 'Monorepo - contracts first - vertical slices - build order: walking skeleton (M0) then add one vertical slice per milestone')
    y = code(p, 30, 90, 1900, 'Monorepo layout', REPO, fs=11)
    tbl(p, 30, y, 1900, 'Roadmap with acceptance criteria', ['Milestone', 'Scope', 'Acceptance criteria'], ROAD, [2, 6, 6])
