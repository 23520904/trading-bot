# 17 Repo Layout & Roadmap

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Repository layout & roadmap
Monorepo - contracts first - vertical slices - build order: walking skeleton (M0) then add one vertical slice per milestone

Monorepo layout

cip/
|-- libs/ cip-core (domain types, candle & feature specs, Avro codegen), cip-kafka (producer/consumer: idempotency, DLQ, tracing),
| cip-obs (OTel, logging, metrics), cip-auth (JWT/JWKS), strategy-core (strategy API + execution simulator)
|-- schemas/ avro/*.avsc, openapi/*.yaml, asyncapi/ws.yaml (CI: compatibility checks)
|-- services/ market-collector, historical-ingestor, data-validator, market-normalizer, dlq-replayer, data-quality,
| strategy-engine, paper-execution, portfolio-service, risk-analytics, model-inference, model-monitoring,
| api-gateway, query-api, command-api, realtime-gateway
|-- jobs/ flink/{candle-aggregator, feature-engine, lake-archiver}, spark/{parquet-compaction, candle-backfill, offline-features},
| ml/model-training, research/{pairs-research, backtesting}, airflow/dags
|-- web/ dashboard (React + Vite + TypeScript)
|-- db/ postgres/migrations (alembic per schema), clickhouse/migrations
|-- deploy/ helm/ (per service), kustomize/{dev,staging,prod}, argocd/
|-- tools/ docker-compose.yml (local), seed + replay scripts, load tests (k6, kafka-producer-perf)
`-- docs/ this design pack (.drawio), ADRs

Roadmap with acceptance criteria

| Milestone | Scope | Acceptance criteria |
|---|---|---|
| M0 Walking skeleton (1-2 wk) | docker-compose; collector -> raw -> validator -> normalizer -> ClickHouse ticks; ClickHouse ingests ticks ONLY via Kafka-engine MV (AvroConfluent; no custom sink scripts); query-api /v1/ticks; dashboard line chart; 1 symbol; CI skeleton + contract checks | live tick visible in UI p95 <= 2 s; gap detected, logged, metric emitted, BackfillRequest published to ops.commands.v1 (actual gap filling in M3); invalid message lands in DLQ. Acceptance rule: unit tests alone never satisfy a milestone; integration evidence is required (testcontainers: Kafka + Schema Registry + ClickHouse). |
| M1 Candles & realtime | Flink candle-aggregator; candles in ClickHouse; realtime-gateway (snapshot + delta); Keycloak + api-gateway; DLQ replay. Proposed milestone assignments (needs maintainer approval): collector.subscriptions + last_seq checkpoint = M1; gap-detector publishing BackfillRequest = M1. | tick -> UI p95 <= 500 ms; WS reconnect resumes without gaps; replay 10k DLQ messages without duplicates |
| M2 Platform & observability | K8s dev via Argo CD; OTel -> Grafana stack; feature-engine; data-quality; KEDA. Proposed milestone assignments (needs maintainer approval): K8s Lease leader election = M2. | SLO dashboards live; chaos: kill broker / Flink TaskManager => recovery < 2 min, no data loss |
| M3 Lake & batch | lake-archiver; historical-ingestor; Spark compaction / backfill; Airflow; candle correction. Proposed milestone assignments (needs maintainer approval): backfill execution = M3. | 30-day BTC/ETH backfill < 15 min; recomputed candles equal real-time within tolerance (parity test) |
| M4 Quant research | strategy-core; backtesting; pairs-research; results UI; command-api jobs | backtest reproducible (same run_id inputs => same metrics); pairs report for 20 symbols < 5 min |
| M5 Paper trading | strategy-engine, paper-execution, portfolio-service, risk-analytics, kill-switch | signal -> position p95 <= 1 s; duplicate signal => single order; risk breach halts new orders <= 1 s |
| M6 ML & hardening | model-training, MLflow, model-inference, model-monitoring; load test 5k msg/s; DR drill; security review | drift alert fires on synthetic shift; model rollback < 5 min; RPO / RTO drill passes |
