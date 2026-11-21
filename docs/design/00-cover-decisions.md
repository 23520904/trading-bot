# 00 Cover & Decisions

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Crypto Intelligence Platform (CIP) - Architecture Design Pack
v2.0 - 21 Nov 2026 - Scope: real-time market-data pipeline, analytics, ML, quant research and PAPER trading (no live order routing) - Method: C4 model (L1-L3) + data-flow, data, API, sequence and deployment views - one level per page, one flow direction per diagram

How to read / build
Engineers: 06 -> 07 -> 08 -> 15-16 -> 17 (contracts first, then data, then spec cards).
Reviewers: 01 -> 02..05 -> 12 -> 14.
Every arrow in pages 02-05 maps to a topic (06) or an API (07); every container has a spec card (15-16) and appears in a sequence (10-11).
Colours (max 5): blue app, orange processing engine, purple Kafka, green datastore, grey external.

Assumptions (verify, ask if wrong)

| ID | Assumption / change vs draft |
|---|---|
| A1 | Single Kubernetes cluster, 1 region / 3 zones, cloud-agnostic (MinIO = S3 API). |
| A2 | Up to 100 spot symbols, <= 5,000 msg/s sustained (about 50 msg/s per symbol). |
| A3 | Latency targets: tick -> dashboard p95 <= 500 ms; tick -> signal p95 <= 1 s. |
| A4 | Team of 3-6; Python 3.12 default; Flink via PyFlink (Java fallback for hot paths). |
| A5 | Coinbase public market-data channels only; no trading permission, no trading keys exist. |
| A6 | Keycloak self-hosted; <= 50 users. |
| A7 | Retention: Kafka raw 3 d, normalized 7 d; lake 365 d; ticks in ClickHouse 90 d hot; candles forever. |
| A8 | Observability = Grafana LGTM (Prometheus, Loki, Tempo, Grafana) + OpenTelemetry. |
| A9 | ADDED vs draft: Keycloak, Schema Registry, dlq-replayer, query-api, command-api, realtime-gateway, lake-archiver, Airflow, MLflow, Tempo, Alertmanager, Argo CD. |
| A10 | REMOVED vs draft: candle-service, feature-service (-> Flink jobs), "Producers" box, Kubernetes as a data-flow node (-> deployment view). Budget unknown: cost = TBD. |

Page index

| Page | Content | Primary reader |
|---|---|---|
| 00 | Cover, assumptions, decisions (ADR) | Everyone - read first |
| 01 | L1 System Context | Executives, new joiners |
| 02 | L2-A Access & request path | Backend / frontend / security |
| 03 | L2-B Ingestion & normalization | Data engineers |
| 04 | L2-C Stream & batch processing, stores | Data engineers |
| 05 | L2-D Quant, ML & paper trading | Quant / ML / trading engineers |
| 06 | Kafka topic catalog & conventions | All service owners (contract) |
| 07 | Event schemas & API contracts (REST, WebSocket) | All service owners (contract) |
| 08 | Data architecture: DDL, tables, buckets, keys, ownership | Backend / data engineers |
| 09 | L3 Component views | Implementers of the 4 key containers |
| 10 | Sequence diagrams A (real-time) | Implementers, testers |
| 11 | Sequence diagrams B (research, ML) | Implementers, testers |
| 12 | Deployment (Kubernetes), environments, CI/CD | DevOps / SRE |
| 13 | Observability, security, SLO, NFR | SRE / security |
| 14 | Failure modes & risk register | Everyone |
| 15-16 | Service spec cards (26 containers) | Implementers - definition of done |
| 17 | Repo layout & roadmap (M0-M6) | Tech lead, PM |

Design decisions (ADR-lite)

| ID | Decision | Alternatives | Trade-off / consequence |
|---|---|---|---|
| ADR-01 | Lambda-lite. Kafka = system of record for streams. Flink = ONLY real-time compute (candles, features, archiving). Spark = batch ONLY (compaction, backfill, offline features). | Full Lambda with duplicated logic; Kappa with Flink only. | Two engines cost ops effort. Mitigation: candle/feature definitions live once in cip-core. Revisit at M3: with <= 100 symbols replace Spark by Flink batch / Polars. |
| ADR-02 | Remove standalone candle-service and feature-service; Flink jobs candle-aggregator and feature-engine are the single owners. | Keep Python services next to Flink. | Removes the duplicated responsibility in the draft; one real-time code path; batch recompute uses the same spec. |
| ADR-03 | data-validator = per-message gate (schema + rules -> DLQ). data-quality = windowed monitoring (gaps, staleness, volume anomalies) -> incidents + metrics. | Merge into one service. | Different state and latency profile; validator stays stateless and scales with KEDA. |
| ADR-04 | Data ownership: schema-per-service in PostgreSQL with separate roles; ClickHouse market.* written only by Kafka-engine MVs and spark-batch; MinIO prefix, Redis key prefix per writer. | Shared DB with free access (draft). | No hidden coupling. Cross-owner reads use read-only roles or owner API (matrix on page 08). |
| ADR-05 | At-least-once everywhere + idempotent consumers (event_id, trade_id). Flink -> Kafka uses exactly-once transactions. Key = symbol => per-symbol order. ClickHouse dedupe via ReplacingMergeTree. | End-to-end exactly-once. | Simpler and robust; duplicates are expected and handled. Cost: dedupe state in Flink (10 min TTL). |
| ADR-06 | Live fan-out: realtime-gateway consumes Kafka directly (ephemeral groups, latest, no commit) + Redis snapshot on subscribe; sequence number per channel. | Redis Pub/Sub (no replay / seq); separate bridge service. | Fewer hops, lower latency. Revisit at > 1,000 concurrent clients: add a Redis fan-out tier. |
| ADR-07 | api-gateway = authN/Z, routing, rate-limit only. CQRS: query-api (reads) / command-api (writes). WebSocket bypasses api-gateway. | Gateway reading databases (draft). | Removes layer bypass; services scale independently; long-lived WS do not pin the gateway. |
| ADR-08 | Backtest/live parity: strategies implemented once in strategy-core; execution simulator (fees, slippage, latency) shared by paper-execution and backtesting. | Separate research and live code. | Avoids research-to-production drift; strategy-core version recorded with every run. |
| ADR-09 | MLflow (tracking + registry; backend PG, artifacts MinIO). Promotion by alias champion; model-inference polls alias (60 s) and hot-swaps; model-monitoring uses PSI / KS vs training baseline. | Custom registry. | Standard tooling; rollback = move alias. |
| ADR-10 | Paper-trading boundary: ExecutionPort interface implemented only by paper-execution. Live trading would add live-execution in a separate trust zone behind pre-trade risk gate + 2-person approval. | Build live trading now. | Safe scope; no exchange trading credentials in the platform. |
| ADR-11 | Avro + Schema Registry, compatibility BACKWARD_TRANSITIVE; topics versioned .vN; breaking change = new topic + dual-publish for >= 1 retention period. | JSON without schema. | Contract tests in CI block incompatible changes. |
| ADR-12 | Keycloak OIDC (JWT RS256), scopes market:read, trading:read, research:write, ops:admin; service-to-service mTLS (Linkerd); Kafka mTLS + ACL per service principal. | Static API keys. | Central identity, short-lived tokens, least privilege. |
| ADR-13 | Wire format: every Avro topic uses Confluent wire format (magic byte 0x00 + 4-byte schema ID). One shared serializer/deserializer in cip-kafka; subject = <topic>-value; schemas registered by CI (auto.register.schemas=false outside local dev). ClickHouse AvroConfluent and Flink read the same format. | Raw schemaless Avro bytes without wire header. | Allows ClickHouse Kafka-engine AvroConfluent and Flink to decode seamlessly with Schema Registry schema ID caching. |
| ADR-14 | Source feed: Coinbase Exchange public WebSocket, channels matches/ticker/heartbeat; use the exchange sequence as source_seq. | Use purely local counter or poll REST ticker. | Enables deterministic gap detection matching exchange order-matching engine sequence. |
| ADR-15 | Feed liveness and reconnect: staleness is measured per symbol on any frame including heartbeat; a symbol is stale after 5 s without a frame; metric and gap event are emitted and do NOT fail pod-level /readyz. Pod /readyz = WS connected + Kafka producer reachable. Reconnect sleep = min(30, backoff * uniform(0.5, 1.5)) starting at 1 s. | Unbounded backoff without jitter; failing /readyz on single symbol staleness. | Guarantees fast failover without cascading pod restarts while alerting on isolated symbol feed drops. |
