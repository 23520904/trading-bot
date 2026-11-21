# 15 Service Specs 1-2

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Service spec cards (1/2)
One row = one deployable container - implement against: topic catalog (06), contracts (07), data ownership (08) - DoD = definition of done

Specs

| Service | Responsibility, inputs -> outputs | Owned data | Stack, scaling, state | Idempotency, retry | Tests, definition of done |
|---|---|---|---|---|---|
| market-collector | Coinbase WSS -> raw frames. Out: market.raw.v1, ops.commands (BackfillRequest) | PG collector.subscriptions | Python 3.12, asyncio, websockets, confluent-kafka; StatefulSet, 1 pod per shard; K8s Lease | event_id UUIDv7; idempotent producer; reconnect 1-30 s + jitter | Recorded-frame replay, chaos disconnect. DoD: 0 gaps after 60 s outage |
| historical-ingestor | REST backfill. In: BackfillRequest, schedule. Out: market.raw.backfill.v1 | PG collector.backfill_cursors | Python httpx + tenacity; CronJob / Job; cursor in PG | resumable by cursor; 429 -> Retry-After; downstream dedupe | VCR cassettes. DoD: 30 d BTC-USD < 15 min |
| data-validator | Gate: Avro decode + rules (price > 0, size > 0, ts skew, symbol whitelist, seq monotonic). In: raw, raw.backfill. Out: validated, dlq | - | Python, confluent-kafka, fastavro; Deployment 2-12, KEDA on lag; stateless | manual commit after produce; poison -> DLQ at once; transient retry 3x | Property tests (hypothesis). DoD: invalid never reaches validated |
| market-normalizer | Canonical model (BTC-USD, Decimal, UTC us, side enum). In: validated. Out: normalized, dlq | - | Python; Deployment 2-12, KEDA; stateless | deterministic mapping => replay-safe | Golden files per channel. DoD: 100% fields mapped |
| dlq-replayer | Archive dead letters; replay on command. In: dlq, ops.commands. Out: MinIO cip-dlq, raw.backfill | MinIO cip-dlq | Python; Deployment x1 | event_id preserved, ingest_mode=replay | testcontainers. DoD: replay 10k msgs, 0 duplicates downstream |
| data-quality | Windowed checks: gaps, staleness, dup-rate, price spikes (z > 8), volume anomalies. In: normalized, late. Out: incidents, metrics | PG dq.* | Python; Deployment x2 (partitioned); 1-min windows | incident key (symbol, check, window) upsert | Fault injection. DoD: detects 1-min gap within 90 s |
| candle-aggregator | Candles 1m..1d (OHLCV, VWAP, count). In: normalized. Out: candles, late, Redis snap:* | Redis snap:candle:*, snap:last:* | Flink 1.19, RocksDB, p=4, checkpoint 30 s | dedupe trade_id 10 min; EOS Kafka txn; Redis SET idempotent | MiniCluster + out-of-order fixtures. DoD: equals Spark recompute (parity) |
| feature-engine | Online features from candles. In: candles. Out: features.online.v1, Redis feat:* | Redis feat:* | Flink, p=2, spec from cip-core | EOS; HSET idempotent; feature_set_version | Offline vs online parity. DoD: max abs diff < 1e-9 |
| lake-archiver | Normalized trades -> Parquet in MinIO. In: normalized | MinIO cip-lake/normalized | Flink FileSink, Parquet + zstd, hourly roll, p=2 | exactly-once via checkpoint-committed files | Read-back test. DoD: row count = Kafka offsets |
| spark-batch | Compaction, candle-backfill, offline-features. In: MinIO, CH. Out: MinIO curated, CH candles | MinIO cip-lake/curated, features | Spark 3.5 on K8s operator; spot executors | overwrite partitions (idempotent); run_id logged | Small-data local tests. DoD: backfill = real-time candles within tolerance |
| airflow | Orchestrates batch DAGs and retrains. Out: SparkApplications, K8s Jobs, research.jobs.v1 | own metadata DB | Airflow 2 Helm, KubernetesExecutor | idempotent tasks; retries 3; SLA alerts | DAG integrity tests. DoD: backfill DAG re-run is a no-op |
| strategy-engine | Runs strategies per bar. In: candles, predictions, Redis feat:*, PG strategies / pairs, ops.commands. Out: signals.v1 | PG trading.strategies, trading.signals | Python, strategy-core; Deployment x2 (partition assignment) | signal_id = hash(strategy, symbol, bar_ts); idempotent publish | Deterministic replay: same candles => same signals. DoD: parity with backtest |
| paper-execution | Simulated fills (fees, slippage, latency) + pre-trade risk check. In: signals, risk.alerts. Out: orders.v1, fills.v1 | PG trading.orders, fills | Python; Deployment 2-6 (KEDA); ExecutionPort impl | orders.signal_id UNIQUE; TX write then publish (outbox) | Simulator unit tests. DoD: duplicate signal => 1 order |
