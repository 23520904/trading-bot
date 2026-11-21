# 08 Data Architecture

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Data architecture - per-store design and ownership
R = read, W = write, U = update - every writer is unique per object (ADR-04) - DDL is the starting point for migrations (alembic / clickhouse-migrate)

PostgreSQL 16 - database cip (schema per owner, one DB role per service)

-- schema collector (owner: market-collector, historical-ingestor)
CREATE TABLE collector.subscriptions(shard_id int, symbol text, channel text, status text, last_seq bigint, updated_at timestamptz DEFAULT now(), PRIMARY KEY(shard_id, symbol, channel));
CREATE TABLE collector.backfill_cursors(symbol text, tf text, cursor_ts timestamptz, status text, PRIMARY KEY(symbol, tf));
-- schema dq (owner: data-quality)
CREATE TABLE dq.incidents(id uuid PRIMARY KEY, symbol text, check_name text, severity text, window_start timestamptz, window_end timestamptz, details jsonb, status text DEFAULT 'open', created_at timestamptz DEFAULT now());
CREATE INDEX ON dq.incidents(symbol, created_at DESC);
-- schema trading (owners: strategy-engine, paper-execution, portfolio-service)
CREATE TABLE trading.strategies(strategy_id text PRIMARY KEY, version text, params jsonb, enabled bool DEFAULT false, updated_at timestamptz);
CREATE TABLE trading.signals(signal_id text PRIMARY KEY, strategy_id text REFERENCES trading.strategies, symbol text, side text, strength real, bar_ts timestamptz, reason text, created_at timestamptz DEFAULT now());
CREATE TABLE trading.orders(order_id uuid PRIMARY KEY, signal_id text UNIQUE REFERENCES trading.signals, account_id text, symbol text, side text, type text, qty numeric(28,10), limit_px numeric(28,10), status text, reject_reason text, created_at timestamptz, updated_at timestamptz);
CREATE TABLE trading.fills(fill_id uuid PRIMARY KEY, order_id uuid REFERENCES trading.orders, qty numeric(28,10), px numeric(28,10), fee numeric(28,10), fee_ccy text, ts timestamptz);
CREATE TABLE trading.positions(account_id text, symbol text, qty numeric(28,10), avg_px numeric(28,10), realized_pnl numeric(28,10), unrealized_pnl numeric(28,10), updated_at timestamptz, PRIMARY KEY(account_id, symbol));
CREATE TABLE trading.pnl_daily(account_id text, day date, realized numeric(28,10), unrealized numeric(28,10), fees numeric(28,10), PRIMARY KEY(account_id, day));
-- schema research (owners: command-api [jobs], backtesting, pairs-research)
CREATE TABLE research.jobs(job_id uuid PRIMARY KEY, type text, params jsonb, status text, requested_by text, created_at timestamptz, started_at timestamptz, finished_at timestamptz, artifact_uri text);
CREATE TABLE research.backtest_runs(run_id uuid PRIMARY KEY, job_id uuid REFERENCES research.jobs, strategy_id text, strategy_version text, start_ts timestamptz, end_ts timestamptz, metrics jsonb, artifact_uri text);
CREATE TABLE research.pair_candidates(pair_id uuid PRIMARY KEY, sym_a text, sym_b text, method text, p_value real, hedge_ratio numeric, half_life_h real, as_of timestamptz, approved bool DEFAULT false);
-- schema risk (owner: risk-analytics)
CREATE TABLE risk.limits(account_id text, rule_id text, metric text, threshold numeric, action text, PRIMARY KEY(account_id, rule_id));
CREATE TABLE risk.snapshots(account_id text, ts timestamptz, gross_exposure numeric, net_exposure numeric, var_95 numeric, max_drawdown numeric, PRIMARY KEY(account_id, ts));
-- schema audit (owner: command-api): audit.events(id uuid PK, actor text, action text, target text, payload jsonb, ts timestamptz)
-- separate databases (owned by the tool): keycloak, mlflow, airflow

ClickHouse - database market (writers: Kafka-engine MVs + spark-batch only)

CREATE TABLE market.ticks (symbol LowCardinality(String), event_ts DateTime64(6,'UTC'), trade_id String, price Decimal(28,10), size Decimal(28,10), side Enum8('BUY'=1,'SELL'=2), ingest_ts DateTime64(6,'UTC'), event_id UUID)
ENGINE = ReplacingMergeTree(ingest_ts) PARTITION BY toYYYYMM(event_ts) ORDER BY (symbol, event_ts, trade_id) TTL toDateTime(event_ts) + INTERVAL 90 DAY TO VOLUME 'cold';
CREATE TABLE market.ticks_queue (...same columns...) ENGINE = Kafka SETTINGS kafka_broker_list='kafka:9093', kafka_topic_list='market.normalized.v1', kafka_group_name='cg.clickhouse.ticks', kafka_format='AvroConfluent', format_avro_schema_registry_url='http://schema-registry:8081', kafka_num_consumers=4;
CREATE MATERIALIZED VIEW market.ticks_mv TO market.ticks AS SELECT * FROM market.ticks_queue;
CREATE TABLE market.candles (symbol LowCardinality(String), tf Enum8('1m'=1,'5m'=2,'15m'=3,'1h'=4,'1d'=5), open_ts DateTime64(3,'UTC'), o Decimal(28,10), h Decimal(28,10), l Decimal(28,10), c Decimal(28,10), volume Decimal(28,10), vwap Decimal(28,10), trade_count UInt32, updated_at DateTime64(6,'UTC'))
ENGINE = ReplacingMergeTree(updated_at) PARTITION BY toYYYYMM(open_ts) ORDER BY (symbol, tf, open_ts);
-- candles_queue (Kafka engine on market.candles.v1) + candles_mv: SELECT ... WHERE is_final = 1. Reads: use argMax(.., updated_at) or FINAL.
CREATE TABLE market.features_log (symbol LowCardinality(String), tf LowCardinality(String), ts DateTime64(3,'UTC'), feature_set_version LowCardinality(String), features Map(String, Float64))
ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (symbol, tf, ts) TTL toDateTime(ts) + INTERVAL 30 DAY;

MinIO buckets (S3 API)

| Bucket / prefix | Writer | Format / partitioning | Lifecycle |
|---|---|---|---|
| cip-lake/normalized/trades/date=YYYY-MM-DD/hour=HH/symbol=X | lake-archiver | Parquet zstd; compacted to >= 128 MB | 365 d then infrequent tier |
| cip-lake/curated/candles/tf=1m/date=... | spark-batch | Parquet | forever |
| cip-lake/features/offline/set=vN/date=... | spark-batch | Parquet (point-in-time correct) | 365 d |
| cip-dlq/YYYY/MM/DD/{event_id}.avro | dlq-replayer | Avro + JSON sidecar (error metadata) | 90 d |
| cip-research/backtests/{run_id} | backtesting | equity.parquet, trades.parquet, report.html | 180 d |
| cip-ml/mlflow-artifacts | MLflow (clients: model-training) | MLflow layout | registered: keep; unregistered 90 d |
| cip-flink/checkpoints/{job} | Flink | RocksDB incremental, retain last 3 | auto |
| cip-backup/{pg-wal, loki, tempo} | PostgreSQL, Loki, Tempo | native formats | 14-30 d |

Redis keyspace (cache semantics: rebuildable from Kafka / ClickHouse; maxmemory-policy volatile-ttl; no persistence required)

| Key | Type | Writer | Readers | TTL |
|---|---|---|---|---|
| snap:candle:{symbol}:{tf} | STRING (JSON) | candle-aggregator | realtime-gateway, query-api, portfolio-service | 24 h |
| snap:last:{symbol} | HASH (px, size, ts) | candle-aggregator | paper-execution | 1 h |
| feat:{symbol}:{tf} | HASH (feature -> value, ts, set_version) | feature-engine | strategy-engine, model-inference, query-api | 10 min |
| ratelimit:{sub}:{route} | STRING counter | api-gateway | api-gateway | 60 s |

Data ownership matrix (service x store)

| Service | PostgreSQL | ClickHouse | Redis | MinIO |
|---|---|---|---|---|
| market-collector | W collector.subscriptions | - | - | - |
| historical-ingestor | RW collector.backfill_cursors | - | - | - |
| data-validator / market-normalizer | - | - | - | - |
| dlq-replayer | - | - | - | W cip-dlq |
| data-quality | W dq.* | R ticks | - | - |
| candle-aggregator | - | - | W snap:* | - |
| feature-engine | - | - | W feat:* | - |
| lake-archiver | - | - | - | W cip-lake/normalized |
| spark-batch | - | W candles (backfill) | - | RW cip-lake/* |
| strategy-engine | RW trading.strategies, signals; R research.pair_candidates | - | R feat:* | - |
| paper-execution | RW trading.orders, fills; R risk.limits | - | R snap:last | - |
| portfolio-service | RW trading.positions, pnl_daily | - | R snap:candle | - |
| risk-analytics | W risk.*; R trading.positions | - | - | - |
| model-inference | - | - | R feat:* | R cip-ml (via MLflow) |
| model-training | - | R features_log | - | R cip-lake/features; W cip-ml |
| model-monitoring | - | R features_log | - | R cip-ml baseline |
| mlflow | own DB mlflow | - | - | W cip-ml |
| pairs-research | W research.pair_candidates | R candles | - | - |
| backtesting | W research.backtest_runs; U research.jobs | R candles, ticks | - | R cip-lake; W cip-research |
| query-api | R trading.*, research.*, dq.* | R candles, ticks, features_log | R snap:*, feat:* | presign cip-research |
| command-api | W research.jobs, trading.strategies.enabled, audit.events | - | - | - |
| realtime-gateway | - | - | R snap:* | - |
| api-gateway | - | - | RW ratelimit:* | - |
