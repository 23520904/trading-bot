-- Migration 001: ClickHouse Market Ticks table, Errors table, Kafka Engine queue, and Materialized Views
-- Per CIP Design Pack (08-data-architecture.md, ADR-04, ADR-13)
--
-- Note on timestamp fields ('received_at' vs 'event_ts' / 'ingest_ts'):
-- 'received_at' is defined exclusively on RawFrame (market.raw.v1) representing the epoch microsecond
-- timestamp when market-collector receives an incoming WebSocket frame.
-- Once validated and normalized into MarketTrade (market.normalized.v1), the canonical event carries:
--   - 'event_ts': execution timestamp from the exchange (microseconds UTC)
--   - 'ingest_ts': platform normalization timestamp (microseconds UTC), used for ReplacingMergeTree deduplication.
-- Therefore, market.ticks stores 'event_ts' and 'ingest_ts', aligned with page 08 and MarketTrade.avsc.

CREATE DATABASE IF NOT EXISTS market;

-- 1. Main Ticks storage table with ReplacingMergeTree deduplication on (symbol, event_ts, trade_id)
-- Explicit UTC timezone on all timestamps (Architecture Invariant 8, ADR-04)
CREATE TABLE IF NOT EXISTS market.ticks (
    symbol LowCardinality(String),
    event_ts DateTime64(6, 'UTC'),
    trade_id String,
    price Decimal(28, 10),
    size Decimal(28, 10),
    side Enum8('BUY' = 1, 'SELL' = 2),
    ingest_ts DateTime64(6, 'UTC'),
    event_id UUID
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (symbol, event_ts, trade_id);

-- 2. Ticks Errors table capturing corrupt, unparseable, or schema-incompatible messages
-- Observability & Alert hook:
--   Prometheus Alert: ClickHouseTicksErrorsDetected
--   Expr: sum by (topic) (increase(clickhouse_table_rows{database="market", table="ticks_errors"}[1m])) > 0
CREATE TABLE IF NOT EXISTS market.ticks_errors (
    topic LowCardinality(String),
    partition UInt64,
    offset UInt64,
    raw_message String,
    error String,
    error_ts DateTime64(6, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(error_ts)
ORDER BY (topic, partition, offset, error_ts);

-- 3. Kafka Engine Queue consuming canonical MarketTrade events in Confluent wire format
-- Templated variables:
--   ${KAFKA_BROKER_LIST:-kafka:9092}
--   ${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}
-- Poison pill handling (ClickHouse 24.3): kafka_handle_error_mode = 'stream' captures corrupt / unparseable
-- payloads into virtual columns _error and _raw_message without halting the consumer stream.
CREATE TABLE IF NOT EXISTS market.ticks_queue (
    symbol LowCardinality(String),
    event_ts Int64,
    trade_id String,
    price Decimal(28, 10),
    size Decimal(28, 10),
    side Enum8('BUY' = 1, 'SELL' = 2),
    ingest_ts Int64,
    event_id String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${KAFKA_BROKER_LIST:-kafka:9092}',
         kafka_topic_list = 'market.normalized.v1',
         kafka_group_name = 'cg.clickhouse.ticks',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = '${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}',
         kafka_handle_error_mode = 'stream',
         kafka_num_consumers = 1;

-- 4. Materialized View routing valid trades into market.ticks
-- Uses fromUnixTimestamp64Micro(ts, 'UTC') returning DateTime64(6, 'UTC') directly.
-- Filters out poison pills (WHERE _error = '') to guarantee ingestion continuity.
CREATE MATERIALIZED VIEW IF NOT EXISTS market.ticks_mv TO market.ticks AS
SELECT
    symbol,
    fromUnixTimestamp64Micro(event_ts, 'UTC') AS event_ts,
    trade_id,
    price,
    size,
    side,
    fromUnixTimestamp64Micro(ingest_ts, 'UTC') AS ingest_ts,
    toUUID(event_id) AS event_id
FROM market.ticks_queue
WHERE _error = '';

-- 5. Materialized View routing corrupt / unparseable messages into market.ticks_errors
CREATE MATERIALIZED VIEW IF NOT EXISTS market.ticks_errors_mv TO market.ticks_errors AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    _raw_message AS raw_message,
    _error AS error,
    now64(6, 'UTC') AS error_ts
FROM market.ticks_queue
WHERE _error != '';
