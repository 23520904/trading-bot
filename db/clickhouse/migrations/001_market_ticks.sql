-- Migration 001: ClickHouse Market Ticks table, Kafka Engine queue, and Materialized View
-- Per CIP Design Pack (08-data-architecture.md, ADR-04, ADR-13)

CREATE DATABASE IF NOT EXISTS market;

-- 1. Main Ticks storage table with ReplacingMergeTree deduplication on (symbol, event_ts, trade_id)
-- Explicit UTC timezone on all timestamps (Architecture Invariant 8)
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

-- 2. Kafka Engine Queue consuming canonical MarketTrade events in Confluent wire format
-- Broker list defaults to kafka:9092 in container network (overridable via ClickHouse config or environment)
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
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'market.normalized.v1',
         kafka_group_name = 'cg.clickhouse.ticks',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         kafka_handle_error_mode = 'stream',
         kafka_num_consumers = 1;

-- 3. Materialized View routing from Kafka Engine queue into market.ticks with explicit UTC timestamps
-- Filters out poison pills (WHERE _error = '') to guarantee ingestion continuity
CREATE MATERIALIZED VIEW IF NOT EXISTS market.ticks_mv TO market.ticks AS
SELECT
    symbol,
    toDateTime64(fromUnixTimestamp64Micro(event_ts), 6, 'UTC') AS event_ts,
    trade_id,
    price,
    size,
    side,
    toDateTime64(fromUnixTimestamp64Micro(ingest_ts), 6, 'UTC') AS ingest_ts,
    toUUID(event_id) AS event_id
FROM market.ticks_queue
WHERE _error = '';
