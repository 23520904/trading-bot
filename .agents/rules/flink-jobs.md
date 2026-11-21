---
trigger: glob
globs: "jobs/flink/**"
description: "Flink job rules: event time, state, exactly-once, parity with batch"
---
# Flink job rules
- Event time only. Watermark = max out-of-orderness 2 s; allowedLateness 30 s; later events go to `market.late.v1` (side output) and increment a metric.
- Dedupe by `trade_id` with keyed state TTL 10 min before windowing. State backend RocksDB; checkpoint every 30 s to `s3://cip-flink/checkpoints/<job>/`; retain last 3.
- Kafka -> Kafka uses exactly-once transactions (`read_committed` consumers downstream). Redis / ClickHouse sinks must be idempotent (SET/HSET, ReplacingMergeTree).
- Candle and feature definitions come from `libs/cip-core`; never re-implement them in the job. Partial candles are emitted with `is_final=false`; only final candles go to ClickHouse.
- Every job has a MiniCluster test with out-of-order, duplicate and late fixtures, and a parity test against the Spark batch implementation (tolerance documented).
- Parallelism and partitions follow page 12/06; changing the partition count requires a planned re-key (ADR).
