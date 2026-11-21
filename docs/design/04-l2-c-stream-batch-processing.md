# 04 L2-C Stream & Batch Processing

> Generated from the .drawio file - do not edit by hand.

## Elements

- Redis 7 - [Datastore: snap:* feat:*]
- candle-aggregator - [Job: Flink 1.19 job, DataStream] - Dedupe -> event-time windows 1m..1d -> OHLCV + VWAP. Single owner of candles (ADR-02).
- feature-engine - [Job: Flink 1.19 job] - Online features: log-returns, rolling vol, RSI, VWAP dev, z-score (cip-core spec).
- market.candles.v1 - key=symbol|tf - 12p - 7d
- features.online.v1 - key=symbol|tf - 12p - 3d
- ClickHouse - [Datastore: ticks, candles, features_log]
- market.normalized.v1 - from page 03
- lake-archiver - [Job: Flink 1.19 job, FileSink] - Normalized trades -> Parquet (zstd), hourly roll; exactly-once via checkpoints.
- MinIO (S3) - [Datastore: cip-lake/*]
- airflow - [Job: Apache Airflow 2, KubernetesExecutor] - Schedules batch DAGs: compaction, backfill, offline features, retrain.
- spark-batch - [Job: Spark 3.5 SparkApplication on K8s] - parquet-compaction, candle-backfill, offline-features (shared cip-core).

## Connections (from -> to : label)

- market.normalized.v1 -> candle-aggregator : consume (async/Kafka)
- market.normalized.v1 -> lake-archiver : consume (async/Kafka)
- market.normalized.v1 -> ClickHouse : Kafka engine: ticks_queue -> ticks_mv (async/Kafka)
- candle-aggregator -> market.candles.v1 : publish (EOS txn) (async/Kafka)
- market.candles.v1 -> feature-engine : consume (async/Kafka)
- feature-engine -> features.online.v1 : publish (EOS) (async/Kafka)
- market.candles.v1 -> ClickHouse : Kafka engine: candles_queue (async/Kafka)
- features.online.v1 -> ClickHouse : Kafka engine: features_log (async/Kafka)
- candle-aggregator -> Redis 7 : SET snap:candle, snap:last (sync)
- feature-engine -> Redis 7 : HSET feat:* (sync)
- lake-archiver -> MinIO (S3) : Parquet [S3] (async/Kafka)
- airflow -> spark-batch : submit [K8s API] (sync)
- spark-batch -> MinIO (S3) : R/W Parquet [S3A] (sync)
- spark-batch -> ClickHouse : bulk load backfill [HTTP] (sync)

## Tables, notes and text blocks

L2-C - Container view: Stream & batch processing and serving stores
Lambda-lite (ADR-01): Flink = only real-time compute; Spark = batch only - shared definitions in cip-core - arrow = data flow

Consumed on page 05
strategy-engine, model-inference, model-monitoring. Candles also by realtime-gateway (page 02).
