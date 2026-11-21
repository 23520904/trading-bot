# 06 Kafka Topics & Conventions

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Kafka topic catalog & conventions
Contract for all producers/consumers - every arrow in pages 03-05 maps to a row here - Schema Registry: Avro, BACKWARD_TRANSITIVE

Topic catalog

| Topic | Key | Partitions | Schema (Avro) | Producer | Consumers (group) | Retention | Notes |
|---|---|---|---|---|---|---|---|
| market.raw.v1 | symbol | 12 | RawFrame | market-collector | data-validator (cg.data-validator) | 3 d | acks=all, idempotent |
| market.raw.backfill.v1 | symbol | 6 | RawFrame | historical-ingestor, dlq-replayer | data-validator (same group) | 7 d | low-priority lane; replay target |
| market.validated.v1 | symbol | 12 | ValidatedEvent | data-validator | market-normalizer (cg.market-normalizer) | 3 d | |
| market.normalized.v1 | symbol | 12 | MarketTrade | market-normalizer | candle-aggregator, lake-archiver, data-quality, ClickHouse ticks_queue | 7 d | dedupe key trade_id |
| market.dlq.v1 | original key | 3 | DeadLetter | data-validator, market-normalizer | dlq-replayer (cg.dlq-replayer) | 30 d | headers: error_code, error_msg, src_topic, src_partition, src_offset, failed_at |
| market.candles.v1 | symbol\|tf | 12 | Candle | candle-aggregator | feature-engine, strategy-engine, realtime-gateway, ClickHouse candles_queue | 7 d | is_final flag; EOS txn |
| market.late.v1 | symbol | 3 | MarketTrade | candle-aggregator | data-quality | 3 d | beyond allowedLateness |
| features.online.v1 | symbol\|tf | 12 | FeatureVector | feature-engine | model-inference, model-monitoring, ClickHouse features_log | 3 d | feature_set_version in value |
| predictions.v1 | symbol | 6 | Prediction | model-inference | strategy-engine, model-monitoring | 7 d | model_name, model_version |
| signals.v1 | strategy_id\|symbol | 6 | Signal | strategy-engine | paper-execution, realtime-gateway | 30 d | signal_id = idempotency key |
| orders.v1 | account\|symbol | 6 | OrderEvent | paper-execution | portfolio-service, realtime-gateway | 30 d | NEW -> FILLED / REJECTED / CANCELLED |
| fills.v1 | account\|symbol | 6 | Fill | paper-execution | portfolio-service | 30 d | fill_id unique |
| positions.v1 | account\|symbol | 6 | PositionSnapshot | portfolio-service | risk-analytics, realtime-gateway | 30 d, compact+delete | latest per key |
| risk.alerts.v1 | account | 3 | RiskAlert | risk-analytics | paper-execution (halt), realtime-gateway | 30 d | severity, rule_id |
| research.jobs.v1 | job_id | 3 | JobRequest | command-api, model-monitoring, airflow | backtesting, pairs-research, model-training (group per job type) | 7 d | KEDA ScaledJob trigger |
| ops.commands.v1 | target | 3 | Command | command-api, market-collector | historical-ingestor, dlq-replayer, strategy-engine | 7 d | BackfillRequest, ReplayRequest, StrategyToggle |

Conventions

| Item | Rule |
|---|---|
| Naming | topic = <domain>.<entity>.v<N>; consumer group = cg.<service>[.<variant>]; one shared DLQ market.dlq.v1. |
| Producer | acks=all, enable.idempotence=true, compression=zstd, linger.ms=5, max.in.flight=5. |
| Consumer | enable.auto.commit=false; commit after sink success; isolation.level=read_committed for Flink-written topics; partition.assignment=cooperative-sticky; max.poll.interval=5 min. |
| Partitioning | key = symbol (per-symbol order). 12 partitions on hot topics (>= max consumers, x3 headroom for 100 symbols). Increase only with a planned re-key. |
| Replication | RF=3, min.insync.replicas=2, unclean.leader.election=false, rack-aware. |
| Headers | traceparent, schema_version, event_id, source, ingest_ts. |
| Wire format | Confluent wire format (magic byte 0x00 + 4-byte schema ID) via cip-kafka; subject = <topic>-value; schemas registered by CI (ADR-13). |
| Timestamps | event_ts = exchange time (UTC, microseconds); ingest_ts = collector time; Kafka record timestamp = event_ts (CreateTime). |
| Late data | Flink watermark = max out-of-orderness 2 s; allowedLateness 30 s; later -> market.late.v1 and corrected by Spark candle-backfill. |
| Failure policy | Deserialization / validation failure -> DLQ immediately; transient error -> retry 3x (200 ms -> 5 s) then DLQ. No infinite retry. |
