# 03 L2-B Ingestion & Normalization

> Generated from the .drawio file - do not edit by hand.

## Elements

- Coinbase WebSocket - [External System: feed] - trades, ticker, heartbeat
- market-collector - [Container: Python 3.12, asyncio] - Subscribes WSS, stamps event_id / ingest_ts, publishes raw frames. State: PG collector.*
- schema-registry - [Container: Apicurio, Confluent-compatible] - Avro schemas; BACKWARD_TRANSITIVE; checked in CI.
- market.raw.v1 - key=symbol - 12p - 3d
- data-validator - [Container: Python 3.12, confluent-kafka] - Per-message gate: Avro schema + business rules. Valid -> validated, invalid -> DLQ. Stateless.
- market-normalizer - [Container: Python 3.12] - Canonical model: BTC-USD symbols, Decimal prices, UTC microsecond timestamps. Stateless.
- data-quality - [Container: Python 3.12] - Windowed checks (gaps, staleness, dup-rate, price spikes) -> PG dq.incidents + metrics.
- market.validated.v1 - key=symbol - 12p - 3d
- market.normalized.v1 - key=symbol - 12p - 7d
- Coinbase REST API - [External System: public] - candles, trades (rate-limited)
- historical-ingestor - [Container: Python 3.12, httpx] - Backfills candles/trades; consumes BackfillRequest (ops.commands.v1); cursor in PG.
- market.raw.backfill.v1 - key=symbol - 6p - 7d
- dlq-replayer - [Container: Python 3.12] - Archives dead letters to MinIO; replays on admin command (filter by reason / time).
- market.dlq.v1 - headers: error_code, src_offset - 30d

## Connections (from -> to : label)

- Coinbase WebSocket -> market-collector : WSS frames (sync)
- market-collector -> market.raw.v1 : publish (async/Kafka)
- Coinbase REST API -> historical-ingestor : HTTPS REST (sync)
- historical-ingestor -> market.raw.backfill.v1 : publish (async/Kafka)
- market.raw.v1 -> data-validator : consume (async/Kafka)
- market.raw.backfill.v1 -> data-validator : consume (async/Kafka)
- data-validator -> market.validated.v1 : valid (async/Kafka)
- data-validator -> market.dlq.v1 : INVALID + error headers (async/Kafka)
- market.validated.v1 -> market-normalizer : consume (async/Kafka)
- market-normalizer -> market.normalized.v1 : publish (async/Kafka)
- market.normalized.v1 -> data-quality : consume (async/Kafka)
- market.normalized.v1 -> Continues on page 04 : to page 04 (async/Kafka)
- market.dlq.v1 -> dlq-replayer : consume (async/Kafka)
- dlq-replayer -> market.raw.backfill.v1 : replay (async/Kafka)
- data-validator -> schema-registry : schema check [HTTP] (sync)

## Tables, notes and text blocks

L2-B - Container view: Ingestion & normalization (event-driven)
Arrow = direction of data flow - dashed = via Kafka - topic pills: key / partitions / retention - failure path (DLQ + replay) included

Continues on page 04
consumers of market.normalized.v1

Delivery semantics
At-least-once; idempotent consumers (event_id). Key=symbol -> per-symbol order. DLQ = poison pills only; transient errors retried 3x (200 ms -> 5 s) first.
