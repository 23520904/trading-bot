# 10 Sequences A (real-time)

> Generated from the .drawio file - do not edit by hand.

## Sequence: S1 - Live tick -> dashboard

Participants: Coinbase, market-collector, Kafka, data-validator, market-normalizer, candle-aggregator (Flink), realtime-gateway, web-dashboard

- 1. WSS match / ticker frame  [Coinbase -> market-collector] (sync)
- 2. raw frame [market.raw.v1]  [market-collector -> Kafka] (async)
- 3. poll raw  [Kafka -> data-validator] (async)
- 4. valid [market.validated.v1]  [data-validator -> Kafka] (async)
- 5. poll validated  [Kafka -> market-normalizer] (async)
- 6. MarketTrade [normalized.v1]  [market-normalizer -> Kafka] (async)
- 7. poll normalized (event-time)  [Kafka -> candle-aggregator (Flink)] (async)
- 8. Candle EOS txn [candles.v1]  [candle-aggregator (Flink) -> Kafka] (async)
- 9. poll candles (ephemeral group)  [Kafka -> realtime-gateway] (async)
- 10. WSS update {ch, seq, data}  [realtime-gateway -> web-dashboard] (sync)

## Sequence: S2 - Invalid message -> DLQ -> replay

Participants: data-validator, Kafka, dlq-replayer, MinIO, command-api, Operator (admin)

- 1. poll raw frame  [Kafka -> data-validator] (async)
- 2. INVALID -> market.dlq.v1 + headers  [data-validator -> Kafka] (async)
- 3. poll dead letter  [Kafka -> dlq-replayer] (async)
- 4. PUT cip-dlq/YYYY/MM/DD/{id}  [dlq-replayer -> MinIO] (sync)
- 5. POST /ops/dlq/replay  [Operator (admin) -> command-api] (sync)
- 6. ReplayRequest [ops.commands.v1]  [command-api -> Kafka] (async)
- 7. poll ReplayRequest  [Kafka -> dlq-replayer] (async)
- 8. GET matching payloads  [dlq-replayer -> MinIO] (sync)
- 9. republish [raw.backfill.v1]  [dlq-replayer -> Kafka] (async)
- 10. poll -> now VALID -> validated.v1  [Kafka -> data-validator] (async)

## Sequence: S3 - Signal -> paper order -> position -> alert

Participants: strategy-engine, Kafka, paper-execution, Redis, PostgreSQL, portfolio-service, risk-analytics, realtime-gateway

- 1. poll candles + predictions  [Kafka -> strategy-engine] (async)
- 2. GET feat:{sym}:{tf}  [strategy-engine -> Redis] (sync)
- 3. Signal [signals.v1]  [strategy-engine -> Kafka] (async)
- 4. poll signal  [Kafka -> paper-execution] (async)
- 5. GET snap:last:{sym}  [paper-execution -> Redis] (sync)
- 6. TX: insert order + fill  [paper-execution -> PostgreSQL] (sync)
- 7. orders.v1 + fills.v1  [paper-execution -> Kafka] (async)
- 8. poll fills  [Kafka -> portfolio-service] (async)
- 9. UPSERT trading.positions  [portfolio-service -> PostgreSQL] (sync)
- 10. PositionSnapshot [positions.v1]  [portfolio-service -> Kafka] (async)
- 11. poll positions  [Kafka -> risk-analytics] (async)
- 12. RiskAlert if limit breached  [risk-analytics -> Kafka] (async)
- 13. positions / signals -> WSS  [Kafka -> realtime-gateway] (async)

## Tables, notes and text blocks

Sequence diagrams A - real-time paths
Solid = synchronous - dashed = asynchronous via Kafka - numbers = order - SLO and idempotency notes on the right

S1 - Live tick -> dashboard

SLO (A3)
tick -> dashboard p95 <= 500 ms; tick -> candle p95 <= 1 s.

Replay safety
event_id preserved, ingest_mode=replay; downstream dedupe makes replay idempotent.

S2 - Invalid message -> DLQ -> replay

S3 - Signal -> paper order -> position -> alert

Idempotency
signal_id = hash(strategy_id, symbol, bar_ts); orders.signal_id UNIQUE -> a duplicate signal never creates a 2nd order.
