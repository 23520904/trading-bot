# 14 Failure Modes & Risks

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Failure modes & risk register
Each failure has detection and mitigation owned by a service spec (pages 15-16)

Failure modes

| Failure | Impact | Detection | Mitigation |
|---|---|---|---|
| Coinbase WS disconnect / sequence gap | missing ticks | gap-detector; feed-liveness alert | auto reconnect with backoff, re-subscribe, BackfillRequest via ops.commands -> REST backfill; dedupe absorbs overlap |
| Duplicates (reconnect, replay, at-least-once) | double counting | dup-rate metric | event_id / trade_id dedupe (Flink 10 min state; ClickHouse ReplacingMergeTree); idempotent consumers |
| Out-of-order / late data | wrong candles | late-events counter | watermark 2 s, allowedLateness 30 s; later -> market.late.v1; Spark candle-backfill corrects (updated_at version) |
| Schema drift / unknown fields | validator rejects | invalid_total by reason | DLQ + Schema Registry compatibility; alert; fix then replay |
| Poison pill / bad deploy | consumer crash loop | restarts, lag | DLQ on deserialization error; Argo Rollouts rollback; 3 retries then DLQ |
| Backpressure / consumer lag | stale data | consumer lag | KEDA scale-out up to partition count; Flink backpressure metrics; gateway drops oldest ticker updates |
| Kafka broker loss | leader failover | under-replicated partitions | RF=3, min.insync=2, rack-aware; idempotent producers retry |
| Flink job / checkpoint failure | candles stop | job status, checkpoint age | auto restart from last checkpoint (30 s interval); EOS sinks; alert if checkpoint age > 2 min |
| ClickHouse outage | queries fail; ingestion lag in Kafka | up, query errors | Kafka keeps 7 d -> catch-up; query-api returns 503 or Redis snapshot; 2 replicas |
| Redis outage | snapshots / features missing | health | strategy-engine pauses (no features -> no signals); WS clients resync after recovery; Flink jobs re-warm keys |
| PostgreSQL failover | brief write errors | CNPG events | retry with backoff; idempotent writes; read-only <= 30 s |
| Clock skew | wrong timestamps | ts-skew histogram | chrony; event_ts from exchange; validator rejects > 5 s in future |
| Model drift / bad model | bad signals | PSI / KS, PnL deviation | alert; move alias champion back; strategy-engine ignores stale predictions |
| Strategy runaway (paper) | exposure beyond limits | risk metrics | risk.alerts.v1 -> paper-execution halts; admin toggle via command-api |
| Coinbase rate limit / outage | slow backfill | HTTP 429 | token bucket 10 rps, honor Retry-After; resumable cursor |

Risk register (likelihood / impact)

| ID | Risk | L / I | Mitigation |
|---|---|---|---|
| R1 | PyFlink maturity / performance | M / H | benchmark in M1; fallback to Java for hot jobs |
| R2 | Ops load: Kafka + Flink + Spark + ClickHouse with small team | H / H | operators, runbooks, staged rollout M0-M6; drop Spark if possible (ADR-01) |
| R3 | Training-serving skew | M / H | shared cip-core spec; parity test offline vs online in CI |
| R4 | Backtest look-ahead / overfitting | H / M | point-in-time data API, walk-forward, embargo; strategy-core forbids future access |
| R5 | Coinbase feed / API changes | M / M | adapter layer + schema versioning; contract tests on recorded fixtures |
| R6 | ReplacingMergeTree dedupe semantics | M / M | argMax / FINAL in views; tests |
| R7 | WebSocket fan-out scale | L / M | load test 1k clients; Redis fan-out tier (ADR-06) |
| R8 | Cost overrun | M / M | A10; scale jobs to zero; spot pool for batch |
