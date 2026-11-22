# 09 L3 Components

> Generated from the .drawio file - do not edit by hand.

## Elements


**Group: candle-aggregator (Flink job) - components**

- kafka-source - market.normalized.v1; event-time; watermark = out-of-orderness 2 s
- dedupe - keyed by trade_id; state TTL 10 min
- window-1m - tumbling 1 m per symbol; allowedLateness 30 s
- rollup-5m..1d - cascades from 1m candles (cip-core spec)
- kafka-sink - EOS txn -> market.candles.v1 (key=symbol|tf)
- late-side-output - beyond lateness -> market.late.v1 + metric
- partial-emitter - 1 s processing-time trigger -> is_final=false
- redis-sink - SET snap:candle:{sym}:{tf}, snap:last:{sym}

**Group: market-collector - components**

- subscription-manager - Shards symbols; K8s Lease leader; reads PG collector.subscriptions
- coinbase-ws-client - websockets; subscribe, heartbeat, reconnect backoff 1-30 s + jitter
- frame-stamper - Adds event_id (UUIDv7), ingest_ts, source_seq
- frame-publisher - confluent-kafka; Avro RawFrame; key=symbol; idempotent, acks=all
- market.raw.v1 - Kafka topic
- health-server - /healthz /readyz /metrics (Prometheus)
- gap-detector - Per-symbol trade_id gaps + heartbeat tail-loss -> BackfillRequest
- checkpoint-store - asyncpg; last_trade_id + last_seq -> PG every 5 s
- ops.commands.v1 - BackfillRequest

**Group: realtime-gateway - components**

- ws-endpoint - Starlette /ws/v1; ping 15 s; auth message within 5 s
- auth-verifier - JWT RS256 via JWKS cache; scopes -> allowed channels
- subscription-registry - conn -> channels; per-channel seq counter
- kafka-fanout - ephemeral consumers (latest, no commit): candles, signals, positions
- metrics - cip_ws_connections, cip_ws_send_seconds, drops_total
- backpressure-guard - bounded queue per conn; drop-oldest (tickers); disconnect slow > 5 s
- snapshot-provider - Redis snap:* on subscribe / resync

**Group: strategy-engine - components**

- event-consumers - candles + predictions (cg.strategy-engine); manual commit
- feature-reader - Redis feat:{sym}:{tf} at bar close
- strategy-runner - on_bar(ctx) -> Signal; plugins from strategy-core
- signal-validator - cooldown, min strength; signal_id = hash(strategy, symbol, bar_ts)
- signal-publisher - Kafka signals.v1 (idempotent)
- pair-config - approved pairs: PG research.pair_candidates
- strategy-registry - PG trading.strategies params; hot-reload via ops.commands.v1
- decision-log - PG trading.signals INSERT ON CONFLICT DO NOTHING

## Connections (from -> to : label)

- subscription-manager -> coinbase-ws-client : symbols (sync)
- coinbase-ws-client -> frame-stamper : frames (sync)
- frame-stamper -> frame-publisher : RawFrame (sync)
- frame-publisher -> market.raw.v1 : publish (async/Kafka)
- coinbase-ws-client -> gap-detector : seq (sync)
- frame-stamper -> checkpoint-store : last_seq (sync)
- gap-detector -> ops.commands.v1 : publish (async/Kafka)
- kafka-source -> dedupe : events (sync)
- dedupe -> window-1m : unique (sync)
- window-1m -> rollup-5m..1d : 1m final (sync)
- rollup-5m..1d -> kafka-sink : candles (sync)
- window-1m -> late-side-output : late (sync)
- window-1m -> partial-emitter : partials (sync)
- partial-emitter -> redis-sink : latest (sync)
- rollup-5m..1d -> redis-sink : final (sync)
- event-consumers -> feature-reader : bars (sync)
- feature-reader -> strategy-runner : features (sync)
- strategy-runner -> signal-validator : signal (sync)
- signal-validator -> signal-publisher : valid (sync)
- strategy-registry -> strategy-runner : params (sync)
- pair-config -> strategy-runner : pairs (sync)
- signal-publisher -> decision-log : record (sync)
- ws-endpoint -> auth-verifier : auth (sync)
- auth-verifier -> subscription-registry : ok (sync)
- kafka-fanout -> subscription-registry : events (async/Kafka)
- subscription-registry -> snapshot-provider : snapshot (sync)
- subscription-registry -> backpressure-guard : frames (sync)
- backpressure-guard -> ws-endpoint : send (sync)

## Tables, notes and text blocks

L3 - Component views of the four most complex containers
Solid = in-process call - dashed = Kafka - each box is a module/class group with a single responsibility - arrow = data/control flow
