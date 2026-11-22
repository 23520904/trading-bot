# 07 Event & API Contracts

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Event schemas & API contracts
Authoritative contract = schemas/avro/*.avsc, schemas/openapi/*.yaml, schemas/asyncapi/ws.yaml in the repo; this page is the readable summary

Avro event schemas

| Schema | Fields (name: type) |
|---|---|
| Envelope (all events) | event_id string(UUIDv7); schema_version int; source string; symbol string; event_ts long(timestamp-micros); ingest_ts long; seq long (collector internal monotonic sequence per source+symbol; exchange sequence stored in source_seq) |
| RawFrame | envelope + channel string; payload bytes (original JSON); ingest_mode enum{live, backfill, replay} |
| MarketTrade | envelope + trade_id string (exchange execution dedupe key & gap detection index); price decimal(28,10); size decimal(28,10); side enum{BUY, SELL} |
| Candle | symbol; tf enum{1m,5m,15m,1h,1d}; open_ts; close_ts; o,h,l,c decimal; volume; vwap; trade_count int; buy_volume; is_final bool; updated_at |
| FeatureVector | symbol; tf; ts; feature_set_version string; features map<string,double> |
| Prediction | symbol; ts; model_name; model_version; horizon string; score double; label string? |
| Signal | signal_id (hash); strategy_id; strategy_version; symbol; side enum{LONG,SHORT,FLAT}; strength double[0..1]; bar_ts; reason string |
| OrderEvent / Fill | OrderEvent: order_id; signal_id; account_id; symbol; side; type enum{MARKET,LIMIT}; qty; limit_px?; status enum{NEW,FILLED,REJECTED,CANCELLED}; reject_reason? \| Fill: fill_id; order_id; qty; px; fee; fee_ccy; ts |
| PositionSnapshot | account_id; symbol; qty; avg_px; mark_px; realized_pnl; unrealized_pnl; ts |
| DeadLetter | original_payload bytes; error_code; error_msg; src_topic; src_partition; src_offset; failed_at; stage enum{validator, normalizer} |
| Evolution rules | Add optional fields with defaults only; never rename or remove; add enum symbols at the end with a default; breaking change => new topic .v2 + dual publish. |

REST API (OpenAPI 3.1) - via api-gateway

| Method + path | Service | Scope | Notes |
|---|---|---|---|
| GET /v1/symbols | query-api | market:read | list active symbols |
| GET /v1/candles?symbol&tf&from&to&limit | query-api | market:read | cursor pagination, max 5,000 rows; Cache-Control by tf |
| GET /v1/ticks?symbol&from&to&cursor | query-api | market:read | max 10,000 rows per page |
| GET /v1/features/latest?symbol&tf | query-api | market:read | from Redis feat:* |
| GET /v1/signals \| /v1/orders \| /v1/positions \| /v1/pnl | query-api | trading:read | filters: account_id, strategy_id, from, to |
| GET /v1/backtests/{run_id} \| /v1/jobs/{job_id} | query-api | trading:read | metrics + presigned artifact URL (MinIO) |
| GET /v1/data-quality/incidents | query-api | market:read | status, severity filters |
| POST /v1/backtests | command-api | research:write | 202 {job_id}; header Idempotency-Key |
| POST /v1/research/pairs | command-api | research:write | 202 {job_id} |
| POST /v1/models/{name}/retrain | command-api | research:write | 202 {job_id} |
| PUT /v1/strategies/{id}/enabled | command-api | ops:admin | audited; publishes StrategyToggle |
| POST /v1/ops/dlq/replay | command-api | ops:admin | body {reason, from, to}; audited |

Common REST rules

| Item | Rule |
|---|---|
| Auth | Authorization: Bearer <JWT RS256 from Keycloak>; access token 10 min; scopes market:read, trading:read, research:write, ops:admin. |
| Errors | RFC 7807 application/problem+json {type, title, status, detail, trace_id}. |
| Pagination | cursor-based: ?limit=&cursor= -> {items, next_cursor}. |
| Rate limit | viewer 60 rpm, researcher 120 rpm, admin 300 rpm per sub; 429 + Retry-After. |
| Versioning | path /v1; additive changes only; deprecation header + 6 months. |
| Idempotency | POST requires Idempotency-Key; same key + same body -> same response for 24 h. |

WebSocket protocol (AsyncAPI) - realtime-gateway

| Item | Rule |
|---|---|
| Endpoint | wss://<host>/ws/v1 (Ingress -> realtime-gateway). First client message must be auth within 5 s, otherwise close 4401. |
| Client -> server | auth {token}; subscribe {channels:[{ch, key}], resume_from:{channel: seq}}; unsubscribe {channels}; pong. |
| Server -> client | snapshot {ch, key, seq, data}; update {ch, key, seq, data}; ping; error {code, msg}; resync_required {ch, key}. |
| Channels | candles.{symbol}.{tf}; ticker.{symbol}; signals.{strategy_id}; positions.{account_id}; risk.{account_id}. |
| Delivery | seq is monotonic per (channel, key). On subscribe: snapshot (Redis) then deltas. Gap or unknown resume_from -> resync_required + fresh snapshot. |
| Heartbeat / reconnect | server ping every 15 s, client pong within 10 s else close. Client reconnect backoff 1,2,4..30 s + jitter, then subscribe with resume_from. |
| Backpressure | bounded queue per connection (1,000 msgs); ticker/candle partials drop-oldest; slow consumer > 5 s -> close 4408. |
| Close codes | 4401 unauthorized, 4403 forbidden, 4408 slow consumer, 4429 rate-limited. |
