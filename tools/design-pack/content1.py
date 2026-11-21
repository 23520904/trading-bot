from lib import *

PAGES = [
    ('00', 'Cover, assumptions, decisions (ADR)', 'Everyone - read first'),
    ('01', 'L1 System Context', 'Executives, new joiners'),
    ('02', 'L2-A Access & request path', 'Backend / frontend / security'),
    ('03', 'L2-B Ingestion & normalization', 'Data engineers'),
    ('04', 'L2-C Stream & batch processing, stores', 'Data engineers'),
    ('05', 'L2-D Quant, ML & paper trading', 'Quant / ML / trading engineers'),
    ('06', 'Kafka topic catalog & conventions', 'All service owners (contract)'),
    ('07', 'Event schemas & API contracts (REST, WebSocket)', 'All service owners (contract)'),
    ('08', 'Data architecture: DDL, tables, buckets, keys, ownership', 'Backend / data engineers'),
    ('09', 'L3 Component views', 'Implementers of the 4 key containers'),
    ('10', 'Sequence diagrams A (real-time)', 'Implementers, testers'),
    ('11', 'Sequence diagrams B (research, ML)', 'Implementers, testers'),
    ('12', 'Deployment (Kubernetes), environments, CI/CD', 'DevOps / SRE'),
    ('13', 'Observability, security, SLO, NFR', 'SRE / security'),
    ('14', 'Failure modes & risk register', 'Everyone'),
    ('15-16', 'Service spec cards (26 containers)', 'Implementers - definition of done'),
    ('17', 'Repo layout & roadmap (M0-M6)', 'Tech lead, PM'),
]

ASSUME = [
    ('A1', 'Single Kubernetes cluster, 1 region / 3 zones, cloud-agnostic (MinIO = S3 API).'),
    ('A2', 'Up to 100 spot symbols, <= 5,000 msg/s sustained (about 50 msg/s per symbol).'),
    ('A3', 'Latency targets: tick -> dashboard p95 <= 500 ms; tick -> signal p95 <= 1 s.'),
    ('A4', 'Team of 3-6; Python 3.12 default; Flink via PyFlink (Java fallback for hot paths).'),
    ('A5', 'Coinbase public market-data channels only; no trading permission, no trading keys exist.'),
    ('A6', 'Keycloak self-hosted; <= 50 users.'),
    ('A7', 'Retention: Kafka raw 3 d, normalized 7 d; lake 365 d; ticks in ClickHouse 90 d hot; candles forever.'),
    ('A8', 'Observability = Grafana LGTM (Prometheus, Loki, Tempo, Grafana) + OpenTelemetry.'),
    ('A9', 'ADDED vs draft: Keycloak, Schema Registry, dlq-replayer, query-api, command-api, realtime-gateway, lake-archiver, Airflow, MLflow, Tempo, Alertmanager, Argo CD.'),
    ('A10', 'REMOVED vs draft: candle-service, feature-service (-> Flink jobs), "Producers" box, Kubernetes as a data-flow node (-> deployment view). Budget unknown: cost = TBD.'),
]

ADR = [
    ('ADR-01', 'Lambda-lite. Kafka = system of record for streams. Flink = ONLY real-time compute (candles, features, archiving). Spark = batch ONLY (compaction, backfill, offline features).',
     'Full Lambda with duplicated logic; Kappa with Flink only.',
     'Two engines cost ops effort. Mitigation: candle/feature definitions live once in cip-core. Revisit at M3: with <= 100 symbols replace Spark by Flink batch / Polars.'),
    ('ADR-02', 'Remove standalone candle-service and feature-service; Flink jobs candle-aggregator and feature-engine are the single owners.',
     'Keep Python services next to Flink.', 'Removes the duplicated responsibility in the draft; one real-time code path; batch recompute uses the same spec.'),
    ('ADR-03', 'data-validator = per-message gate (schema + rules -> DLQ). data-quality = windowed monitoring (gaps, staleness, volume anomalies) -> incidents + metrics.',
     'Merge into one service.', 'Different state and latency profile; validator stays stateless and scales with KEDA.'),
    ('ADR-04', 'Data ownership: schema-per-service in PostgreSQL with separate roles; ClickHouse market.* written only by Kafka-engine MVs and spark-batch; MinIO prefix, Redis key prefix per writer.',
     'Shared DB with free access (draft).', 'No hidden coupling. Cross-owner reads use read-only roles or owner API (matrix on page 08).'),
    ('ADR-05', 'At-least-once everywhere + idempotent consumers (event_id, trade_id). Flink -> Kafka uses exactly-once transactions. Key = symbol => per-symbol order. ClickHouse dedupe via ReplacingMergeTree.',
     'End-to-end exactly-once.', 'Simpler and robust; duplicates are expected and handled. Cost: dedupe state in Flink (10 min TTL).'),
    ('ADR-06', 'Live fan-out: realtime-gateway consumes Kafka directly (ephemeral groups, latest, no commit) + Redis snapshot on subscribe; sequence number per channel.',
     'Redis Pub/Sub (no replay / seq); separate bridge service.', 'Fewer hops, lower latency. Revisit at > 1,000 concurrent clients: add a Redis fan-out tier.'),
    ('ADR-07', 'api-gateway = authN/Z, routing, rate-limit only. CQRS: query-api (reads) / command-api (writes). WebSocket bypasses api-gateway.',
     'Gateway reading databases (draft).', 'Removes layer bypass; services scale independently; long-lived WS do not pin the gateway.'),
    ('ADR-08', 'Backtest/live parity: strategies implemented once in strategy-core; execution simulator (fees, slippage, latency) shared by paper-execution and backtesting.',
     'Separate research and live code.', 'Avoids research-to-production drift; strategy-core version recorded with every run.'),
    ('ADR-09', 'MLflow (tracking + registry; backend PG, artifacts MinIO). Promotion by alias champion; model-inference polls alias (60 s) and hot-swaps; model-monitoring uses PSI / KS vs training baseline.',
     'Custom registry.', 'Standard tooling; rollback = move alias.'),
    ('ADR-10', 'Paper-trading boundary: ExecutionPort interface implemented only by paper-execution. Live trading would add live-execution in a separate trust zone behind pre-trade risk gate + 2-person approval.',
     'Build live trading now.', 'Safe scope; no exchange trading credentials in the platform.'),
    ('ADR-11', 'Avro + Schema Registry, compatibility BACKWARD_TRANSITIVE; topics versioned .vN; breaking change = new topic + dual-publish for >= 1 retention period.',
     'JSON without schema.', 'Contract tests in CI block incompatible changes.'),
    ('ADR-12', 'Keycloak OIDC (JWT RS256), scopes market:read, trading:read, research:write, ops:admin; service-to-service mTLS (Linkerd); Kafka mTLS + ACL per service principal.',
     'Static API keys.', 'Central identity, short-lived tokens, least privilege.'),
    ('ADR-13', 'Wire format: every Avro topic uses Confluent wire format (magic byte 0x00 + 4-byte schema ID). One shared serializer/deserializer in cip-kafka; subject = <topic>-value; schemas registered by CI (auto.register.schemas=false outside local dev). ClickHouse AvroConfluent and Flink read the same format.',
     'Raw schemaless Avro bytes without wire header.', 'Allows ClickHouse Kafka-engine AvroConfluent and Flink to decode seamlessly with Schema Registry schema ID caching.'),
    ('ADR-14', 'Source feed: Coinbase Exchange public WSS (wss://ws-feed.exchange.coinbase.com; checked 2026-11-22 via https://docs.cdp.coinbase.com/exchange/docs/websocket-channels/#matches-channel). Trade gap detection uses exchange trade_id (strictly contiguous N, N+1; verified 10-min live capture: 4,599 trades, 0 gaps, 100% contiguous); sequence is exchange-wide orderbook sequence with expected gaps on matches channel (verified: 4,598 gaps, 0% contiguous, max gap 1,267); source_seq records exchange sequence for L3 correlation.',
     'Use sequence for trade gap detection; poll REST ticker.', 'Aligns gap detection with true trade continuity (trade_id contiguous 100%) while preserving orderbook sequence for cross-channel audit.'),
    ('ADR-15', 'Feed liveness and reconnect: staleness is measured per symbol on any frame including heartbeat; a symbol is stale after 5 s without a frame; metric and gap event are emitted and do NOT fail pod-level /readyz. Pod /readyz = WS connected + Kafka producer reachable. Reconnect sleep = min(30, backoff * uniform(0.5, 1.5)) starting at 1 s.',
     'Unbounded backoff without jitter; failing /readyz on single symbol staleness.', 'Guarantees fast failover without cascading pod restarts while alerting on isolated symbol feed drops.'),
]


def p_cover(p):
    p.vertex(30, 20, 1900, 90, '<b style="font-size:26px">Crypto Intelligence Platform (CIP) - Architecture Design Pack</b><br>'
             '<span style="font-size:12px;color:#555555">v2.0 - 21 Nov 2026 - Scope: real-time market-data pipeline, analytics, ML, quant research and PAPER trading (no live order routing) - '
             'Method: C4 model (L1-L3) + data-flow, data, API, sequence and deployment views - one level per page, one flow direction per diagram</span>',
             'text;html=1;align=left;verticalAlign=top;whiteSpace=wrap;', track=False)
    legend(p, 30, 130, 'Pipeline pages: arrow = data flow. Access pages: arrow = caller -> callee.')
    note(p, 390, 130, 520, 190, '<b>How to read / build</b><br>Engineers: 06 -> 07 -> 08 -> 15-16 -> 17 (contracts first, then data, then spec cards).<br>'
         'Reviewers: 01 -> 02..05 -> 12 -> 14.<br>Every arrow in pages 02-05 maps to a topic (06) or an API (07); every container has a spec card (15-16) and appears in a sequence (10-11).<br>'
         'Colours (max 5): blue app, orange processing engine, purple Kafka, green datastore, grey external.')
    y1 = tbl(p, 30, 350, 880, 'Page index', ['Page', 'Content', 'Primary reader'], PAGES, [1, 5, 3])
    y2 = tbl(p, 950, 130, 980, 'Assumptions (verify, ask if wrong)', ['ID', 'Assumption / change vs draft'], ASSUME, [1, 12])
    tbl(p, 30, max(y1, y2) + 10, 1900, 'Design decisions (ADR-lite)', ['ID', 'Decision', 'Alternatives', 'Trade-off / consequence'], ADR, [1, 8, 3, 7])


TOPICS = [
    ('market.raw.v1', 'symbol', '12', 'RawFrame', 'market-collector', 'data-validator (cg.data-validator)', '3 d', 'acks=all, idempotent'),
    ('market.raw.backfill.v1', 'symbol', '6', 'RawFrame', 'historical-ingestor, dlq-replayer', 'data-validator (same group)', '7 d', 'low-priority lane; replay target'),
    ('market.validated.v1', 'symbol', '12', 'ValidatedEvent', 'data-validator', 'market-normalizer (cg.market-normalizer)', '3 d', ''),
    ('market.normalized.v1', 'symbol', '12', 'MarketTrade', 'market-normalizer', 'candle-aggregator, lake-archiver, data-quality, ClickHouse ticks_queue', '7 d', 'dedupe key trade_id'),
    ('market.dlq.v1', 'original key', '3', 'DeadLetter', 'data-validator, market-normalizer', 'dlq-replayer (cg.dlq-replayer)', '30 d', 'headers: error_code, error_msg, src_topic, src_partition, src_offset, failed_at'),
    ('market.candles.v1', 'symbol|tf', '12', 'Candle', 'candle-aggregator', 'feature-engine, strategy-engine, realtime-gateway, ClickHouse candles_queue', '7 d', 'is_final flag; EOS txn'),
    ('market.late.v1', 'symbol', '3', 'MarketTrade', 'candle-aggregator', 'data-quality', '3 d', 'beyond allowedLateness'),
    ('features.online.v1', 'symbol|tf', '12', 'FeatureVector', 'feature-engine', 'model-inference, model-monitoring, ClickHouse features_log', '3 d', 'feature_set_version in value'),
    ('predictions.v1', 'symbol', '6', 'Prediction', 'model-inference', 'strategy-engine, model-monitoring', '7 d', 'model_name, model_version'),
    ('signals.v1', 'strategy_id|symbol', '6', 'Signal', 'strategy-engine', 'paper-execution, realtime-gateway', '30 d', 'signal_id = idempotency key'),
    ('orders.v1', 'account|symbol', '6', 'OrderEvent', 'paper-execution', 'portfolio-service, realtime-gateway', '30 d', 'NEW -> FILLED / REJECTED / CANCELLED'),
    ('fills.v1', 'account|symbol', '6', 'Fill', 'paper-execution', 'portfolio-service', '30 d', 'fill_id unique'),
    ('positions.v1', 'account|symbol', '6', 'PositionSnapshot', 'portfolio-service', 'risk-analytics, realtime-gateway', '30 d, compact+delete', 'latest per key'),
    ('risk.alerts.v1', 'account', '3', 'RiskAlert', 'risk-analytics', 'paper-execution (halt), realtime-gateway', '30 d', 'severity, rule_id'),
    ('research.jobs.v1', 'job_id', '3', 'JobRequest', 'command-api, model-monitoring, airflow', 'backtesting, pairs-research, model-training (group per job type)', '7 d', 'KEDA ScaledJob trigger'),
    ('ops.commands.v1', 'target', '3', 'Command', 'command-api, market-collector', 'historical-ingestor, dlq-replayer, strategy-engine', '7 d', 'BackfillRequest, ReplayRequest, StrategyToggle'),
]
CONV = [
    ('Naming', 'topic = <domain>.<entity>.v<N>; consumer group = cg.<service>[.<variant>]; one shared DLQ market.dlq.v1.'),
    ('Producer', 'acks=all, enable.idempotence=true, compression=zstd, linger.ms=5, max.in.flight=5.'),
    ('Consumer', 'enable.auto.commit=false; commit after sink success; isolation.level=read_committed for Flink-written topics; partition.assignment=cooperative-sticky; max.poll.interval=5 min.'),
    ('Partitioning', 'key = symbol (per-symbol order). 12 partitions on hot topics (>= max consumers, x3 headroom for 100 symbols). Increase only with a planned re-key.'),
    ('Replication', 'RF=3, min.insync.replicas=2, unclean.leader.election=false, rack-aware.'),
    ('Headers', 'traceparent, schema_version, event_id, source, ingest_ts.'),
    ('Wire format', 'Confluent wire format (magic byte 0x00 + 4-byte schema ID) via cip-kafka; subject = <topic>-value; schemas registered by CI (ADR-13).'),
    ('Timestamps', 'event_ts = exchange time (UTC, microseconds); ingest_ts = collector time; Kafka record timestamp = event_ts (CreateTime).'),
    ('Late data', 'Flink watermark = max out-of-orderness 2 s; allowedLateness 30 s; later -> market.late.v1 and corrected by Spark candle-backfill.'),
    ('Failure policy', 'Deserialization / validation failure -> DLQ immediately; transient error -> retry 3x (200 ms -> 5 s) then DLQ. No infinite retry.'),
]


def p_topics(p):
    title(p, 'Kafka topic catalog & conventions', 'Contract for all producers/consumers - every arrow in pages 03-05 maps to a row here - Schema Registry: Avro, BACKWARD_TRANSITIVE')
    y = tbl(p, 30, 90, 2000, 'Topic catalog', ['Topic', 'Key', 'Partitions', 'Schema (Avro)', 'Producer', 'Consumers (group)', 'Retention', 'Notes'],
            TOPICS, [3, 2, 1.2, 2, 3, 5, 1.5, 4])
    tbl(p, 30, y, 2000, 'Conventions', ['Item', 'Rule'], CONV, [1, 10])


SCHEMAS = [
    ('Envelope (all events)', 'event_id string(UUIDv7); schema_version int; source string; symbol string; event_ts long(timestamp-micros); ingest_ts long; seq long (per source+symbol)'),
    ('RawFrame', 'envelope + channel string; payload bytes (original JSON); ingest_mode enum{live, backfill, replay}'),
    ('MarketTrade', 'envelope + trade_id string; price decimal(28,10); size decimal(28,10); side enum{BUY, SELL}'),
    ('Candle', 'symbol; tf enum{1m,5m,15m,1h,1d}; open_ts; close_ts; o,h,l,c decimal; volume; vwap; trade_count int; buy_volume; is_final bool; updated_at'),
    ('FeatureVector', 'symbol; tf; ts; feature_set_version string; features map<string,double>'),
    ('Prediction', 'symbol; ts; model_name; model_version; horizon string; score double; label string?'),
    ('Signal', 'signal_id (hash); strategy_id; strategy_version; symbol; side enum{LONG,SHORT,FLAT}; strength double[0..1]; bar_ts; reason string'),
    ('OrderEvent / Fill', 'OrderEvent: order_id; signal_id; account_id; symbol; side; type enum{MARKET,LIMIT}; qty; limit_px?; status enum{NEW,FILLED,REJECTED,CANCELLED}; reject_reason?  |  Fill: fill_id; order_id; qty; px; fee; fee_ccy; ts'),
    ('PositionSnapshot', 'account_id; symbol; qty; avg_px; mark_px; realized_pnl; unrealized_pnl; ts'),
    ('DeadLetter', 'original_payload bytes; error_code; error_msg; src_topic; src_partition; src_offset; failed_at; stage enum{validator, normalizer}'),
    ('Evolution rules', 'Add optional fields with defaults only; never rename or remove; add enum symbols at the end with a default; breaking change => new topic .v2 + dual publish.'),
]
REST = [
    ('GET /v1/symbols', 'query-api', 'market:read', 'list active symbols'),
    ('GET /v1/candles?symbol&tf&from&to&limit', 'query-api', 'market:read', 'cursor pagination, max 5,000 rows; Cache-Control by tf'),
    ('GET /v1/ticks?symbol&from&to&cursor', 'query-api', 'market:read', 'max 10,000 rows per page'),
    ('GET /v1/features/latest?symbol&tf', 'query-api', 'market:read', 'from Redis feat:*'),
    ('GET /v1/signals | /v1/orders | /v1/positions | /v1/pnl', 'query-api', 'trading:read', 'filters: account_id, strategy_id, from, to'),
    ('GET /v1/backtests/{run_id} | /v1/jobs/{job_id}', 'query-api', 'trading:read', 'metrics + presigned artifact URL (MinIO)'),
    ('GET /v1/data-quality/incidents', 'query-api', 'market:read', 'status, severity filters'),
    ('POST /v1/backtests', 'command-api', 'research:write', '202 {job_id}; header Idempotency-Key'),
    ('POST /v1/research/pairs', 'command-api', 'research:write', '202 {job_id}'),
    ('POST /v1/models/{name}/retrain', 'command-api', 'research:write', '202 {job_id}'),
    ('PUT /v1/strategies/{id}/enabled', 'command-api', 'ops:admin', 'audited; publishes StrategyToggle'),
    ('POST /v1/ops/dlq/replay', 'command-api', 'ops:admin', 'body {reason, from, to}; audited'),
]
COMMON = [
    ('Auth', 'Authorization: Bearer <JWT RS256 from Keycloak>; access token 10 min; scopes market:read, trading:read, research:write, ops:admin.'),
    ('Errors', 'RFC 7807 application/problem+json {type, title, status, detail, trace_id}.'),
    ('Pagination', 'cursor-based: ?limit=&cursor= -> {items, next_cursor}.'),
    ('Rate limit', 'viewer 60 rpm, researcher 120 rpm, admin 300 rpm per sub; 429 + Retry-After.'),
    ('Versioning', 'path /v1; additive changes only; deprecation header + 6 months.'),
    ('Idempotency', 'POST requires Idempotency-Key; same key + same body -> same response for 24 h.'),
]
WS = [
    ('Endpoint', 'wss://<host>/ws/v1 (Ingress -> realtime-gateway). First client message must be auth within 5 s, otherwise close 4401.'),
    ('Client -> server', 'auth {token}; subscribe {channels:[{ch, key}], resume_from:{channel: seq}}; unsubscribe {channels}; pong.'),
    ('Server -> client', 'snapshot {ch, key, seq, data}; update {ch, key, seq, data}; ping; error {code, msg}; resync_required {ch, key}.'),
    ('Channels', 'candles.{symbol}.{tf}; ticker.{symbol}; signals.{strategy_id}; positions.{account_id}; risk.{account_id}.'),
    ('Delivery', 'seq is monotonic per (channel, key). On subscribe: snapshot (Redis) then deltas. Gap or unknown resume_from -> resync_required + fresh snapshot.'),
    ('Heartbeat / reconnect', 'server ping every 15 s, client pong within 10 s else close. Client reconnect backoff 1,2,4..30 s + jitter, then subscribe with resume_from.'),
    ('Backpressure', 'bounded queue per connection (1,000 msgs); ticker/candle partials drop-oldest; slow consumer > 5 s -> close 4408.'),
    ('Close codes', '4401 unauthorized, 4403 forbidden, 4408 slow consumer, 4429 rate-limited.'),
]


def p_contracts(p):
    title(p, 'Event schemas & API contracts', 'Authoritative contract = schemas/avro/*.avsc, schemas/openapi/*.yaml, schemas/asyncapi/ws.yaml in the repo; this page is the readable summary')
    y = tbl(p, 30, 90, 1900, 'Avro event schemas', ['Schema', 'Fields (name: type)'], SCHEMAS, [2, 12])
    y2 = tbl(p, 30, y, 1900, 'REST API (OpenAPI 3.1) - via api-gateway', ['Method + path', 'Service', 'Scope', 'Notes'], REST, [6, 2, 2, 5])
    y3 = tbl(p, 30, y2, 1900, 'Common REST rules', ['Item', 'Rule'], COMMON, [1, 10])
    tbl(p, 30, y3, 1900, 'WebSocket protocol (AsyncAPI) - realtime-gateway', ['Item', 'Rule'], WS, [2, 10])


PG_DDL = """-- schema collector (owner: market-collector, historical-ingestor)
CREATE TABLE collector.subscriptions(shard_id int, symbol text, channel text, status text, last_seq bigint, updated_at timestamptz DEFAULT now(), PRIMARY KEY(shard_id, symbol, channel));
CREATE TABLE collector.backfill_cursors(symbol text, tf text, cursor_ts timestamptz, status text, PRIMARY KEY(symbol, tf));
-- schema dq (owner: data-quality)
CREATE TABLE dq.incidents(id uuid PRIMARY KEY, symbol text, check_name text, severity text, window_start timestamptz, window_end timestamptz, details jsonb, status text DEFAULT 'open', created_at timestamptz DEFAULT now());
CREATE INDEX ON dq.incidents(symbol, created_at DESC);
-- schema trading (owners: strategy-engine, paper-execution, portfolio-service)
CREATE TABLE trading.strategies(strategy_id text PRIMARY KEY, version text, params jsonb, enabled bool DEFAULT false, updated_at timestamptz);
CREATE TABLE trading.signals(signal_id text PRIMARY KEY, strategy_id text REFERENCES trading.strategies, symbol text, side text, strength real, bar_ts timestamptz, reason text, created_at timestamptz DEFAULT now());
CREATE TABLE trading.orders(order_id uuid PRIMARY KEY, signal_id text UNIQUE REFERENCES trading.signals, account_id text, symbol text, side text, type text, qty numeric(28,10), limit_px numeric(28,10), status text, reject_reason text, created_at timestamptz, updated_at timestamptz);
CREATE TABLE trading.fills(fill_id uuid PRIMARY KEY, order_id uuid REFERENCES trading.orders, qty numeric(28,10), px numeric(28,10), fee numeric(28,10), fee_ccy text, ts timestamptz);
CREATE TABLE trading.positions(account_id text, symbol text, qty numeric(28,10), avg_px numeric(28,10), realized_pnl numeric(28,10), unrealized_pnl numeric(28,10), updated_at timestamptz, PRIMARY KEY(account_id, symbol));
CREATE TABLE trading.pnl_daily(account_id text, day date, realized numeric(28,10), unrealized numeric(28,10), fees numeric(28,10), PRIMARY KEY(account_id, day));
-- schema research (owners: command-api [jobs], backtesting, pairs-research)
CREATE TABLE research.jobs(job_id uuid PRIMARY KEY, type text, params jsonb, status text, requested_by text, created_at timestamptz, started_at timestamptz, finished_at timestamptz, artifact_uri text);
CREATE TABLE research.backtest_runs(run_id uuid PRIMARY KEY, job_id uuid REFERENCES research.jobs, strategy_id text, strategy_version text, start_ts timestamptz, end_ts timestamptz, metrics jsonb, artifact_uri text);
CREATE TABLE research.pair_candidates(pair_id uuid PRIMARY KEY, sym_a text, sym_b text, method text, p_value real, hedge_ratio numeric, half_life_h real, as_of timestamptz, approved bool DEFAULT false);
-- schema risk (owner: risk-analytics)
CREATE TABLE risk.limits(account_id text, rule_id text, metric text, threshold numeric, action text, PRIMARY KEY(account_id, rule_id));
CREATE TABLE risk.snapshots(account_id text, ts timestamptz, gross_exposure numeric, net_exposure numeric, var_95 numeric, max_drawdown numeric, PRIMARY KEY(account_id, ts));
-- schema audit (owner: command-api): audit.events(id uuid PK, actor text, action text, target text, payload jsonb, ts timestamptz)
-- separate databases (owned by the tool): keycloak, mlflow, airflow"""
CH_DDL = """CREATE TABLE market.ticks (symbol LowCardinality(String), event_ts DateTime64(6,'UTC'), trade_id String, price Decimal(28,10), size Decimal(28,10), side Enum8('BUY'=1,'SELL'=2), ingest_ts DateTime64(6,'UTC'), event_id UUID)
ENGINE = ReplacingMergeTree(ingest_ts) PARTITION BY toYYYYMM(event_ts) ORDER BY (symbol, event_ts, trade_id) TTL toDateTime(event_ts) + INTERVAL 90 DAY TO VOLUME 'cold';
CREATE TABLE market.ticks_queue (...same columns...) ENGINE = Kafka SETTINGS kafka_broker_list='kafka:9093', kafka_topic_list='market.normalized.v1', kafka_group_name='cg.clickhouse.ticks', kafka_format='AvroConfluent', format_avro_schema_registry_url='http://schema-registry:8081', kafka_num_consumers=4;
CREATE MATERIALIZED VIEW market.ticks_mv TO market.ticks AS SELECT * FROM market.ticks_queue;
CREATE TABLE market.candles (symbol LowCardinality(String), tf Enum8('1m'=1,'5m'=2,'15m'=3,'1h'=4,'1d'=5), open_ts DateTime64(3,'UTC'), o Decimal(28,10), h Decimal(28,10), l Decimal(28,10), c Decimal(28,10), volume Decimal(28,10), vwap Decimal(28,10), trade_count UInt32, updated_at DateTime64(6,'UTC'))
ENGINE = ReplacingMergeTree(updated_at) PARTITION BY toYYYYMM(open_ts) ORDER BY (symbol, tf, open_ts);
-- candles_queue (Kafka engine on market.candles.v1) + candles_mv: SELECT ... WHERE is_final = 1.  Reads: use argMax(.., updated_at) or FINAL.
CREATE TABLE market.features_log (symbol LowCardinality(String), tf LowCardinality(String), ts DateTime64(3,'UTC'), feature_set_version LowCardinality(String), features Map(String, Float64))
ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (symbol, tf, ts) TTL toDateTime(ts) + INTERVAL 30 DAY;"""
MINIO = [
    ('cip-lake/normalized/trades/date=YYYY-MM-DD/hour=HH/symbol=X/', 'lake-archiver', 'Parquet zstd; compacted to >= 128 MB', '365 d then infrequent tier'),
    ('cip-lake/curated/candles/tf=1m/date=.../', 'spark-batch', 'Parquet', 'forever'),
    ('cip-lake/features/offline/set=vN/date=.../', 'spark-batch', 'Parquet (point-in-time correct)', '365 d'),
    ('cip-dlq/YYYY/MM/DD/{event_id}.avro', 'dlq-replayer', 'Avro + JSON sidecar (error metadata)', '90 d'),
    ('cip-research/backtests/{run_id}/', 'backtesting', 'equity.parquet, trades.parquet, report.html', '180 d'),
    ('cip-ml/mlflow-artifacts/', 'MLflow (clients: model-training)', 'MLflow layout', 'registered: keep; unregistered 90 d'),
    ('cip-flink/checkpoints/{job}/', 'Flink', 'RocksDB incremental, retain last 3', 'auto'),
    ('cip-backup/{pg-wal, loki, tempo}', 'PostgreSQL, Loki, Tempo', 'native formats', '14-30 d'),
]
REDIS = [
    ('snap:candle:{symbol}:{tf}', 'STRING (JSON)', 'candle-aggregator', 'realtime-gateway, query-api, portfolio-service', '24 h'),
    ('snap:last:{symbol}', 'HASH (px, size, ts)', 'candle-aggregator', 'paper-execution', '1 h'),
    ('feat:{symbol}:{tf}', 'HASH (feature -> value, ts, set_version)', 'feature-engine', 'strategy-engine, model-inference, query-api', '10 min'),
    ('ratelimit:{sub}:{route}', 'STRING counter', 'api-gateway', 'api-gateway', '60 s'),
]
MATRIX = [
    ('market-collector', 'W collector.subscriptions', '-', '-', '-'), ('historical-ingestor', 'RW collector.backfill_cursors', '-', '-', '-'),
    ('data-validator / market-normalizer', '-', '-', '-', '-'), ('dlq-replayer', '-', '-', '-', 'W cip-dlq'),
    ('data-quality', 'W dq.*', 'R ticks', '-', '-'), ('candle-aggregator', '-', '-', 'W snap:*', '-'), ('feature-engine', '-', '-', 'W feat:*', '-'),
    ('lake-archiver', '-', '-', '-', 'W cip-lake/normalized'), ('spark-batch', '-', 'W candles (backfill)', '-', 'RW cip-lake/*'),
    ('strategy-engine', 'RW trading.strategies, signals; R research.pair_candidates', '-', 'R feat:*', '-'),
    ('paper-execution', 'RW trading.orders, fills; R risk.limits', '-', 'R snap:last', '-'),
    ('portfolio-service', 'RW trading.positions, pnl_daily', '-', 'R snap:candle', '-'), ('risk-analytics', 'W risk.*; R trading.positions', '-', '-', '-'),
    ('model-inference', '-', '-', 'R feat:*', 'R cip-ml (via MLflow)'), ('model-training', '-', 'R features_log', '-', 'R cip-lake/features; W cip-ml'),
    ('model-monitoring', '-', 'R features_log', '-', 'R cip-ml baseline'), ('mlflow', 'own DB mlflow', '-', '-', 'W cip-ml'),
    ('pairs-research', 'W research.pair_candidates', 'R candles', '-', '-'), ('backtesting', 'W research.backtest_runs; U research.jobs', 'R candles, ticks', '-', 'R cip-lake; W cip-research'),
    ('query-api', 'R trading.*, research.*, dq.*', 'R candles, ticks, features_log', 'R snap:*, feat:*', 'presign cip-research'),
    ('command-api', 'W research.jobs, trading.strategies.enabled, audit.events', '-', '-', '-'), ('realtime-gateway', '-', '-', 'R snap:*', '-'),
    ('api-gateway', '-', '-', 'RW ratelimit:*', '-'),
]


def p_data(p):
    title(p, 'Data architecture - per-store design and ownership', 'R = read, W = write, U = update - every writer is unique per object (ADR-04) - DDL is the starting point for migrations (alembic / clickhouse-migrate)')
    y = code(p, 30, 90, 1900, 'PostgreSQL 16 - database cip (schema per owner, one DB role per service)', PG_DDL, fs=10)
    y = code(p, 30, y, 1900, 'ClickHouse - database market (writers: Kafka-engine MVs + spark-batch only)', CH_DDL, fs=10)
    y = tbl(p, 30, y, 1900, 'MinIO buckets (S3 API)', ['Bucket / prefix', 'Writer', 'Format / partitioning', 'Lifecycle'], MINIO, [5, 2, 3, 2])
    y = tbl(p, 30, y, 1900, 'Redis keyspace (cache semantics: rebuildable from Kafka / ClickHouse; maxmemory-policy volatile-ttl; no persistence required)',
            ['Key', 'Type', 'Writer', 'Readers', 'TTL'], REDIS, [3, 3, 2, 4, 1])
    tbl(p, 30, y, 1900, 'Data ownership matrix (service x store)', ['Service', 'PostgreSQL', 'ClickHouse', 'Redis', 'MinIO'], MATRIX, [3, 5, 3, 2, 3])
