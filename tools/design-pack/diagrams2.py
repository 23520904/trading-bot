from lib import *


def p_l3(p):
    title(p, 'L3 - Component views of the four most complex containers',
          'Solid = in-process call - dashed = Kafka - each box is a module/class group with a single responsibility - arrow = data/control flow')
    y = 90
    y = comp(p, 30, y, 'market-collector - components', 5, 3, {
        'a_sub': (0, 0, 'subscription-manager', 'Shards symbols; K8s Lease leader; reads PG collector.subscriptions'),
        'a_ws': (1, 0, 'coinbase-ws-client', 'websockets; subscribe, heartbeat, reconnect backoff 1-30 s + jitter'),
        'a_st': (2, 0, 'frame-stamper', 'Adds event_id (UUIDv7), ingest_ts, source_seq'),
        'a_pub': (3, 0, 'frame-publisher', 'confluent-kafka; Avro RawFrame; key=symbol; idempotent, acks=all'),
        'a_tp': (4, 0, 'market.raw.v1', 'Kafka topic', 'topic'),
        'a_h': (0, 1, 'health-server', '/healthz /readyz /metrics (Prometheus)'),
        'a_gap': (1, 1, 'gap-detector', 'Per-symbol trade_id gaps + heartbeat tail-loss -> BackfillRequest'),
        'a_ck': (2, 1, 'checkpoint-store', 'asyncpg; last_trade_id + last_seq -> PG every 5 s'),
        'a_cmd': (1, 2, 'ops.commands.v1', 'BackfillRequest', 'topic'),
    }, [('a_sub', 'a_ws', 'symbols', 'sync'), ('a_ws', 'a_st', 'frames', 'sync'), ('a_st', 'a_pub', 'RawFrame', 'sync'),
        ('a_pub', 'a_tp', 'publish', 'async'), ('a_ws', 'a_gap', 'seq', 'sync'), ('a_st', 'a_ck', 'last_seq', 'sync'),
        ('a_gap', 'a_cmd', 'publish', 'async')])
    y = comp(p, 30, y, 'candle-aggregator (Flink job) - components', 5, 2, {
        'b_src': (0, 0, 'kafka-source', 'market.normalized.v1; event-time; watermark = out-of-orderness 2 s'),
        'b_dd': (1, 0, 'dedupe', 'keyed by trade_id; state TTL 10 min'),
        'b_w': (2, 0, 'window-1m', 'tumbling 1 m per symbol; allowedLateness 30 s'),
        'b_ro': (3, 0, 'rollup-5m..1d', 'cascades from 1m candles (cip-core spec)'),
        'b_sink': (4, 0, 'kafka-sink', 'EOS txn -> market.candles.v1 (key=symbol|tf)'),
        'b_late': (1, 1, 'late-side-output', 'beyond lateness -> market.late.v1 + metric'),
        'b_par': (2, 1, 'partial-emitter', '1 s processing-time trigger -> is_final=false'),
        'b_rs': (3, 1, 'redis-sink', 'SET snap:candle:{sym}:{tf}, snap:last:{sym}'),
    }, [('b_src', 'b_dd', 'events', 'sync'), ('b_dd', 'b_w', 'unique', 'sync'), ('b_w', 'b_ro', '1m final', 'sync'),
        ('b_ro', 'b_sink', 'candles', 'sync'), ('b_w', 'b_late', 'late', 'sync'), ('b_w', 'b_par', 'partials', 'sync'),
        ('b_par', 'b_rs', 'latest', 'sync'), ('b_ro', 'b_rs', 'final', 'sync')])
    y = comp(p, 30, y, 'strategy-engine - components', 5, 2, {
        'c_in': (0, 0, 'event-consumers', 'candles + predictions (cg.strategy-engine); manual commit'),
        'c_fr': (1, 0, 'feature-reader', 'Redis feat:{sym}:{tf} at bar close'),
        'c_run': (2, 0, 'strategy-runner', 'on_bar(ctx) -> Signal; plugins from strategy-core'),
        'c_val': (3, 0, 'signal-validator', 'cooldown, min strength; signal_id = hash(strategy, symbol, bar_ts)'),
        'c_pub': (4, 0, 'signal-publisher', 'Kafka signals.v1 (idempotent)'),
        'c_pair': (1, 1, 'pair-config', 'approved pairs: PG research.pair_candidates'),
        'c_reg': (2, 1, 'strategy-registry', 'PG trading.strategies params; hot-reload via ops.commands.v1'),
        'c_log': (3, 1, 'decision-log', 'PG trading.signals INSERT ON CONFLICT DO NOTHING'),
    }, [('c_in', 'c_fr', 'bars', 'sync'), ('c_fr', 'c_run', 'features', 'sync'), ('c_run', 'c_val', 'signal', 'sync'),
        ('c_val', 'c_pub', 'valid', 'sync'), ('c_reg', 'c_run', 'params', 'sync'), ('c_pair', 'c_run', 'pairs', 'sync'),
        ('c_pub', 'c_log', 'record', 'sync')])
    comp(p, 30, y, 'realtime-gateway - components', 5, 2, {
        'd_ws': (0, 0, 'ws-endpoint', 'Starlette /ws/v1; ping 15 s; auth message within 5 s'),
        'd_au': (1, 0, 'auth-verifier', 'JWT RS256 via JWKS cache; scopes -> allowed channels'),
        'd_reg': (2, 0, 'subscription-registry', 'conn -> channels; per-channel seq counter'),
        'd_kf': (3, 0, 'kafka-fanout', 'ephemeral consumers (latest, no commit): candles, signals, positions'),
        'd_m': (0, 1, 'metrics', 'cip_ws_connections, cip_ws_send_seconds, drops_total'),
        'd_bp': (1, 1, 'backpressure-guard', 'bounded queue per conn; drop-oldest (tickers); disconnect slow > 5 s'),
        'd_snap': (2, 1, 'snapshot-provider', 'Redis snap:* on subscribe / resync'),
    }, [('d_ws', 'd_au', 'auth', 'sync'), ('d_au', 'd_reg', 'ok', 'sync'), ('d_kf', 'd_reg', 'events', 'async'),
        ('d_reg', 'd_snap', 'snapshot', 'sync'), ('d_reg', 'd_bp', 'frames', 'sync'), ('d_bp', 'd_ws', 'send', 'sync')])


def p_seq_a(p):
    title(p, 'Sequence diagrams A - real-time paths', 'Solid = synchronous - dashed = asynchronous via Kafka - numbers = order - SLO and idempotency notes on the right')
    y = sequence(p, 30, 110, 'S1 - Live tick -> dashboard', [
        ('cb', 'Coinbase', 'grey'), ('col', 'market-collector', 'blue'), ('kf', 'Kafka', 'purple'), ('val', 'data-validator', 'blue'),
        ('nrm', 'market-normalizer', 'blue'), ('fl', 'candle-aggregator (Flink)', 'orange'), ('rt', 'realtime-gateway', 'blue'), ('ui', 'web-dashboard', 'blue')], [
        ('cb', 'col', 'WSS match / ticker frame', 's'), ('col', 'kf', 'raw frame [market.raw.v1]', 'a'), ('kf', 'val', 'poll raw', 'a'),
        ('val', 'kf', 'valid [market.validated.v1]', 'a'), ('kf', 'nrm', 'poll validated', 'a'), ('nrm', 'kf', 'MarketTrade [normalized.v1]', 'a'),
        ('kf', 'fl', 'poll normalized (event-time)', 'a'), ('fl', 'kf', 'Candle EOS txn [candles.v1]', 'a'),
        ('kf', 'rt', 'poll candles (ephemeral group)', 'a'), ('rt', 'ui', 'WSS update {ch, seq, data}', 's')])
    note(p, 1560, 140, 300, 90, '<b>SLO (A3)</b><br>tick -> dashboard p95 <= 500 ms; tick -> candle p95 <= 1 s.')
    y = sequence(p, 30, y + 40, 'S2 - Invalid message -> DLQ -> replay', [
        ('val', 'data-validator', 'blue'), ('kf', 'Kafka', 'purple'), ('rp', 'dlq-replayer', 'blue'), ('s3', 'MinIO', 'green'),
        ('cmd', 'command-api', 'blue'), ('op', 'Operator (admin)', 'grey')], [
        ('kf', 'val', 'poll raw frame', 'a'), ('val', 'kf', 'INVALID -> market.dlq.v1 + headers', 'a'), ('kf', 'rp', 'poll dead letter', 'a'),
        ('rp', 's3', 'PUT cip-dlq/YYYY/MM/DD/{id}', 's'), ('op', 'cmd', 'POST /ops/dlq/replay', 's'),
        ('cmd', 'kf', 'ReplayRequest [ops.commands.v1]', 'a'), ('kf', 'rp', 'poll ReplayRequest', 'a'),
        ('rp', 's3', 'GET matching payloads', 's'), ('rp', 'kf', 'republish [raw.backfill.v1]', 'a'),
        ('kf', 'val', 'poll -> now VALID -> validated.v1', 'a')])
    note(p, 1160, 600, 330, 90, '<b>Replay safety</b><br>event_id preserved, ingest_mode=replay; downstream dedupe makes replay idempotent.')
    sequence(p, 30, y + 40, 'S3 - Signal -> paper order -> position -> alert', [
        ('se', 'strategy-engine', 'blue'), ('kf', 'Kafka', 'purple'), ('pe', 'paper-execution', 'blue'), ('rd', 'Redis', 'green'),
        ('pg', 'PostgreSQL', 'green'), ('pf', 'portfolio-service', 'blue'), ('ra', 'risk-analytics', 'blue'), ('rt', 'realtime-gateway', 'blue')], [
        ('kf', 'se', 'poll candles + predictions', 'a'), ('se', 'rd', 'GET feat:{sym}:{tf}', 's'), ('se', 'kf', 'Signal [signals.v1]', 'a'),
        ('kf', 'pe', 'poll signal', 'a'), ('pe', 'rd', 'GET snap:last:{sym}', 's'), ('pe', 'pg', 'TX: insert order + fill', 's'),
        ('pe', 'kf', 'orders.v1 + fills.v1', 'a'), ('kf', 'pf', 'poll fills', 'a'), ('pf', 'pg', 'UPSERT trading.positions', 's'),
        ('pf', 'kf', 'PositionSnapshot [positions.v1]', 'a'), ('kf', 'ra', 'poll positions', 'a'),
        ('ra', 'kf', 'RiskAlert if limit breached', 'a'), ('kf', 'rt', 'positions / signals -> WSS', 'a')])
    note(p, 1560, 1360, 300, 110, '<b>Idempotency</b><br>signal_id = hash(strategy_id, symbol, bar_ts); orders.signal_id UNIQUE -> a duplicate signal never creates a 2nd order.')


def p_seq_b(p):
    title(p, 'Sequence diagrams B - research & ML lifecycle', 'Solid = synchronous - dashed = asynchronous via Kafka')
    y = sequence(p, 30, 110, 'S4 - Backtest run', [
        ('ui', 'web-dashboard', 'blue'), ('gw', 'api-gateway', 'blue'), ('cmd', 'command-api', 'blue'), ('pg', 'PostgreSQL', 'green'),
        ('kf', 'Kafka', 'purple'), ('bt', 'backtesting (Job)', 'orange'), ('ch', 'ClickHouse', 'green'), ('s3', 'MinIO', 'green')], [
        ('ui', 'gw', 'POST /v1/backtests (JWT)', 's'), ('gw', 'cmd', 'scope research:write -> forward', 's'),
        ('cmd', 'pg', 'INSERT research.jobs (queued)', 's'), ('cmd', 'kf', 'JobRequest [research.jobs.v1]', 'a'),
        ('cmd', 'ui', '202 {job_id}', 's'), ('kf', 'bt', 'KEDA starts Job; poll request', 'a'),
        ('bt', 'ch', 'SELECT candles / features', 's'), ('bt', 's3', 'PUT cip-research/backtests/{run}/', 's'),
        ('bt', 'pg', 'UPDATE jobs; INSERT backtest_runs', 's'), ('ui', 'gw', 'GET /v1/backtests/{run} (poll)', 's')])
    note(p, 1560, 140, 300, 100, '<b>Parity (ADR-08)</b><br>strategy-core + execution simulator are the same code as live trading. run_id = hash(strategy_version, params, data_snapshot).')
    sequence(p, 30, y + 40, 'S5 - Model train -> register -> deploy -> drift', [
        ('af', 'airflow', 'orange'), ('sp', 'spark-batch', 'orange'), ('s3', 'MinIO', 'green'), ('mt', 'model-training', 'orange'),
        ('ml', 'MLflow', 'blue'), ('mi', 'model-inference', 'blue'), ('kf', 'Kafka', 'purple'), ('mm', 'model-monitoring', 'blue'),
        ('am', 'Alertmanager', 'blue')], [
        ('af', 'sp', 'submit offline-features', 's'), ('sp', 's3', 'write features/offline/set=vN', 's'), ('af', 'mt', 'start training Job', 's'),
        ('mt', 's3', 'read offline features', 's'), ('mt', 'ml', 'log run, register model', 's'), ('mt', 'ml', 'alias champion if better', 's'),
        ('mi', 'ml', 'poll alias 60 s -> load model', 's'), ('mi', 'kf', 'Prediction [predictions.v1]', 'a'),
        ('kf', 'mm', 'poll features + predictions', 'a'), ('mm', 'am', 'PSI / KS > threshold -> alert', 's'),
        ('mm', 'kf', 'JobRequest retrain [research.jobs.v1]', 'a'), ('kf', 'mt', 'start retrain (KEDA)', 'a')])


def p_deploy(p, NS):
    title(p, 'Deployment view - Kubernetes', 'One cluster, 1 region / 3 zones (A1) - namespaces = trust and ownership zones - GitOps with Argo CD - details of policies below')
    colx = [50 + i * 600 for i in range(4)]
    W = 540
    rows = [NS[0], NS[1], NS[2]]
    hs = [max(ns_height(W, it) for _, it, _ in r) for r in rows]
    ys = [130, 130 + hs[0] + 50, 130 + hs[0] + hs[1] + 100]
    boundary(p, 20, 90, 4 * 600 + 20, sum(hs) + 180, 'Kubernetes cluster (managed K8s, node pools: general / data (local NVMe) / batch (spot, optional GPU))')
    for ri, r in enumerate(rows):
        for ci, (name, items, color) in enumerate(r):
            nsbox(p, colx[ci], ys[ri], W, 'ns/' + name, items, color, h=hs[ri], key='n_' + name)
    p.edge('n_ingress-system', 'n_app', 'HTTP 8080')
    p.edge('n_app', 'n_streaming', 'Kafka 9093 mTLS', 'async')
    p.edge('n_streaming', 'n_data', 'RESP / HTTP / S3')
    return ys[2] + hs[2] + 100


def p_obs(p):
    title(p, 'Cross-cutting - observability pipeline (drawn once for all containers)', 'Arrow = data flow - one OTel pipeline; trace context propagated via HTTP headers and Kafka headers (traceparent)')
    node(p, 'all', 'svc', 30, 120, 'all application containers', 'OTel SDK, Prometheus client', 'Traces (OTLP), metrics (/metrics), JSON logs (stdout). traceparent in Kafka headers.', w=270, h=110)
    node(p, 'otel', 'svc', 400, 120, 'otel-collector', 'OpenTelemetry Collector', 'Agent (DaemonSet) -> gateway (Deployment); batching, tail sampling 10% + errors.', w=250, h=110)
    node(p, 'prom', 'svc', 790, 20, 'prometheus', 'kube-prometheus-stack', 'Metrics TSDB, rules, ServiceMonitors', w=230, h=80)
    node(p, 'loki', 'svc', 790, 140, 'loki', 'Grafana Loki, S3 chunks', 'Log store (JSON logs)', w=230, h=80)
    node(p, 'tempo', 'svc', 790, 260, 'tempo', 'Grafana Tempo, S3', 'Trace store', w=230, h=80)
    node(p, 'graf', 'svc', 1160, 140, 'grafana', 'Grafana 11', 'Dashboards, explore, SLO views', w=230, h=80)
    node(p, 'am', 'svc', 1160, 20, 'alertmanager', 'Alertmanager', 'Routing, grouping, silences', w=230, h=80)
    node(p, 'slack', 'ext', 1500, 20, 'Slack / PagerDuty', 'receiver', '', w=200, h=80)
    p.edge('all', 'otel', 'OTLP gRPC 4317')
    p.edge('otel', 'prom', 'remote write', style='exitX=1;exitY=0.3;entryX=0;entryY=0.5;')
    p.edge('otel', 'loki', 'push logs', style='exitX=1;exitY=0.5;entryX=0;entryY=0.5;')
    p.edge('otel', 'tempo', 'OTLP traces', style='exitX=1;exitY=0.7;entryX=0;entryY=0.5;')
    p.edge('prom', 'am', 'fires alerts')
    p.edge('am', 'slack', 'webhook')
    p.edge('prom', 'graf', 'PromQL', style='exitX=1;exitY=0.8;entryX=0.2;entryY=0;')
    p.edge('loki', 'graf', 'LogQL')
    p.edge('tempo', 'graf', 'TraceQL', style='exitX=1;exitY=0.3;entryX=0.2;entryY=1;')
    legend(p, 30, 380)
    return 600
