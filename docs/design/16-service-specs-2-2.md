# 16 Service Specs 2-2

> Generated from the .drawio file - do not edit by hand.

## Tables, notes and text blocks

Service spec cards (2/2)
One row = one deployable container - implement against: topic catalog (06), contracts (07), data ownership (08) - DoD = definition of done

Specs

| Service | Responsibility, inputs -> outputs | Owned data | Stack, scaling, state | Idempotency, retry | Tests, definition of done |
|---|---|---|---|---|---|
| portfolio-service | Positions, avg price, realized / unrealized PnL. In: fills, snap:candle. Out: positions.v1 | PG trading.positions, pnl_daily | Python; Deployment x2 (partition by account) | fill_id dedupe; UPSERT idempotent | Ledger invariants (property tests). DoD: sum of fills = position |
| risk-analytics | Exposure, VaR95, drawdown; limit breach alerts. In: positions. Out: risk.alerts.v1, PG risk.* | PG risk.* | Python, numpy / pandas; Deployment 1-2 | alert key (account, rule_id, window) | Scenario tests. DoD: breach => alert <= 1 s |
| model-inference | Online scoring. In: features.online.v1, MLflow alias. Out: predictions.v1 | - (model cache) | Python, MLflow client, ONNX / sklearn; Deployment x2; hot-swap | idempotent by (symbol, ts, model_version) | Golden predictions. DoD: p95 < 50 ms per event |
| model-training | Train, evaluate, register. In: MinIO features, research.jobs.v1. Out: MLflow runs / models | MLflow (cip-ml) | Python (sklearn / xgboost / PyTorch); K8s Job, GPU optional | seeded; data snapshot id logged | Reproducibility test. DoD: same data + seed => same metrics |
| model-monitoring | Drift (PSI / KS) and performance decay. In: features, predictions. Out: metrics, research.jobs.v1 (retrain) | - | Python; Deployment x1 | windowed; alert dedupe in Alertmanager | Synthetic shift. DoD: alert fires on PSI > 0.2 |
| mlflow | Tracking + model registry | PG db mlflow; MinIO cip-ml | MLflow 2; Deployment x2 | - | Smoke test. DoD: registry meets API SLO |
| pairs-research | Cointegration (Engle-Granger, Johansen), hedge ratio, half-life. In: CH candles, research.jobs.v1 | PG research.pair_candidates | Python, statsmodels; K8s Job | upsert by job_id | Synthetic cointegrated series. DoD: 20 symbols < 5 min |
| backtesting | Event-driven backtests via strategy-core + simulator. In: CH / MinIO data, research.jobs.v1 | PG research.backtest_runs; MinIO cip-research | Python; KEDA ScaledJob | run_id = hash(strategy_version, params, data_snapshot) | Golden-run regression, no look-ahead test. DoD: reproducible |
| api-gateway | AuthN/Z, routing, rate limit. In: HTTPS. Out: HTTP to query-api / command-api | Redis ratelimit:* | FastAPI + authlib; Deployment 2-6, HPA; stateless | stateless | OpenAPI contract tests. DoD: 401 / 403 / 429 covered |
| query-api | Read side: candles, ticks, features, trading, results. Out: JSON | - (RO roles on CH / PG / Redis) | FastAPI, clickhouse-connect, asyncpg, redis-py; Deployment 2-6, HPA | GET idempotent; ETag / Cache-Control | Schemathesis. DoD: p95 <= 300 ms |
| command-api | Write side: jobs, strategy toggle, DLQ replay; audit | PG research.jobs, audit.events | FastAPI; Deployment x2 | Idempotency-Key; TX + outbox publish | Contract + authZ tests. DoD: every command audited |
| realtime-gateway | WSS fan-out, snapshot + delta. In: Kafka candles / signals / positions, Redis snap:* | - | Starlette WS, aiokafka, redis-py; Deployment 2-8, HPA on connections | per-channel seq; resync on gap | Load test 1k clients. DoD: server-side p95 <= 100 ms |
| web-dashboard | UI: charts, research, paper trading, reconnect UX | - | React 18, TypeScript, Vite, lightweight-charts, TanStack Query; nginx static | - | Playwright e2e. DoD: reconnect and resync tested |
