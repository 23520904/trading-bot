# 11 Sequences B (research & ML)

> Generated from the .drawio file - do not edit by hand.

## Sequence: S4 - Backtest run

Participants: web-dashboard, api-gateway, command-api, PostgreSQL, Kafka, backtesting (Job), ClickHouse, MinIO

- 1. POST /v1/backtests (JWT)  [web-dashboard -> api-gateway] (sync)
- 2. scope research:write -> forward  [api-gateway -> command-api] (sync)
- 3. INSERT research.jobs (queued)  [command-api -> PostgreSQL] (sync)
- 4. JobRequest [research.jobs.v1]  [command-api -> Kafka] (async)
- 5. 202 {job_id}  [command-api -> web-dashboard] (sync)
- 6. KEDA starts Job; poll request  [Kafka -> backtesting (Job)] (async)
- 7. SELECT candles / features  [backtesting (Job) -> ClickHouse] (sync)
- 8. PUT cip-research/backtests/{run}/  [backtesting (Job) -> MinIO] (sync)
- 9. UPDATE jobs; INSERT backtest_runs  [backtesting (Job) -> PostgreSQL] (sync)
- 10. GET /v1/backtests/{run} (poll)  [web-dashboard -> api-gateway] (sync)

## Sequence: S5 - Model train -> register -> deploy -> drift

Participants: airflow, spark-batch, MinIO, model-training, MLflow, model-inference, Kafka, model-monitoring, Alertmanager

- 1. submit offline-features  [airflow -> spark-batch] (sync)
- 2. write features/offline/set=vN  [spark-batch -> MinIO] (sync)
- 3. start training Job  [airflow -> model-training] (sync)
- 4. read offline features  [model-training -> MinIO] (sync)
- 5. log run, register model  [model-training -> MLflow] (sync)
- 6. alias champion if better  [model-training -> MLflow] (sync)
- 7. poll alias 60 s -> load model  [model-inference -> MLflow] (sync)
- 8. Prediction [predictions.v1]  [model-inference -> Kafka] (async)
- 9. poll features + predictions  [Kafka -> model-monitoring] (async)
- 10. PSI / KS > threshold -> alert  [model-monitoring -> Alertmanager] (sync)
- 11. JobRequest retrain [research.jobs.v1]  [model-monitoring -> Kafka] (async)
- 12. start retrain (KEDA)  [Kafka -> model-training] (async)

## Tables, notes and text blocks

Sequence diagrams B - research & ML lifecycle
Solid = synchronous - dashed = asynchronous via Kafka

S4 - Backtest run

Parity (ADR-08)
strategy-core + execution simulator are the same code as live trading. run_id = hash(strategy_version, params, data_snapshot).

S5 - Model train -> register -> deploy -> drift
