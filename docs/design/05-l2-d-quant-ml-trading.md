# 05 L2-D Quant, ML & Trading

> Generated from the .drawio file - do not edit by hand.

## Elements

- market.candles.v1
- model-inference - [Container: Python 3.12, MLflow client] - Scores features online; hot-swaps model on alias champion.
- strategy-engine - [Container: Python 3.12, strategy-core] - Runs strategies per bar. Reads Redis feat:*, PG strategies & pairs.
- paper-execution - [Container: Python 3.12] - Simulated fills (fees, slippage); pre-trade risk check; ExecutionPort impl.
- portfolio-service - [Container: Python 3.12] - Positions, avg price, realized / unrealized PnL (PG trading.*).
- risk-analytics - [Container: Python 3.12, numpy] - Exposure, VaR95, drawdown; limit breach -> alert + kill-switch.
- features.online.v1
- mlflow - [Container: MLflow 2 tracking + registry] - Backend PG(mlflow); artifacts MinIO cip-ml/. Alias champion = prod model.
- model-monitoring - [Container: Python 3.12] - Drift PSI/KS and perf decay. In: features, predictions.
- research.jobs.v1
- model-training - [Job: K8s Job, sklearn/xgboost/PyTorch] - Trains, evaluates, registers. Triggers: research.jobs.v1, Airflow.
- pairs-research - [Job: K8s Job, statsmodels] - Cointegration (EG, Johansen), hedge ratio, half-life. Reads CH candles.
- backtesting - [Job: KEDA ScaledJob, strategy-core] - Event-driven backtests, same simulator as live (ADR-08).
- MinIO (S3) - [Datastore: cip-lake, cip-ml]
- PostgreSQL - [Datastore: research.*, trading.*, risk.*]
- ClickHouse - [Datastore: candles, features_log]

## Connections (from -> to : label)

- features.online.v1 -> model-inference : consume (async/Kafka)
- model-inference -> strategy-engine : predictions.v1 (async/Kafka)
- market.candles.v1 -> strategy-engine : consume (async/Kafka)
- strategy-engine -> paper-execution : signals.v1 (async/Kafka)
- paper-execution -> portfolio-service : orders.v1, fills.v1 (async/Kafka)
- portfolio-service -> risk-analytics : positions.v1 (async/Kafka)
- risk-analytics -> paper-execution : risk.alerts.v1 (halt new orders) (async/Kafka)
- model-inference -> mlflow : load champion [HTTP] (sync)
- model-inference -> model-monitoring : predictions.v1 (async/Kafka)
- model-monitoring -> model-training : research.jobs.v1 (retrain) (async/Kafka)
- model-training -> mlflow : log + register [HTTP] (sync)
- model-training -> MinIO (S3) : read offline features [S3] (sync)
- research.jobs.v1 -> pairs-research : consume (async/Kafka)
- research.jobs.v1 -> backtesting : consume (async/Kafka)
- pairs-research -> PostgreSQL : write pair candidates [SQL] (sync)
- backtesting -> PostgreSQL : write runs [SQL] (sync)
- backtesting -> ClickHouse : read candles [HTTP] (sync)

## Tables, notes and text blocks

L2-D - Container view: Quant research, ML & paper trading
Real-time chain (top) - ML lane (left) - quant batch lane (bottom right) - direct service arrows carry the Kafka topic name - input/output topics per service: page 06

Invariants
signal_id = hash(strategy, symbol, bar_ts); orders.signal_id UNIQUE -> duplicate signal = no 2nd order.
Kill-switch: risk.alerts.v1 makes paper-execution reject new orders.

Quant lane
Jobs are triggered via command-api -> research.jobs.v1 (KEDA). strategy-core is a shared library, versioned and logged with every run.
