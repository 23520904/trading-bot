---
trigger: glob
globs: "services/strategy-engine/**, services/paper-execution/**, services/portfolio-service/**, services/risk-analytics/**, libs/strategy-core/**, jobs/research/**, jobs/ml/**"
description: "Safety and correctness rules for paper trading, backtesting and ML"
---
# Trading, research and ML rules
- **Paper only.** The only `ExecutionPort` implementation is `paper-execution`. Do not add HTTP calls to exchange order/account endpoints, and do not add exchange private-key handling.
- `signal_id = hash(strategy_id, symbol, bar_ts)`; `trading.orders.signal_id` is UNIQUE. A duplicate signal must never create a second order (test it).
- Pre-trade risk check before every simulated fill; `risk.alerts.v1` halts new orders (kill-switch). Fees, slippage and latency come from the shared simulator in `strategy-core`.
- Strategies are pure functions of point-in-time data (`on_bar(ctx) -> Signal`). Forbid look-ahead: the data API must not return anything with `ts` later than the decision time. Include a look-ahead regression test.
- Backtests are reproducible: `run_id = hash(strategy_version, params, data_snapshot)`; random seeds fixed; artifacts stored under `cip-research/backtests/<run_id>/`.
- Ledger invariants (property tests): sum of fills = position; realized + unrealized PnL reconcile; no negative cash unless margin is modelled.
- ML: features come from `cip-core` feature spec (same code online and offline). Register models in MLflow; promote by alias `champion`; record data snapshot id and seed; monitor drift (PSI/KS) and keep a rollback path.
