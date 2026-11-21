---
trigger: model_decision
description: "Apply when writing tests, adding logs/metrics/traces, or defining SLO alerts for any CIP service or job"
---
# Testing and observability rules
- Pyramid: unit (pure domain) -> integration with `testcontainers` (Kafka, PostgreSQL, ClickHouse, Redis, MinIO) -> contract tests -> e2e on the walking skeleton. Use recorded Coinbase frames, never the live feed, in CI.
- Every service needs tests for: duplicate message, out-of-order message, poison pill -> DLQ, downstream outage and recovery, graceful shutdown.
- Property-based tests (hypothesis) for validators, ledger and candle aggregation.
- Metrics: `cip_<domain>_<name>_<unit>`; required per consumer: processed_total, failed_total, lag, processing_seconds; per pipeline: `cip_pipeline_e2e_seconds`, `cip_ingest_last_event_age_seconds`.
- Traces: W3C `traceparent` through HTTP and Kafka headers; one span per message handled; error spans carry `error_code`.
- SLOs and alert thresholds: page 13. A service is not done until its dashboards/alerts exist in `deploy/`.
