---
trigger: glob
globs: "db/**, **/*.sql"
description: "PostgreSQL and ClickHouse schema and migration rules"
---
# Datastore and migration rules
- Schema/DDL starting point: page 08. One PostgreSQL schema per owner; one DB role per service; cross-owner reads via read-only roles. No cross-schema writes, no foreign keys across owners.
- Migrations: alembic per PostgreSQL schema, `clickhouse-migrate` for ClickHouse. Forward-only; expand-and-contract (never drop/rename a column in one deploy); `CREATE INDEX CONCURRENTLY` on existing tables; each migration is idempotent and tested on an empty DB and on the previous version.
- Types: `numeric(28,10)` for money/prices/sizes, `timestamptz` for time. Natural idempotency keys as primary/unique keys (`signal_id`, `fill_id`, `order_id`).
- ClickHouse: market tables are written only by Kafka-engine materialized views and `spark-batch`. Use `ReplacingMergeTree` with a version column; read with `argMax(..., updated_at)` or `FINAL` when exact values matter. Put `ORDER BY (symbol, ...)` and monthly `PARTITION BY`; TTLs as page 08.
- Redis is a cache and must be rebuildable; every key has a TTL and an owner (page 08).
- Never run destructive SQL against shared environments without explicit human confirmation.
