# AGENTS.md - Crypto Intelligence Platform (CIP)

Shared, tool-neutral instructions for every AI coding agent working in this repo.
Antigravity-specific behaviour is in `GEMINI.md`; detailed per-area rules are in `.agents/rules/`.

## What we are building
Real-time crypto market-data pipeline -> analytics -> quant research -> ML -> **paper** trading, with a React dashboard.
Coinbase (public WSS + REST) -> Kafka -> Flink/Spark -> ClickHouse / PostgreSQL / Redis / MinIO -> FastAPI -> React. Runs on Kubernetes.
**Scope limit: paper trading only. No code path may place a real order or hold exchange trading credentials.**

## Source of truth (read this before coding)
The architecture lives in `docs/architecture/CIP-architecture-design-pack.drawio` (18 pages).
**Never open the `.drawio` file** (huge XML). Read the generated Markdown instead: `docs/design/README.md` (index).

| Need | Read |
|---|---|
| Context, assumptions A1-A10, decisions ADR-01..12 | `00-cover-decisions.md` |
| Who talks to whom (containers) | `02`, `03`, `04`, `05` |
| Topics, keys, partitions, consumers | `06-kafka-topics-conventions.md` |
| Event schemas, REST, WebSocket contracts | `07-event-api-contracts.md` |
| DDL, buckets, Redis keys, who may read/write what | `08-data-architecture.md` |
| Internal modules of the 4 key services | `09-l3-components.md` |
| Runtime behaviour, idempotency, failure paths | `10`, `11`, `14` |
| Per-service spec card + definition of done | `15`, `16` |
| Repo layout, milestones M0-M6, acceptance criteria | `17-repo-layout-roadmap.md` |

If code and design disagree, **the design wins** until a change is agreed (see "Changing the design").

## Architecture invariants (never violate; reviewers will reject)
1. **Kafka is the system of record for streams.** Topic names, keys, partitions and schemas come only from pages 06/07. Never invent a topic or field - use the `add-or-change-contract` skill.
2. **Key = `symbol`** on market topics (per-symbol ordering). Delivery is at-least-once: consumers are idempotent (`event_id`, `trade_id`), commit offsets **after** the sink succeeds, send poison pills to `market.dlq.v1`.
3. **One owner per data object** (page 08 matrix). No cross-schema writes in PostgreSQL, no writes to another service's Redis prefix or MinIO prefix. Cross-owner reads use read-only roles or the owner's API.
4. **Candles and features are computed only in Flink jobs (real-time) and Spark jobs (batch), using the shared definitions in `libs/cip-core`.** No other service recomputes them.
5. **Strategy logic lives only in `libs/strategy-core`.** `strategy-engine` and `backtesting` import the same code and the same execution simulator. No look-ahead: only point-in-time data.
6. **`api-gateway` never touches a datastore** (except Redis `ratelimit:*`). Reads go through `query-api`, writes through `command-api`. WebSocket traffic goes ingress -> `realtime-gateway` directly.
7. **Clients never access databases.** The dashboard talks to HTTP/WebSocket only.
8. **Money and prices are `Decimal` (never float).** Timestamps are UTC with microsecond precision. Symbols are `BASE-QUOTE` (e.g. `BTC-USD`).
9. **Every container exposes** `/healthz`, `/readyz`, `/metrics`, emits OpenTelemetry traces, JSON logs, and propagates `traceparent` in HTTP and Kafka headers.
10. **No secrets in the repo, logs, images or tests.** Config comes from environment / ConfigMap / External Secrets.
11. **Schema compatibility is BACKWARD_TRANSITIVE.** Breaking change = new topic `.v2` + dual publish.
12. **Avro topics use the Confluent wire format via `cip-kafka`; no custom components that write to ClickHouse `market.*` (ADR-04).** ClickHouse ingests ticks only via Kafka-engine Materialized Views.

## Repo layout (target, see page 17)
`libs/` (cip-core, cip-kafka, cip-obs, cip-auth, strategy-core) - `schemas/` (avro, openapi, asyncapi) - `services/<name>/` - `jobs/{flink,spark,ml,research,airflow}` -
`web/dashboard` - `db/{postgres,clickhouse}/migrations` - `deploy/{helm,kustomize,argocd}` - `tools/` - `docs/`.

## Toolchain and commands
Python 3.12, `uv` per service (`pyproject.toml`), `ruff`, `mypy --strict`, `pytest`, `testcontainers`. Web: TypeScript, Vite, Playwright.
Make targets (create them in M0 if missing, keep names stable): `make up` (local docker-compose), `make lint`, `make test`, `make contracts` (Avro / OpenAPI / AsyncAPI compatibility), `make docs` (regenerate `docs/design`).

## How to work
1. **Plan first.** For any task, state the plan (files, contracts touched, tests) before writing code. Prefer one service or one vertical slice per change.
2. **Contracts first.** Update schemas/OpenAPI/DDL, then tests, then implementation.
3. **Read before coding:** the service's spec card (15/16), pages 06-08, the relevant L2 page and sequence diagram.
4. **Do not invent components.** Need something the design lacks? Stop and write a *Design question* (problem, 2-3 options, recommendation). Do not silently deviate.
5. **Definition of done** (all required): lint + types + tests green; `make contracts` green; the spec card's DoD met; metrics/logs/traces present; no TODO secrets; `docs/` updated if behaviour or contracts changed. Milestone acceptance needs integration evidence, not only unit tests.
6. **Verify, do not assume.** Run the commands and report real output. Say clearly what you did not run.
7. **Small, reviewable commits.** Conventional Commits (`feat(candle-aggregator): ...`). Never commit generated build output.

## Changing the design
Design changes go through the `sync-design-pack` skill: edit the generator data in `tools/design-pack/`, run it, run `make docs`, and add or update an ADR row on page 00. Never hand-edit `docs/design/*.md` (generated).

## Safety
No real trading, no exchange order endpoints, no private keys. Destructive commands (drop/truncate tables, delete topics/buckets, `kubectl delete`) require explicit human confirmation.
