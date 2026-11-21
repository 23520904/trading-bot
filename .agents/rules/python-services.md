---
trigger: glob
globs: "services/**/*.py, libs/**/*.py"
description: "Python service conventions for CIP"
---
# Python service rules
- Python 3.12, fully typed (`mypy --strict`), `ruff` clean, `asyncio` for I/O-bound services. One `pyproject.toml` per service.
- Layout: `src/<service>/{config.py, main.py, domain/, adapters/, api|consumers/}`; domain code has no I/O imports. Tests in `tests/{unit,integration}`.
- Use `Decimal` for money/prices/sizes; never `float`. Timestamps are timezone-aware UTC datetimes with microseconds. Symbol format `BASE-QUOTE`.
- Config: pydantic-settings from env; no hard-coded hosts, no secrets in code, logs or tests.
- Shared code only through `libs/` (cip-core, cip-kafka, cip-obs, cip-auth, strategy-core). Do not copy-paste between services.
- Each service exposes `/healthz` (process alive), `/readyz` (dependencies reachable, consumer assigned), `/metrics`. Metric names `cip_<domain>_<name>_<unit>`; no high-cardinality labels (no ids).
- Logs: JSON to stdout with `ts, level, service, trace_id, span_id, event, symbol`. Never log payloads containing secrets or tokens.
- Graceful shutdown: stop polling, finish in-flight work, flush producer, commit offsets, then exit (SIGTERM within 30 s).
- Datastore access only through the owner relationships on page 08; use the service's own DB role.
