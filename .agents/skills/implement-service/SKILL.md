---
name: implement-service
description: Implement or extend one CIP container (service, Flink/Spark job, or web app) strictly from its design spec card, topic catalog, contracts and data-ownership rules. Use for "/implement-service <name>".
---
# Implement one service

Input: service/job name exactly as on pages 15/16. If unknown, ask.

## 1. Read (only these)
- Spec card row in `docs/design/15-*.md` or `16-*.md` (responsibility, I/O, owned data, stack, idempotency, tests, DoD).
- Topics it produces/consumes: `06-kafka-topics-conventions.md`. Schemas/APIs: `07-event-api-contracts.md`. Data it may read/write: `08-data-architecture.md`.
- The L2 page showing it (`02`-`05`), its sequence diagram(s) (`10`/`11`), and `09` if it has an L3 view.
- Matching rules in `.agents/rules/` load automatically by file path.

## 2. Plan (artifact)
Files to create, contracts touched, test list (duplicate, out-of-order, poison pill, outage/recovery, shutdown), and the DoD checklist. Flag any conflict with the design before coding.

## 3. Build in this order
1. Contracts: Avro/OpenAPI/AsyncAPI/DDL (use `/add-or-change-contract` if anything new is needed).
2. Tests (unit + testcontainers integration) written to fail first.
3. Domain code, then adapters (Kafka, DB, HTTP), then wiring (`main.py`), health/metrics/tracing.
4. Dockerfile + Helm values stub in `deploy/` (ask before changing cluster-level manifests).

## 4. Verify
Run `make lint test contracts` and the service's integration tests. Show real output. For each DoD item on the spec card, give evidence or say "not verified".

## 5. Report
Summary, files changed, commands run + results, deviations (should be none), follow-ups. Run `make docs` only if design docs changed.
