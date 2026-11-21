---
name: start-milestone
description: Plan and kick off a roadmap milestone (M0-M6) from the CIP design pack. Use when the user says to start, plan or continue a milestone, e.g. "/start-milestone M0".
---
# Start milestone

Input: milestone id (M0..M6). If missing, ask which one.

1. Read `docs/design/17-repo-layout-roadmap.md` (scope + acceptance criteria) and `docs/design/00-cover-decisions.md` (assumptions, ADRs).
2. List the containers in scope. For each, open its spec card (`15`/`16`) and the L2 page that shows it (`02`-`05`). Do not open the `.drawio`.
3. Check the repo: which services, schemas, migrations, make targets already exist?
4. Produce a plan artifact:
   - ordered vertical slices (smallest end-to-end first), each with: containers touched, topics/APIs/DDL touched, tests, acceptance check;
   - risks from page 14 relevant to this milestone;
   - open design questions (do not guess).
5. Ask the maintainer to approve the plan. Then execute slice by slice with `/implement-service`.
6. Finish with an acceptance report: each criterion from page 17 -> evidence (command output, numbers). Mark anything unverified as such.

M0 notes: docker-compose (Kafka KRaft, Schema Registry, ClickHouse, PostgreSQL, Redis, MinIO, Keycloak), one symbol (BTC-USD), collector -> raw -> validator -> normalizer -> ClickHouse ticks -> query-api `/v1/ticks` -> dashboard chart; create `make up|lint|test|contracts|docs` and the CI skeleton.
