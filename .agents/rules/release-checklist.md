---
trigger: manual
description: "Pre-release / milestone sign-off checklist (use with @release-checklist)"
---
# Release checklist
- [ ] `make lint test contracts docs` green on a clean checkout
- [ ] Milestone acceptance criteria on page 17 verified with evidence (commands, screenshots, load-test numbers)
- [ ] Every container in scope has: spec-card DoD met, health endpoints, metrics, dashboards, alerts, runbook entry
- [ ] No schema/API breaking change without ADR + dual publish plan
- [ ] Chaos checks done: kill broker, kill Flink task manager, drop Redis, 60 s WebSocket outage
- [ ] Images scanned (Trivy), signed (cosign), SBOM attached; no secrets in repo history
- [ ] Docs regenerated; ADR table (page 00) up to date; risk register reviewed
