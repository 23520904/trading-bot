---
trigger: glob
globs: "services/api-gateway/**, services/query-api/**, services/command-api/**, services/realtime-gateway/**, schemas/openapi/**, schemas/asyncapi/**, web/dashboard/src/api/**"
description: "REST and WebSocket contract rules for CIP"
---
# API and WebSocket rules
- Contract = `schemas/openapi/*.yaml` and `schemas/asyncapi/ws.yaml`; update them first, generate types, keep `make contracts` green.
- Auth: `Authorization: Bearer <JWT RS256>`; scopes `market:read`, `trading:read`, `research:write`, `ops:admin`. Check scope in `api-gateway`, check resource access again in the service.
- Errors: RFC 7807 `application/problem+json` with `trace_id`. Pagination: `?limit=&cursor=` -> `{items, next_cursor}`. All paths under `/v1`; additive changes only.
- POST requires `Idempotency-Key`; same key + same body returns the same response for 24 h. Long work returns `202 {job_id}`.
- Role split: `query-api` reads only (RO DB roles), `command-api` writes only and audits every command in `audit.events`, `api-gateway` has no datastore access.
- WebSocket (page 07): auth message within 5 s; `snapshot` then `update` with monotonic `seq` per (channel, key); gap or unknown `resume_from` -> `resync_required` + snapshot; server ping 15 s; bounded queue per connection, drop-oldest for tickers, close slow consumers (4408). `realtime-gateway` validates the JWT itself.
- Rate limits per role (viewer 60, researcher 120, admin 300 rpm) with `429` + `Retry-After`.
