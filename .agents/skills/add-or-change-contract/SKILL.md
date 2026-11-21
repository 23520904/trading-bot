---
name: add-or-change-contract
description: Add or change a Kafka topic, Avro schema, REST endpoint, WebSocket message or database table in CIP while keeping the design pack, schemas and code consistent. Use before touching any contract.
---
# Add or change a contract

1. State the need and which containers produce/consume/own it. Read pages `06`, `07`, `08`.
2. Classify: **additive** (new optional field, new endpoint, new table/column) or **breaking** (rename/remove/retype, key change, partition change, semantic change).
3. Breaking -> stop and write a Design question with options (usually new `.v2` topic + dual publish, expand-and-contract migration, or `/v2` API). Wait for approval and add an ADR row on page 00 via `/sync-design-pack`.
4. Additive -> apply in this order and keep all in one change:
   - `schemas/avro/*.avsc` or `schemas/openapi/*.yaml` or `schemas/asyncapi/ws.yaml` or `db/**/migrations`;
   - generated types / client code;
   - producers, consumers, tests (including old-version compatibility and idempotency tests);
   - topic catalog / contracts / data pages via `/sync-design-pack` (the design pack must match the code).
5. Run `make contracts lint test`. Report compatibility results explicitly.
Never add a topic, field or table that is not reflected in the design pack.
