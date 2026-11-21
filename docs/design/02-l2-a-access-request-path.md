# 02 L2-A Access & Request Path

> Generated from the .drawio file - do not edit by hand.

## Elements

- ClickHouse - [Datastore: market.*]
- keycloak - [Container: Keycloak 25, OIDC] - Identity provider; issues JWT (RS256); roles viewer / researcher / admin
- query-api - [Container: FastAPI] - Read side (CQRS): candles, features, signals, positions, results
- PostgreSQL - [Datastore: schemas per service]
- web-dashboard - [Container: React 18 SPA, Vite, TypeScript] - Charts, research & paper-trading UI; OIDC PKCE login
- nginx-ingress - [Container: NGINX Ingress Controller] - TLS termination; /api -> gateway, /ws -> realtime-gateway
- api-gateway - [Container: FastAPI] - JWT verify, scope check, rate limit, routing. NO datastore access
- command-api - [Container: FastAPI] - Write side: jobs, strategy toggle, DLQ replay; audit log
- Traders / Researchers
- Redis - [Datastore: snap:* feat:* ratelimit:*]
- kafka-cluster - [Container: Apache Kafka 3.8 KRaft] - topics: page 06
- realtime-gateway - [Container: FastAPI / Starlette WebSocket] - WSS fan-out, snapshot + delta; validates JWT itself (bypasses api-gateway)

## Connections (from -> to : label)

- Traders / Researchers -> web-dashboard : HTTPS (sync)
- web-dashboard -> nginx-ingress : HTTPS /api - WSS /ws (sync)
- web-dashboard -> keycloak : OIDC code + PKCE [HTTPS] (sync)
- nginx-ingress -> api-gateway : REST /api/* [HTTP] (sync)
- nginx-ingress -> realtime-gateway : WebSocket /ws [WS] (sync)
- api-gateway -> keycloak : JWKS fetch, cached [HTTPS] (sync)
- api-gateway -> query-api : GET /v1/* [HTTP] (sync)
- api-gateway -> command-api : POST / PUT /v1/* [HTTP] (sync)
- query-api -> ClickHouse : SQL (read) [HTTP] (sync)
- query-api -> PostgreSQL : SQL (read) (sync)
- query-api -> Redis : GET (read) [RESP] (sync)
- command-api -> PostgreSQL : INSERT jobs [SQL] (sync)
- command-api -> kafka-cluster : jobs, commands [Kafka] (async/Kafka)
- realtime-gateway -> Redis : GET snap:* [RESP] (sync)
- kafka-cluster -> realtime-gateway : candles, signals, positions (async/Kafka)

## Tables, notes and text blocks

L2-A - Container view: Access & request path (synchronous)
Arrow = caller -> callee - no client->datastore and no gateway->datastore access - Kafka drawn as one container (topic detail: pages 03-06)

Design rules
1) api-gateway = authN/Z + routing + rate-limit only (counters in Redis ratelimit:*).
2) Reads -> query-api (CQRS read side); writes -> command-api. Both own their datastore access via per-service DB roles.
3) WebSocket bypasses api-gateway (long-lived connections); realtime-gateway verifies the JWT itself (ADR-06, ADR-07).
4) Every container emits OTel traces/metrics/logs (page 13).
