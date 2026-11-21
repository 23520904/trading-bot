# 13 Observability, Security, NFR

> Generated from the .drawio file - do not edit by hand.

## Elements

- prometheus - [Container: kube-prometheus-stack] - Metrics TSDB, rules, ServiceMonitors
- alertmanager - [Container: Alertmanager] - Routing, grouping, silences
- Slack / PagerDuty - [External System: receiver]
- all application containers - [Container: OTel SDK, Prometheus client] - Traces (OTLP), metrics (/metrics), JSON logs (stdout). traceparent in Kafka headers.
- otel-collector - [Container: OpenTelemetry Collector] - Agent (DaemonSet) -> gateway (Deployment); batching, tail sampling 10% + errors.
- loki - [Container: Grafana Loki, S3 chunks] - Log store (JSON logs)
- grafana - [Container: Grafana 11] - Dashboards, explore, SLO views
- tempo - [Container: Grafana Tempo, S3] - Trace store

## Connections (from -> to : label)

- all application containers -> otel-collector : OTLP gRPC 4317 (sync)
- otel-collector -> prometheus : remote write (sync)
- otel-collector -> loki : push logs (sync)
- otel-collector -> tempo : OTLP traces (sync)
- prometheus -> alertmanager : fires alerts (sync)
- alertmanager -> Slack / PagerDuty : webhook (sync)
- prometheus -> grafana : PromQL (sync)
- loki -> grafana : LogQL (sync)
- tempo -> grafana : TraceQL (sync)

## Tables, notes and text blocks

Cross-cutting - observability pipeline (drawn once for all containers)
Arrow = data flow - one OTel pipeline; trace context propagated via HTTP headers and Kafka headers (traceparent)

SLI / SLO and alerts

| SLI | SLO (proposed) | Measurement | Alert |
|---|---|---|---|
| Pipeline freshness | tick -> candle p95 <= 1 s | cip_pipeline_e2e_seconds | page if p95 > 2 s for 5 min |
| Dashboard latency | tick -> WS update p95 <= 500 ms | cip_ws_e2e_seconds | warn > 1 s |
| Completeness | raw -> normalized >= 99.99% (excluding DLQ) | per-symbol per-minute count compare | ticket |
| DLQ rate | <= 0.1% of raw (5 min) | invalid_total / frames_total | warn 1%, page 5% |
| Feed liveness | no symbol silent > 30 s | cip_ingest_last_event_age_seconds | page |
| API | 99.9% availability (30 d); p95 <= 300 ms (<= 5k rows) | http_server_duration | burn-rate 2% / 1 h |
| Consumer lag | < 10k msgs or < 30 s | kafka_consumergroup_lag | warn after 5 min |
| Paper trading | signal -> order p95 <= 1 s | cip_signal_to_order_seconds | warn |
| Model drift | PSI < 0.2 per feature | cip_model_psi{model,feature} | ticket + retrain job |

Telemetry conventions

| Signal | Rule |
|---|---|
| Metrics | name = cip_<domain>_<name>_<unit>; labels service, symbol (<= 100), tf, topic; no high-cardinality labels (ids). |
| Logs | JSON to stdout: ts, level, service, trace_id, span_id, event, symbol; collected by OTel -> Loki. |
| Traces | W3C traceparent in HTTP and Kafka headers; head sampling 10% + 100% errors (tail sampling in collector). |

Security controls

| Control | Implementation |
|---|---|
| Edge TLS | cert-manager, TLS 1.3, HSTS. |
| AuthN | Keycloak OIDC code + PKCE; JWT RS256; access 10 min, refresh 8 h; JWKS cached 10 min in api-gateway and realtime-gateway. |
| AuthZ | scope -> route in api-gateway; resource checks in services; admin actions written to audit.events. |
| Service-to-service | Linkerd mTLS; default-deny NetworkPolicies. |
| Kafka | mTLS + ACL per service principal (produce / consume only own topics); Schema Registry writes restricted to CI. |
| Datastores | one DB role per service (least privilege), read-only roles for cross-owner reads; secrets via External Secrets -> Vault, rotated 90 d. |
| Secrets | none in Git; Coinbase public channels need none (A5). |
| Supply chain | SBOM, Trivy gate, cosign signature verified by Kyverno. |
| Data classes | market data = public; positions / PnL (paper) = internal; user identity = confidential. |

Non-functional requirements (all values proposed)

| Attribute | Target | Basis |
|---|---|---|
| Throughput | 5,000 msg/s sustained, 20,000 burst (x4 headroom) | A2; load test at M6 |
| Latency | see SLO table | A3 |
| Availability | read path 99.9%; ingestion 99.5% (single region) | proposed |
| Durability | RPO <= 1 min Kafka (RF=3), <= 5 min PG (WAL), MinIO EC; RTO <= 30 min (Argo CD resync + restore) | proposed; DR drill at M6 |
| Scalability | 100 -> 500 symbols: partitions 12 -> 48 and Flink parallelism x4 (planned re-key) | proposed |
| Consistency | eventual across views; per-symbol ordering; ClickHouse dedupe after merge (use argMax / FINAL) | ADR-05 |
| Cost | TBD - budget unknown; first estimate after M2 load test | A10 |
