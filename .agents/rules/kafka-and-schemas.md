---
trigger: glob
globs: "schemas/**, **/*.avsc, libs/cip-kafka/**, services/**/kafka/**"
description: "Kafka producer/consumer and Avro schema rules for CIP"
---
# Kafka and schema rules
- Topic names, keys, partitions, retention: only what page 06 (`docs/design/06-kafka-topics-conventions.md`) defines. Missing? Use `/add-or-change-contract`.
- Producer: `acks=all`, `enable.idempotence=true`, `compression.type=zstd`, `linger.ms=5`. Key = `symbol` (or the key listed on page 06).
- Consumer: `enable.auto.commit=false`; commit after the sink succeeded; cooperative-sticky assignment; group name `cg.<service>[.<variant>]`.
- Failure policy: deserialization/validation failure -> `market.dlq.v1` immediately with headers `error_code`, `error_msg`, `src_topic`, `src_partition`, `src_offset`, `failed_at`; transient errors retry 3x (200 ms -> 5 s) then DLQ. Never retry forever, never drop silently.
- Always set headers `traceparent`, `schema_version`, `event_id`, `source`, `ingest_ts`.
- Avro: every event has the envelope (`event_id` UUIDv7, `schema_version`, `source`, `symbol`, `event_ts`, `ingest_ts`, `seq`). Prices/sizes are `decimal(28,10)`. Only additive, optional fields with defaults; never rename/remove; new enum symbols go last with a default.
- Compatibility mode BACKWARD_TRANSITIVE; `make contracts` must pass. Breaking change = new topic `.v2` + dual publish for at least one retention period.
- Wire format (ADR-13): every Avro topic must use the Confluent wire format (magic byte `0x00` + 4-byte schema ID). Serialization and deserialization must go through `cip-kafka` using the Confluent Schema Registry client with schema-id caching. Subject naming follows `<topic>-value`.
- Consumers must be idempotent (`event_id`, `trade_id`). Add a test that replays the same message twice.
