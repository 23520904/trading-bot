# CIP Integration Tests

Integration test suite for the Crypto Intelligence Platform (CIP) real-time pipeline, Kafka, Schema Registry, ClickHouse, Postgres, and Redis.

## Running Tests

### 1. Fast Unit Tests (No Containers Required)
Runs all isolated, in-memory domain and service tests:
```bash
make test
```

### 2. Integration Tests (Requires Docker)
Spins up or verifies local container infrastructure, registers schemas, applies ClickHouse migrations, and executes end-to-end integration tests:
```bash
make test-integration
```

## CI/CD Pipeline Strategy

- **PR / Fast Commit Gate**: Runs `make lint` and `make test`. Does **NOT** run `make test-integration` (keeps PR feedback fast and deterministic without requiring Docker-in-Docker daemons).
- **Merge / Nightly / Staging Gate**: Runs `make test-integration` on runners with Docker enabled. Sets `TEARDOWN_CONTAINERS=true` to automatically tear down containers after the session finishes.

## Port Isolation & Configuration
Default ports are mapped to avoid collisions:
| Service | Environment Variable | Default Host Port | Container Internal Port |
|---|---|---|---|
| Kafka (Internal) | `KAFKA_PORT` | `9092` | `9092` |
| Kafka (Host/Test) | `KAFKA_EXTERNAL_PORT` | `9093` | `9093` |
| Schema Registry | `SCHEMA_REGISTRY_PORT` | `8081` | `8081` |
| PostgreSQL | `POSTGRES_PORT` | `5432` | `5432` |
| ClickHouse HTTP | `CLICKHOUSE_HTTP_PORT` | `8123` | `8123` |
| ClickHouse TCP | `CLICKHOUSE_TCP_PORT` | `9000` | `9000` |
| Redis | `REDIS_PORT` | `6379` | `6379` |

To run multiple concurrent integration test runners on a single host:
```bash
KAFKA_EXTERNAL_PORT=9095 SCHEMA_REGISTRY_PORT=8085 CLICKHOUSE_HTTP_PORT=8125 make test-integration
```

## Teardown Behavior
- **Local Dev (`TEARDOWN_CONTAINERS` unset or `false`)**: Containers remain running so repeated test runs complete in < 6 seconds.
- **CI Environments (`TEARDOWN_CONTAINERS=true`)**: Conftest session fixture runs `docker compose down -v` automatically after all tests finish.
