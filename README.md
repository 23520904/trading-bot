# CIP (Crypto/Capital Investment Platform) Monorepo

## Monorepo Layout

- **libs/**: Shared libraries (`cip-core`, `cip-kafka`, `cip-obs`, `cip-auth`, `strategy-core`)
- **schemas/**: Schema definitions (`avro/*.avsc`, `openapi/*.yaml`, `asyncapi/ws.yaml`)
- **services/**: Microservices architecture components
- **jobs/**: Stream & batch processing jobs (`flink`, `spark`, `ml`, `research`, `airflow`)
- **web/**: Web frontend (`dashboard` - React + Vite + TypeScript)
- **db/**: Database migration scripts (`postgres`, `clickhouse`)
- **deploy/**: Kubernetes / GitOps deployments (`helm`, `kustomize`, `argocd`)
- **tools/**: Development tooling, seed/replay scripts, and load testing (`docker-compose.yml`, k6, perf)
- **docs/**: Architecture Decision Records (ADRs) and design documentation
