"""Script to register all CIP Avro schemas with Schema Registry.

Enforces ADR-13:
- In production / staging, schemas are registered in CI/CD before any services start.
- Sets subject compatibility level to BACKWARD_TRANSITIVE.
- Enables services to run with auto_register=False (fetching schema IDs without writing).

CI Step Usage:
--------------
In CI/CD deployment pipelines (GitHub Actions, GitLab CI, Argo CD hooks):

  - name: Register Avro Schemas
    env:
      SCHEMA_REGISTRY_URL: http://schema-registry:8081
    run: |
      python tools/register_schemas.py
"""

import json
import os
import sys
from pathlib import Path

import httpx

SUBJECT_MAPPINGS: list[tuple[str, str]] = [
    # (subject_name, avro_schema_path)
    ("market.raw.v1-value", "schemas/avro/RawFrame.avsc"),
    ("market.raw.backfill.v1-value", "schemas/avro/RawFrame.avsc"),
    ("market.validated.v1-value", "schemas/avro/ValidatedEvent.avsc"),
    ("market.normalized.v1-value", "schemas/avro/MarketTrade.avsc"),
    ("market.dlq.v1-value", "schemas/avro/DeadLetter.avsc"),
    ("market.envelope.v1-value", "schemas/avro/Envelope.avsc"),
    # Also register by record name for tools referencing record subjects
    ("RawFrame-value", "schemas/avro/RawFrame.avsc"),
    ("ValidatedEvent-value", "schemas/avro/ValidatedEvent.avsc"),
    ("MarketTrade-value", "schemas/avro/MarketTrade.avsc"),
    ("DeadLetter-value", "schemas/avro/DeadLetter.avsc"),
    ("Envelope-value", "schemas/avro/Envelope.avsc"),
]


def register_all_schemas(sr_url: str | None = None) -> bool:
    url = (sr_url or os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")).rstrip("/")
    print(f"📡 Connecting to Schema Registry at {url}...")

    try:
        ping = httpx.get(f"{url}/subjects", timeout=5.0)
        if ping.status_code != 200:
            print(f"❌ Schema Registry returned HTTP {ping.status_code}: {ping.text}")
            return False
    except Exception as e:
        print(f"❌ Could not connect to Schema Registry at {url}: {e}")
        return False

    all_success = True
    print(f"🚀 Registering {len(SUBJECT_MAPPINGS)} schema subjects with BACKWARD_TRANSITIVE compatibility...\n")

    for subject, schema_path in SUBJECT_MAPPINGS:
        path = Path(schema_path)
        if not path.exists():
            print(f"❌ Schema file missing: {schema_path}")
            all_success = False
            continue

        raw_schema = path.read_text(encoding="utf-8")
        parsed_schema = json.loads(raw_schema)
        schema_json_str = json.dumps(parsed_schema)

        # 1. Set subject compatibility level to BACKWARD_TRANSITIVE
        try:
            cfg_resp = httpx.put(
                f"{url}/config/{subject}",
                json={"compatibility": "BACKWARD_TRANSITIVE"},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=5.0,
            )
            if cfg_resp.status_code not in (200, 201):
                print(f"⚠️  Could not set compatibility for {subject}: {cfg_resp.status_code} {cfg_resp.text}")
        except Exception as ex:
            print(f"⚠️  Exception setting compatibility for {subject}: {ex}")

        # 2. Register the schema version
        try:
            reg_resp = httpx.post(
                f"{url}/subjects/{subject}/versions",
                json={"schema": schema_json_str},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=5.0,
            )
            if reg_resp.status_code in (200, 201):
                schema_id = reg_resp.json().get("id")
                print(f"  ✅ Subject '{subject}' -> schema ID: {schema_id} (from {path.name})")
            else:
                print(f"  ❌ Failed to register '{subject}': HTTP {reg_resp.status_code} - {reg_resp.text}")
                all_success = False
        except Exception as e:
            print(f"  ❌ Exception registering '{subject}': {e}")
            all_success = False

    return all_success


def main() -> None:
    success = register_all_schemas()
    if success:
        print("\n🎉 All schemas successfully registered in Schema Registry!")
        sys.exit(0)
    else:
        print("\n💥 Some schemas failed to register.")
        sys.exit(1)


if __name__ == "__main__":
    main()
