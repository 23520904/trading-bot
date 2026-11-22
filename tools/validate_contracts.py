"""Script to validate Avro schemas, OpenAPI specifications, and AsyncAPI contracts.

Includes BACKWARD_TRANSITIVE compatibility check via Schema Registry compat API and/or previous git version.
"""

import glob
import json
import os
import subprocess
import sys
from pathlib import Path

import fastavro
import httpx


def validate_avro_schemas() -> bool:
    schema_files = glob.glob("schemas/avro/*.avsc")
    if not schema_files:
        print("ℹ️  No .avsc schema files found in schemas/avro/")
        return True

    all_valid = True
    print(f"🔍 Validating {len(schema_files)} Avro schema file(s)...")
    for file_path in schema_files:
        try:
            with open(file_path, encoding="utf-8") as f:
                schema_dict = json.load(f)
            fastavro.parse_schema(schema_dict)
            print(f"  ✅ {file_path}: Valid Avro schema")
        except Exception as e:
            print(f"  ❌ {file_path}: Invalid Avro schema -> {e}")
            all_valid = False
    return all_valid


def validate_avro_compatibility() -> bool:
    """Validate BACKWARD_TRANSITIVE compatibility against Schema Registry or git history."""
    schema_files = glob.glob("schemas/avro/*.avsc")
    if not schema_files:
        return True

    print("🔍 Checking BACKWARD_TRANSITIVE schema compatibility...")
    all_compat = True

    # 1. Check against local/remote Schema Registry if accessible
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
    sr_available = False

    try:
        resp = httpx.get(f"{sr_url}/subjects", timeout=2.0)
        if resp.status_code == 200:
            sr_available = True
    except Exception:
        sr_available = False

    if not sr_available:
        if os.environ.get("CI") == "true":
            print(f"  ❌ Schema Registry unreachable at {sr_url} in CI mode (CI=true)!")
            return False
        else:
            print(f"  ⚠️  Schema Registry unreachable at {sr_url} - SKIPPED live check (local dev, CI=false)")
    else:
        print(f"  📡 Connected to Schema Registry at {sr_url}")
        for file_path in schema_files:
            stem = Path(file_path).stem
            subject = f"{stem}-value"

            with open(file_path, encoding="utf-8") as f:
                schema_str = f.read()

            # Handle HTTP 404 (subject not found / newly introduced schema)
            versions_resp = httpx.get(f"{sr_url}/subjects/{subject}/versions", timeout=2.0)
            if versions_resp.status_code == 404:
                print(f"  ℹ️  {subject}: Subject not found in Schema Registry (HTTP 404) - newly introduced schema")
                # Set subject compatibility level to BACKWARD_TRANSITIVE for future evolutions
                try:
                    httpx.put(
                        f"{sr_url}/config/{subject}",
                        json={"compatibility": "BACKWARD_TRANSITIVE"},
                        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                        timeout=2.0,
                    )
                except Exception as ex:
                    print(f"  ⚠️  Could not set compatibility config for {subject}: {ex}")
                continue

            if versions_resp.status_code != 200:
                print(f"  ⚠️  {subject}: Could not fetch versions ({versions_resp.status_code}): {versions_resp.text}")
                continue

            versions: list[int] = versions_resp.json()

            # Verify and enforce BACKWARD_TRANSITIVE compatibility level on subject
            cfg_resp = httpx.get(f"{sr_url}/config/{subject}", timeout=2.0)
            current_compat = cfg_resp.json().get("compatibilityLevel") if cfg_resp.status_code == 200 else None
            if current_compat != "BACKWARD_TRANSITIVE":
                put_cfg = httpx.put(
                    f"{sr_url}/config/{subject}",
                    json={"compatibility": "BACKWARD_TRANSITIVE"},
                    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                    timeout=2.0,
                )
                if put_cfg.status_code == 200:
                    print(f"  ⚙️  {subject}: Configured compatibility to BACKWARD_TRANSITIVE")
                else:
                    print(f"  ⚠️  {subject}: Could not set BACKWARD_TRANSITIVE config: {put_cfg.text}")

            # Check compatibility against ALL versions (BACKWARD_TRANSITIVE requirement)
            for v in versions:
                compat_resp = httpx.post(
                    f"{sr_url}/compatibility/subjects/{subject}/versions/{v}",
                    json={"schema": schema_str},
                    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                    timeout=3.0,
                )
                if compat_resp.status_code == 200:
                    is_compat = compat_resp.json().get("is_compatible", False)
                    if is_compat:
                        print(f"  ✅ {subject}: Compatible with Schema Registry version {v}")
                    else:
                        print(f"  ❌ {subject}: INCOMPATIBLE with Schema Registry version {v}!")
                        all_compat = False
                else:
                    print(f"  ⚠️  {subject}: Compatibility check returned {compat_resp.status_code}: {compat_resp.text}")
                    all_compat = False

    # 2. Check against previous git commit (HEAD~1)
    for file_path in schema_files:
        git_prev = subprocess.run(
            ["git", "show", f"HEAD~1:{file_path}"],
            capture_output=True,
            text=True,
        )
        if git_prev.returncode == 0 and git_prev.stdout.strip():
            try:
                prev_schema = json.loads(git_prev.stdout)
                with open(file_path, encoding="utf-8") as f:
                    curr_schema = json.load(f)

                prev_fields = {f["name"]: f for f in prev_schema.get("fields", [])}
                curr_fields = {f["name"]: f for f in curr_schema.get("fields", [])}

                # Catch removed fields
                for f_name, f_prev in prev_fields.items():
                    if f_name not in curr_fields:
                        print(f"  ❌ {file_path}: BACKWARD_TRANSITIVE violation: field '{f_name}' was removed!")
                        all_compat = False

                # Catch added fields without default values
                for f_name, f_curr in curr_fields.items():
                    if f_name not in prev_fields:
                        if "default" not in f_curr:
                            print(
                                f"  ❌ {file_path}: BACKWARD_TRANSITIVE violation: new field '{f_name}' has no default value!"
                            )
                            all_compat = False
                        else:
                            print(f"  ✅ {file_path}: New field '{f_name}' has default value: {f_curr['default']!r}")

                # Catch type changes and enum changes on existing fields
                for f_name, f_prev in prev_fields.items():
                    if f_name in curr_fields:
                        f_curr = curr_fields[f_name]
                        p_type = f_prev.get("type")
                        c_type = f_curr.get("type")
                        if p_type != c_type:
                            print(
                                f"  ❌ {file_path}: BACKWARD_TRANSITIVE violation: field '{f_name}' type changed: {p_type!r} -> {c_type!r}!"
                            )
                            all_compat = False

                        # If enum, check symbol removals/alterations
                        if isinstance(p_type, dict) and p_type.get("type") == "enum":
                            p_symbols = set(p_type.get("symbols", []))
                            c_symbols = set(c_type.get("symbols", []) if isinstance(c_type, dict) else [])
                            removed_symbols = p_symbols - c_symbols
                            if removed_symbols:
                                print(
                                    f"  ❌ {file_path}: Incompatible enum change on '{f_name}': symbols removed {removed_symbols}!"
                                )
                                all_compat = False
            except Exception as ex:
                print(f"  ⚠️  Could not compare git history for {file_path}: {ex}")

    if all_compat:
        print("  ✅ All Avro schemas satisfy BACKWARD_TRANSITIVE compatibility")
    return all_compat


def validate_openapi_specs() -> bool:
    import yaml

    spec_files = glob.glob("schemas/openapi/*.yaml") + glob.glob("schemas/openapi/*.yml")
    if not spec_files:
        print("ℹ️  No OpenAPI files found in schemas/openapi/")
        return True

    all_valid = True
    print(f"🔍 Validating {len(spec_files)} OpenAPI spec file(s)...")
    for file_path in spec_files:
        try:
            with open(file_path, encoding="utf-8") as f:
                content = yaml.safe_load(f)
            if not isinstance(content, dict) or "openapi" not in content:
                print(f"  ❌ {file_path}: Missing 'openapi' version key")
                all_valid = False
            else:
                print(f"  ✅ {file_path}: Valid OpenAPI YAML structure (v{content['openapi']})")
        except Exception as e:
            print(f"  ❌ {file_path}: YAML parse error -> {e}")
            all_valid = False
    return all_valid


def validate_asyncapi_specs() -> bool:
    import yaml

    spec_file = Path("schemas/asyncapi/ws.yaml")
    if not spec_file.exists():
        print("ℹ️  No asyncapi/ws.yaml found")
        return True

    print(f"🔍 Validating AsyncAPI spec file: {spec_file}...")
    try:
        with open(spec_file, encoding="utf-8") as f:
            content = yaml.safe_load(f)
        if not content:
            print(f"  ℹ️  {spec_file} is empty (stub)")
            return True
        if not isinstance(content, dict) or "asyncapi" not in content:
            print(f"  ❌ {spec_file}: Missing 'asyncapi' key")
            return False
        print(f"  ✅ {spec_file}: Valid AsyncAPI YAML structure (v{content['asyncapi']})")
        return True
    except Exception as e:
        print(f"  ❌ {spec_file}: YAML parse error -> {e}")
        return False


def main() -> None:
    print("=== CIP Contracts Verification ===")
    avro_ok = validate_avro_schemas()
    compat_ok = validate_avro_compatibility()
    openapi_ok = validate_openapi_specs()
    asyncapi_ok = validate_asyncapi_specs()

    if avro_ok and compat_ok and openapi_ok and asyncapi_ok:
        print("\n🎉 All contract checks PASSED!")
        sys.exit(0)
    else:
        print("\n💥 Contract checks FAILED!")
        sys.exit(1)


if __name__ == "__main__":
    main()
