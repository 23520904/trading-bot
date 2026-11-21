"""Script to validate Avro schemas, OpenAPI specifications, and AsyncAPI contracts."""

import glob
import json
import sys
from pathlib import Path

import fastavro


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
    openapi_ok = validate_openapi_specs()
    asyncapi_ok = validate_asyncapi_specs()

    if avro_ok and openapi_ok and asyncapi_ok:
        print("\n🎉 All contract checks PASSED!")
        sys.exit(0)
    else:
        print("\n💥 Contract checks FAILED!")
        sys.exit(1)


if __name__ == "__main__":
    main()
