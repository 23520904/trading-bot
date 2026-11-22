"""ClickHouse migration runner with environment variable templating.

Supports `${VAR:-default}` pattern substitution for config-driven broker lists,
registry URLs, and database parameters across local dev, CI, and Kubernetes.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import httpx

TEMPLATE_REGEX = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def render_template(sql_text: str) -> str:
    """Replace ${VAR:-default} and ${VAR} patterns with os.environ values or fallbacks."""

    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default_val = match.group(2) if match.group(2) is not None else ""
        return os.environ.get(var_name, default_val)

    return TEMPLATE_REGEX.sub(replacer, sql_text)


def apply_migrations(
    migrations_dir: str | Path = "db/clickhouse/migrations",
    clickhouse_http_url: str = "http://localhost:8123",
    dry_run: bool = False,
) -> bool:
    migrations_path = Path(migrations_dir)
    if not migrations_path.exists():
        print(f"❌ Migrations directory not found: {migrations_path}")
        return False

    sql_files = sorted(migrations_path.glob("*.sql"))
    if not sql_files:
        print(f"ℹ️  No migration files found in {migrations_path}")
        return True

    print(f"🔍 Found {len(sql_files)} ClickHouse migration file(s).")
    for sql_file in sql_files:
        raw_sql = sql_file.read_text(encoding="utf-8")
        rendered_sql = render_template(raw_sql)

        if dry_run:
            print(f"\n--- [DRY RUN] Rendered {sql_file.name} ---")
            print(rendered_sql)
            continue

        print(f"🚀 Applying {sql_file.name}...")
        # Try Docker exec first if running inside local docker-compose
        try:
            subprocess.run(
                ["docker", "exec", "-i", "cip-clickhouse", "clickhouse-client", "--multiquery"],
                input=rendered_sql.encode("utf-8"),
                capture_output=True,
                check=True,
            )
            print(f"  ✅ {sql_file.name} applied successfully via clickhouse-client")
        except Exception:
            # Fall back to ClickHouse HTTP interface
            try:
                resp = httpx.post(
                    f"{clickhouse_http_url}/",
                    content=rendered_sql.encode("utf-8"),
                    timeout=15.0,
                )
                if resp.status_code == 200:
                    print(f"  ✅ {sql_file.name} applied successfully via ClickHouse HTTP")
                else:
                    print(f"  ❌ {sql_file.name} failed via HTTP: {resp.status_code} - {resp.text}")
                    return False
            except Exception as e:
                print(f"  ❌ Failed to apply {sql_file.name}: {e}")
                return False

    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply ClickHouse migrations with templating.")
    parser.add_argument("--dry-run", action="store_true", help="Print rendered SQL without executing")
    parser.add_argument("--dir", default="db/clickhouse/migrations", help="Migrations directory")
    parser.add_argument("--url", default=os.environ.get("CLICKHOUSE_HTTP_URL", "http://localhost:8123"))
    args = parser.parse_args()

    success = apply_migrations(migrations_dir=args.dir, clickhouse_http_url=args.url, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
