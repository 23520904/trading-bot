"""Pytest configuration and session fixtures for integration tests."""

import subprocess
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session", autouse=True)
def ensure_infrastructure() -> Generator[None, None, None]:
    """Ensure local docker-compose infrastructure is running and healthy."""
    # Check docker compose services
    try:
        subprocess.run(
            ["docker", "compose", "-f", "tools/docker-compose.yml", "up", "-d", "--wait"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        pytest.fail(f"Failed to start/verify docker compose infrastructure: {e}")

    # Ensure ClickHouse migrations are applied
    with open("db/clickhouse/migrations/001_market_ticks.sql") as f:
        sql = f.read()

    subprocess.run(
        ["docker", "exec", "-i", "cip-clickhouse", "clickhouse-client", "--multiquery"],
        input=sql.encode("utf-8"),
        check=True,
        capture_output=True,
    )

    yield
