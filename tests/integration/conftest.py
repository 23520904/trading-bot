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

    # Ensure ClickHouse migrations are applied via templated runner
    try:
        import sys

        subprocess.run(
            [sys.executable, "db/clickhouse/apply_migrations.py"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        pytest.fail(f"Failed to apply ClickHouse migrations via apply_migrations.py: {e}")

    yield

    # Teardown behavior:
    # In local development, containers remain running for fast developer feedback.
    # In CI runners or isolated test environments, TEARDOWN_CONTAINERS=true triggers cleanup.
    import os

    if os.environ.get("TEARDOWN_CONTAINERS") == "true":
        subprocess.run(
            ["docker", "compose", "-f", "tools/docker-compose.yml", "down", "-v"],
            check=False,
            capture_output=True,
        )
