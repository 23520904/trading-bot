"""Root pytest configuration for CIP."""

import pytest


@pytest.fixture(autouse=True)
def configure_test_environment(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Set CIP_ENV=test ONLY for unit tests.

    Integration tests (marked with 'integration') must not have CIP_ENV=test so they use real infrastructure.
    """
    if "integration" not in request.keywords:
        monkeypatch.setenv("CIP_ENV", "test")
