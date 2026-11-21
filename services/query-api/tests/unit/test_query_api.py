"""Unit tests for services/query-api."""

from fastapi.testclient import TestClient
from query_api.main import app

client = TestClient(app)


def test_healthz() -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
