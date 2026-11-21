"""Unit tests for services/query-api."""

import httpx
import pytest
from query_api.main import app


@pytest.mark.asyncio
async def test_healthz() -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
