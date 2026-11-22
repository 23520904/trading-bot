"""Unit tests for services/market-collector."""

import json

import pytest
from market_collector.main import CoinbaseCollector, CollectorSettings


def test_collector_seq_increment() -> None:
    settings = CollectorSettings(schema_registry_url="")
    collector = CoinbaseCollector(settings)

    s1 = collector.get_next_seq("BTC-USD")
    s2 = collector.get_next_seq("BTC-USD")
    s3 = collector.get_next_seq("ETH-USD")

    assert s1 == 1
    assert s2 == 2
    assert s3 == 1


def test_collector_message_filtering() -> None:
    settings = CollectorSettings(schema_registry_url="")
    collector = CoinbaseCollector(settings)

    # Subscriptions or status frames should be ignored without raising exceptions
    subscribe_frame = json.dumps({"type": "subscriptions", "channels": []})
    collector.handle_message(subscribe_frame)


@pytest.mark.asyncio
async def test_collector_readyz_semantics() -> None:
    """/readyz returns 503 when disconnected, and 200 when WS and Kafka are healthy."""
    from httpx import ASGITransport, AsyncClient
    from market_collector import main as mc_main

    async with AsyncClient(transport=ASGITransport(app=mc_main.app), base_url="http://test") as client:
        # Initially disconnected
        mc_main.is_connected = False
        mc_main.collector_instance = None
        r1 = await client.get("/readyz")
        assert r1.status_code == 503
        assert r1.json()["status"] == "not_ready"

        # Mock healthy collector and connected WS
        settings = CollectorSettings(schema_registry_url="")
        collector = CoinbaseCollector(settings)

        class MockWS:
            closed = False

        collector.active_ws = MockWS()
        mc_main.collector_instance = collector
        mc_main.is_connected = True

        r2 = await client.get("/readyz")
        assert r2.status_code == 200
        assert r2.json()["status"] == "ok"
        assert r2.json()["ws"] == "connected"
        assert r2.json()["kafka"] == "healthy"
