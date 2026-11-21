"""Unit tests for services/market-collector."""

import json

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
