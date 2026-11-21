"""Unit tests for services/market-normalizer."""

from decimal import Decimal

from cip_core import (
    MarketTrade,
    TradeSide,
    generate_uuid7,
    now_utc_micros,
)
from cip_kafka import ConfluentAvroCodec, SchemaRegistryClient
from market_normalizer.main import MarketNormalizer, NormalizerSettings


def test_normalizer_initialization() -> None:
    settings = NormalizerSettings(schema_registry_url="")
    normalizer = MarketNormalizer(settings)
    assert normalizer.settings.consumer_group == "cg.market-normalizer"


def test_market_normalizer_canonical_conversion() -> None:
    client = SchemaRegistryClient.in_memory()
    trade_codec = ConfluentAvroCodec("schemas/avro/MarketTrade.avsc", client, auto_register=True)
    event_id = generate_uuid7()
    event_ts = now_utc_micros()

    trade = MarketTrade(
        event_id=event_id,
        schema_version=1,
        source="market-normalizer",
        symbol="BTC-USD",
        event_ts=event_ts,
        ingest_ts=now_utc_micros(),
        seq=42,
        trade_id="cb-trade-8849",
        price=Decimal("94500.5000000000"),
        size=Decimal("0.5000000000"),
        side=TradeSide.BUY,
    )

    record = trade.model_dump()
    record["side"] = trade.side.value
    encoded = trade_codec.encode("market.normalized.v1", record)
    assert encoded[0] == 0x00
    decoded = trade_codec.decode(encoded)

    # Invariants verification
    assert decoded["symbol"] == "BTC-USD"
    assert decoded["price"] == Decimal("94500.5000000000")
    assert decoded["size"] == Decimal("0.5000000000")
    assert decoded["side"] == "BUY"
    assert decoded["trade_id"] == "cb-trade-8849"
    assert isinstance(decoded["event_ts"], int)
