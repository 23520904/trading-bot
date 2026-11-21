"""Tests for libs/cip-core domain models and Avro codecs."""

from decimal import Decimal
from pathlib import Path

from cip_core import (
    AvroCodec,
    DeadLetter,
    MarketTrade,
    RawFrame,
    TradeSide,
    datetime_to_micros,
    generate_uuid7,
    micros_to_datetime,
    now_utc_micros,
)


def test_uuid7_generation() -> None:
    uuids = [generate_uuid7() for _ in range(50)]
    for u in uuids:
        assert len(u) == 36
        # Version should be 7 (RFC 9562 / UUIDv7)
        assert u[14] == "7"
        # Variant should be RFC 4122 (0b10xx -> '8', '9', 'a', 'b')
        assert u[19].lower() in ("8", "9", "a", "b")
    # Monotonicity: subsequent UUIDv7 generated in sequence must be strictly monotonically increasing
    for i in range(len(uuids) - 1):
        assert uuids[i] < uuids[i + 1]


def test_micros_conversion() -> None:
    now_micros = now_utc_micros()
    dt = micros_to_datetime(now_micros)
    micros_back = datetime_to_micros(dt)
    assert abs(now_micros - micros_back) < 1000


def test_market_trade_avro_codec() -> None:
    schema_path = Path("schemas/avro/MarketTrade.avsc")
    codec = AvroCodec(schema_path)

    trade = MarketTrade(
        event_id=generate_uuid7(),
        schema_version=1,
        source="market-normalizer",
        symbol="BTC-USD",
        event_ts=now_utc_micros(),
        ingest_ts=now_utc_micros(),
        seq=101,
        trade_id="trade-999",
        price=Decimal("94500.1234567890"),
        size=Decimal("1.2500000000"),
        side=TradeSide.BUY,
    )

    record = trade.model_dump()
    # Enum to string for Avro
    record["side"] = trade.side.value

    encoded = codec.encode(record)
    assert len(encoded) > 0

    decoded = codec.decode(encoded)
    assert decoded["symbol"] == "BTC-USD"
    assert decoded["trade_id"] == "trade-999"
    assert decoded["price"] == Decimal("94500.1234567890")
    assert decoded["size"] == Decimal("1.2500000000")
    assert decoded["side"] == "BUY"


def test_raw_frame_avro_codec() -> None:
    schema_path = Path("schemas/avro/RawFrame.avsc")
    codec = AvroCodec(schema_path)

    raw = RawFrame(
        event_id=generate_uuid7(),
        source="market-collector",
        symbol="BTC-USD",
        event_ts=now_utc_micros(),
        ingest_ts=now_utc_micros(),
        seq=1,
        channel="matches",
        payload=b'{"type":"match","trade_id":123}',
        ingest_mode="live",
    )

    encoded = codec.encode(raw.model_dump())
    assert len(encoded) > 0

    decoded = codec.decode(encoded)
    assert decoded["symbol"] == "BTC-USD"
    assert decoded["channel"] == "matches"
    assert decoded["payload"] == b'{"type":"match","trade_id":123}'


def test_dead_letter_avro_codec() -> None:
    schema_path = Path("schemas/avro/DeadLetter.avsc")
    codec = AvroCodec(schema_path)

    dl = DeadLetter(
        original_payload=b"bad-data",
        error_code="INVALID_SCHEMA",
        error_msg="Deserialization failed",
        src_topic="market.raw.v1",
        src_partition=0,
        src_offset=42,
        failed_at=now_utc_micros(),
        stage="validator",
    )

    encoded = codec.encode(dl.model_dump())
    assert len(encoded) > 0

    decoded = codec.decode(encoded)
    assert decoded["error_code"] == "INVALID_SCHEMA"
    assert decoded["src_offset"] == 42
