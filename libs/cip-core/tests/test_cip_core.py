"""Tests for libs/cip-core domain models, Avro codecs, and RFC 9562 UUIDv7."""

import uuid
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
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
    reset_uuid7_state,
)


@pytest.fixture(autouse=True)
def clean_uuid7_state() -> Generator[None, None, None]:
    reset_uuid7_state()
    yield
    reset_uuid7_state()


def test_uuid7_strict_monotonic_10k_ids() -> None:
    """Generate 10,000 UUIDv7 in tight loop and verify strict monotonicity and uniqueness."""
    count = 10_000
    uuids = [generate_uuid7() for _ in range(count)]

    # 1. Uniqueness
    assert len(set(uuids)) == count, "All generated UUIDv7 must be unique"

    # 2. Strict monotonicity
    for i in range(count - 1):
        assert uuids[i] < uuids[i + 1], f"Monotonicity violation at index {i}: {uuids[i]} >= {uuids[i + 1]}"


def test_uuid7_counter_overflow() -> None:
    """When generating more than 4,096 IDs in the exact same millisecond, counter overflow increments timestamp."""
    fixed_time = datetime(2026, 11, 22, 12, 0, 0, tzinfo=UTC)

    with patch("cip_core.domain.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_time
        # Generate 5,000 IDs on the exact same millisecond to force counter overflow (> 4,095)
        uuids = [generate_uuid7() for _ in range(5_000)]

    assert len(set(uuids)) == 5_000
    for i in range(len(uuids) - 1):
        assert uuids[i] < uuids[i + 1], f"Counter overflow broken monotonicity at {i}"


def test_uuid7_clock_going_backwards() -> None:
    """When system clock steps backwards (NTP drift), monotonicity is strictly maintained."""
    base_time = datetime(2026, 11, 22, 12, 0, 10, tzinfo=UTC)

    with patch("cip_core.domain.datetime") as mock_dt:
        mock_dt.now.return_value = base_time
        u1 = generate_uuid7()
        u2 = generate_uuid7()

        # Clock jumps backwards by 60 seconds
        mock_dt.now.return_value = base_time - timedelta(seconds=60)
        u3 = generate_uuid7()
        u4 = generate_uuid7()

    assert u1 < u2 < u3 < u4, "Monotonicity must be preserved across backward clock jumps"


def test_uuid7_version_and_variant_bits() -> None:
    """Verify version 7 (0b0111) and RFC 4122/9562 variant (0b10xx) bits."""
    for _ in range(100):
        u_str = generate_uuid7()
        assert len(u_str) == 36
        assert u_str[8] == "-" and u_str[13] == "-" and u_str[18] == "-" and u_str[23] == "-"

        u = uuid.UUID(u_str)
        # Version 7: bits 48-51 are 0b0111
        assert u.version == 7
        assert u_str[14] == "7"

        # Variant: bits 64-65 are 0b10 -> character at index 19 is in (8, 9, a, b)
        assert u.variant == uuid.RFC_4122
        assert u_str[19].lower() in ("8", "9", "a", "b")


def test_uuid7_timestamp_roundtrip() -> None:
    """First 48 bits of UUIDv7 must decode to UTC millisecond timestamp matching generation time."""
    before_ms = int(datetime.now(UTC).timestamp() * 1000)
    u_str = generate_uuid7()
    after_ms = int(datetime.now(UTC).timestamp() * 1000)

    u = uuid.UUID(u_str)
    ts_ms = int.from_bytes(u.bytes[:6], byteorder="big")

    assert before_ms <= ts_ms <= after_ms, (
        f"Decoded timestamp {ts_ms} outside expected window [{before_ms}, {after_ms}]"
    )


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
