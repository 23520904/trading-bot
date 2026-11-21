"""Tests for Confluent wire-format Avro serialization, deserialization, and schema evolution (ADR-13)."""

import struct
from decimal import Decimal
from pathlib import Path

import pytest
from cip_core import generate_uuid7, now_utc_micros
from cip_kafka import (
    ConfluentAvroCodec,
    ConfluentAvroDeserializer,
    ConfluentAvroSerializer,
    SchemaRegistryClient,
)


def test_confluent_wire_format_framing() -> None:
    client = SchemaRegistryClient()  # In-memory mode
    schema = {
        "type": "record",
        "name": "SimpleEvent",
        "namespace": "cip.test",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "count", "type": "int"},
        ],
    }

    serializer = ConfluentAvroSerializer(client, schema)
    deserializer = ConfluentAvroDeserializer(client, reader_schema=schema)

    record = {"event_id": "evt-1234", "count": 42}
    payload = serializer.encode("test.topic.v1", record)

    # 1. Byte 0 must be 0x00 magic byte
    assert len(payload) >= 5
    assert payload[0] == 0x00

    # 2. Bytes 1-4 must be big-endian unsigned 32-bit integer schema ID
    schema_id = struct.unpack(">I", payload[1:5])[0]
    assert schema_id == 1

    # 3. Deserializer recovers record accurately
    decoded = deserializer.decode(payload)
    assert decoded["event_id"] == "evt-1234"
    assert decoded["count"] == 42


def test_confluent_wire_format_validation() -> None:
    client = SchemaRegistryClient()
    deserializer = ConfluentAvroDeserializer(client)

    # Too short (< 5 bytes)
    with pytest.raises(ValueError, match="Payload too short"):
        deserializer.decode(b"\x00\x01\x02")

    # Invalid magic byte
    with pytest.raises(ValueError, match="Invalid Confluent wire format magic byte"):
        deserializer.decode(b"\x01\x00\x00\x00\x01payload")


def test_schema_evolution_roundtrip_forward_and_backward() -> None:
    """Verify schema evolution across Confluent wire format:
    - V1 writer -> V2 reader (backward compatibility with defaults)
    - V2 writer -> V1 reader (forward compatibility, reader ignores unknown fields)
    """
    client = SchemaRegistryClient()

    schema_v1 = {
        "type": "record",
        "name": "Trade",
        "namespace": "cip.market",
        "fields": [
            {"name": "trade_id", "type": "string"},
            {"name": "price", "type": "string"},
        ],
    }

    schema_v2 = {
        "type": "record",
        "name": "Trade",
        "namespace": "cip.market",
        "fields": [
            {"name": "trade_id", "type": "string"},
            {"name": "price", "type": "string"},
            {"name": "size", "type": "string", "default": "0.0"},
            {"name": "side", "type": "string", "default": "BUY"},
        ],
    }

    # Register both schemas in Schema Registry
    id_v1 = client.register_schema("market.trades-value", schema_v1)
    id_v2 = client.register_schema("market.trades-value", schema_v2)
    assert id_v1 != id_v2

    ser_v1 = ConfluentAvroSerializer(client, schema_v1)
    deser_v1 = ConfluentAvroDeserializer(client, reader_schema=schema_v1)

    ser_v2 = ConfluentAvroSerializer(client, schema_v2)
    deser_v2 = ConfluentAvroDeserializer(client, reader_schema=schema_v2)

    # 1. Old writer V1 -> New reader V2 (backward compatibility)
    msg_v1 = {"trade_id": "T-100", "price": "95000.50"}
    encoded_v1 = ser_v1.encode("market.trades", msg_v1)
    assert struct.unpack(">I", encoded_v1[1:5])[0] == id_v1

    decoded_by_v2 = deser_v2.decode(encoded_v1)
    assert decoded_by_v2["trade_id"] == "T-100"
    assert decoded_by_v2["price"] == "95000.50"
    # Defaults filled in by reader V2
    assert decoded_by_v2["size"] == "0.0"
    assert decoded_by_v2["side"] == "BUY"

    # 2. New writer V2 -> Old reader V1 (forward compatibility)
    msg_v2 = {"trade_id": "T-200", "price": "96000.00", "size": "1.25", "side": "SELL"}
    encoded_v2 = ser_v2.encode("market.trades", msg_v2)
    assert struct.unpack(">I", encoded_v2[1:5])[0] == id_v2

    decoded_by_v1 = deser_v1.decode(encoded_v2)
    assert decoded_by_v1["trade_id"] == "T-200"
    assert decoded_by_v1["price"] == "96000.00"
    # V1 reader schema does not have size or side
    assert "size" not in decoded_by_v1
    assert "side" not in decoded_by_v1


def test_canonical_schemas_confluent_codec() -> None:
    client = SchemaRegistryClient()

    raw_codec = ConfluentAvroCodec(Path("schemas/avro/RawFrame.avsc"), client)
    trade_codec = ConfluentAvroCodec(Path("schemas/avro/MarketTrade.avsc"), client)

    raw_record = {
        "event_id": generate_uuid7(),
        "schema_version": 1,
        "source": "market-collector",
        "symbol": "BTC-USD",
        "event_ts": now_utc_micros(),
        "ingest_ts": now_utc_micros(),
        "seq": 10,
        "channel": "matches",
        "payload": b'{"price": "95000"}',
        "ingest_mode": "live",
    }

    raw_bytes = raw_codec.encode("market.raw.v1", raw_record)
    assert raw_bytes[0] == 0x00
    assert len(raw_bytes) > 5

    raw_decoded = raw_codec.decode(raw_bytes)
    assert raw_decoded["symbol"] == "BTC-USD"
    assert raw_decoded["channel"] == "matches"
    assert raw_decoded["seq"] == 10

    trade_record = {
        "event_id": generate_uuid7(),
        "schema_version": 1,
        "source": "market-normalizer",
        "symbol": "BTC-USD",
        "event_ts": now_utc_micros(),
        "ingest_ts": now_utc_micros(),
        "seq": 10,
        "trade_id": "t-1",
        "price": Decimal("95000.0000000000"),
        "size": Decimal("0.5000000000"),
        "side": "BUY",
    }
    trade_bytes = trade_codec.encode("market.normalized.v1", trade_record)
    assert trade_bytes[0] == 0x00
    trade_decoded = trade_codec.decode(trade_bytes)
    assert trade_decoded["price"] == Decimal("95000.0000000000")
    assert trade_decoded["side"] == "BUY"
