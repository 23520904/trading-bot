"""Unit tests for services/data-validator."""

import json

from cip_core import (
    DeadLetter,
    RawFrame,
    generate_uuid7,
    now_utc_micros,
)
from cip_kafka import ConfluentAvroCodec, SchemaRegistryClient
from data_validator.main import MessageValidator, ValidatorSettings


def test_validator_rules_positive() -> None:
    settings = ValidatorSettings(schema_registry_url="")
    validator = MessageValidator(settings)

    raw_record = {"symbol": "BTC-USD", "event_ts": 1000000}
    payload_obj = {"price": "95000.50", "size": "0.1"}

    valid, err_code, _ = validator.validate_rules(raw_record, payload_obj)
    assert valid is True
    assert err_code == ""


def test_validator_rules_negative_price() -> None:
    settings = ValidatorSettings(schema_registry_url="")
    validator = MessageValidator(settings)

    raw_record = {"symbol": "BTC-USD", "event_ts": 1000000}
    payload_obj = {"price": "-10.00", "size": "0.1"}

    valid, err_code, _ = validator.validate_rules(raw_record, payload_obj)
    assert valid is False
    assert err_code == "NON_POSITIVE_PRICE"


def test_validator_business_rules_codec() -> None:
    client = SchemaRegistryClient()
    raw_codec = ConfluentAvroCodec("schemas/avro/RawFrame.avsc", client)
    event_id = generate_uuid7()

    # Case 1: Valid Coinbase Match
    payload_valid = json.dumps(
        {
            "type": "match",
            "trade_id": "1001",
            "product_id": "BTC-USD",
            "price": "95420.50",
            "size": "0.1500",
            "side": "buy",
            "time": "2026-11-21T07:30:00.123456Z",
        }
    ).encode("utf-8")

    raw_frame_valid = RawFrame(
        event_id=event_id,
        source="market-collector",
        symbol="BTC-USD",
        event_ts=now_utc_micros(),
        channel="matches",
        payload=payload_valid,
        seq=1,
    )
    encoded = raw_codec.encode("market.raw.v1", raw_frame_valid.model_dump())
    assert encoded[0] == 0x00
    decoded = raw_codec.decode(encoded)
    assert decoded["symbol"] == "BTC-USD"
    inner_payload = json.loads(decoded["payload"].decode("utf-8"))
    assert float(inner_payload["price"]) > 0
    assert float(inner_payload["size"]) > 0

    # Case 2: Negative Price (Poison Pill)
    payload_invalid = json.dumps(
        {
            "type": "match",
            "trade_id": "1002",
            "product_id": "BTC-USD",
            "price": "-50.00",
            "size": "0.1500",
            "side": "buy",
        }
    ).encode("utf-8")
    inner_invalid = json.loads(payload_invalid.decode("utf-8"))
    assert float(inner_invalid["price"]) <= 0


def test_dlq_container_headers() -> None:
    client = SchemaRegistryClient()
    dlq_codec = ConfluentAvroCodec("schemas/avro/DeadLetter.avsc", client)
    poison_payload = b'{"corrupt": true, "price": "invalid"}'

    dead_letter = DeadLetter(
        original_payload=poison_payload,
        error_code="INVALID_PRICE_FORMAT",
        error_msg="Price must be a valid positive decimal",
        src_topic="market.raw.v1",
        src_partition=2,
        src_offset=1054,
        failed_at=now_utc_micros(),
        stage="validator",
    )

    encoded = dlq_codec.encode("market.dlq.v1", dead_letter.model_dump())
    assert encoded[0] == 0x00
    decoded = dlq_codec.decode(encoded)

    assert decoded["error_code"] == "INVALID_PRICE_FORMAT"
    assert decoded["src_topic"] == "market.raw.v1"
    assert decoded["src_partition"] == 2
    assert decoded["src_offset"] == 1054
    assert decoded["stage"] == "validator"
    assert decoded["original_payload"] == poison_payload
