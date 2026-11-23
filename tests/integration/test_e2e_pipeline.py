"""End-to-end integration test across the real pipeline with live containers:

frames -> collector -> validator -> normalizer -> ClickHouse (market.ticks)
AND
bad message -> validator -> DLQ (market.dlq.v1)
"""

import json
import time
from decimal import Decimal
from typing import Any

import pytest
from cip_core import RawFrame, generate_uuid7, now_utc_micros
from cip_kafka import (
    CIPKafkaConsumer,
    CIPKafkaProducer,
    ConfluentAvroCodec,
    SchemaRegistryClient,
)
from data_validator.main import MessageValidator, ValidatorSettings
from market_normalizer.main import MarketNormalizer, NormalizerSettings

SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP = "localhost:9093"


@pytest.mark.integration
def test_e2e_valid_trade_flow_to_clickhouse(ch_client: Any) -> None:
    """Happy Path E2E Test:

    RawFrame on market.raw.v1 -> data-validator -> market.validated.v1 ->
    market-normalizer -> market.normalized.v1 -> ClickHouse (market.ticks_mv -> market.ticks).
    """
    sr_client = SchemaRegistryClient(SCHEMA_REGISTRY_URL)
    assert sr_client._http_client is not None, "Real Schema Registry client must be used (not in-memory)"
    raw_codec = ConfluentAvroCodec("schemas/avro/RawFrame.avsc", sr_client, auto_register=True)
    producer = CIPKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="e2e-raw-producer")

    test_event_id = generate_uuid7()
    test_trade_id = f"e2e-trade-{test_event_id}"
    symbol = "BTC-USD"
    now_micros = now_utc_micros()

    # 1. Collector produces RawFrame into market.raw.v1
    match_payload = {
        "type": "match",
        "trade_id": test_trade_id,
        "product_id": symbol,
        "price": "97850.50",
        "size": "0.15000000",
        "side": "buy",
        "time": "2026-11-22T01:45:00.000000Z",
        "sequence": 140001234567,
    }
    raw_frame = RawFrame(
        event_id=test_event_id,
        schema_version=1,
        source="market-collector",
        symbol=symbol,
        event_ts=now_micros,
        ingest_ts=now_micros,
        seq=50001,
        channel="match",
        payload=json.dumps(match_payload).encode("utf-8"),
        ingest_mode="live",
    )
    raw_bytes = raw_codec.encode("market.raw.v1", raw_frame.model_dump())
    producer.produce(topic="market.raw.v1", key=symbol, value=raw_bytes)
    producer.flush(10.0)

    # 2. Instantiate validator and normalizer workers
    val_settings = ValidatorSettings(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        consumer_group=f"cg.e2e.validator.{test_event_id}",
    )
    validator = MessageValidator(val_settings)

    norm_settings = NormalizerSettings(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        consumer_group=f"cg.e2e.normalizer.{test_event_id}",
    )
    normalizer = MarketNormalizer(norm_settings)

    # 3. Drive pipeline loop until trade lands in ClickHouse market.ticks
    row_found = None
    deadline = time.time() + 25.0
    while time.time() < deadline:
        validator.process_one()
        validator.producer.poll(0)
        normalizer.process_one()
        normalizer.producer.poll(0)

        res = ch_client.query(
            "SELECT symbol, trade_id, price, size, side FROM market.ticks WHERE trade_id = {tid:String}",
            parameters={"tid": test_trade_id},
        )
        if res.result_rows:
            row_found = res.result_rows[0]
            break
        time.sleep(0.1)

    validator.producer.flush(2.0)
    normalizer.producer.flush(2.0)
    validator.consumer.close()
    normalizer.consumer.close()

    assert row_found is not None, f"Trade {test_trade_id} did not land in ClickHouse market.ticks within deadline"
    assert row_found[0] == symbol
    assert row_found[1] == test_trade_id
    assert row_found[2] == Decimal("97850.5000000000")
    assert row_found[3] == Decimal("0.1500000000")
    assert row_found[4] == "BUY"


@pytest.mark.integration
def test_e2e_invalid_message_routed_to_dlq(ch_client: Any) -> None:
    """Negative Path E2E Test:

    An invalid raw frame (negative price) must be caught by data-validator,
    routed directly to market.dlq.v1, and NEVER appear in ClickHouse market.ticks.
    """
    sr_client = SchemaRegistryClient(SCHEMA_REGISTRY_URL)
    assert sr_client._http_client is not None, "Real Schema Registry client must be used (not in-memory)"
    raw_codec = ConfluentAvroCodec("schemas/avro/RawFrame.avsc", sr_client, auto_register=True)
    dlq_codec = ConfluentAvroCodec("schemas/avro/DeadLetter.avsc", sr_client, auto_register=True)
    producer = CIPKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="e2e-bad-raw-producer")

    bad_event_id = generate_uuid7()
    bad_trade_id = f"e2e-bad-trade-{bad_event_id}"
    symbol = "BTC-USD"
    now_micros = now_utc_micros()

    # Pre-create DLQ consumer and poll to trigger group assignment
    dlq_consumer = CIPKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"cg.e2e.dlq.verify.{bad_event_id}",
        topics=["market.dlq.v1"],
        auto_offset_reset="latest",
    )
    dlq_consumer.poll(0.5)

    # 1. Producer publishes invalid RawFrame (negative price) into market.raw.v1
    bad_match_payload = {
        "type": "match",
        "trade_id": bad_trade_id,
        "product_id": symbol,
        "price": "-500.00",  # INVALID: negative price violates business rule
        "size": "0.10000000",
        "side": "sell",
        "time": "2026-11-22T01:45:00.000000Z",
    }
    raw_frame = RawFrame(
        event_id=bad_event_id,
        schema_version=1,
        source="market-collector",
        symbol=symbol,
        event_ts=now_micros,
        ingest_ts=now_micros,
        seq=50002,
        channel="match",
        payload=json.dumps(bad_match_payload).encode("utf-8"),
        ingest_mode="live",
    )
    raw_bytes = raw_codec.encode("market.raw.v1", raw_frame.model_dump())
    producer.produce(topic="market.raw.v1", key=symbol, value=raw_bytes)
    producer.flush(10.0)

    # 2. Run data-validator to catch error and route to DLQ
    val_settings = ValidatorSettings(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        consumer_group=f"cg.e2e.validator.{bad_event_id}",
    )
    validator = MessageValidator(val_settings)

    deadline = time.time() + 25.0
    dlq_message_found = None
    while time.time() < deadline:
        if validator.process_one():
            validator.producer.flush(1.0)

        msg = dlq_consumer.poll(timeout=0.2)
        if msg is not None and not msg.error() and msg.value() is not None:
            raw_val = msg.value()
            if len(raw_val) >= 5 and raw_val[0] == 0:
                try:
                    decoded = dlq_codec.decode(raw_val)
                    if decoded.get("error_code") == "NON_POSITIVE_PRICE":
                        dlq_message_found = decoded
                        break
                except Exception:
                    pass
        time.sleep(0.1)

    validator.producer.flush(2.0)
    validator.consumer.close()
    dlq_consumer.close()

    assert dlq_message_found is not None, "Invalid message was not routed to market.dlq.v1"
    assert dlq_message_found["error_code"] == "NON_POSITIVE_PRICE"
    assert dlq_message_found["stage"] == "validator"

    # Confirm it NEVER landed in ClickHouse
    res = ch_client.query(
        "SELECT count(*) FROM market.ticks WHERE trade_id = {tid:String}",
        parameters={"tid": bad_trade_id},
    )
    assert res.result_rows[0][0] == 0, "Invalid message must NEVER land in ClickHouse market.ticks"
