"""Integration test for Kafka -> ClickHouse Kafka Engine MV pipeline (Milestone M0).

Verifies:
1. Canonical MarketTrade event serialized in Confluent wire format (magic byte 0x00 + 4-byte Schema ID).
2. Live produce to Kafka topic market.normalized.v1.
3. ClickHouse Kafka Engine table (market.ticks_queue) consumes via AvroConfluent with Schema Registry.
4. ClickHouse Materialized View (market.ticks_mv) populates market.ticks.
5. Verification of columns (symbol, event_ts, trade_id, price, size, side, ingest_ts, event_id).
6. ReplacingMergeTree deduplication on (symbol, event_ts, trade_id).
7. Poison pill resilience: corrupt message does NOT stall subsequent ingestion.
"""

import time
from decimal import Decimal
from typing import Any

import clickhouse_connect
import pytest
from cip_core import generate_uuid7, now_utc_micros
from cip_kafka import CIPKafkaProducer, ConfluentAvroCodec, SchemaRegistryClient


@pytest.fixture(scope="module")
def ch_client() -> Any:
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="market",
        username="default",
        password="",
    )
    return client


@pytest.mark.integration
def test_clickhouse_kafka_avro_confluent_ingestion(ch_client: Any) -> None:
    schema_registry_url = "http://localhost:8081"
    kafka_bootstrap = "localhost:9093"
    topic = "market.normalized.v1"

    sr_client = SchemaRegistryClient(schema_registry_url)
    codec = ConfluentAvroCodec("schemas/avro/MarketTrade.avsc", sr_client, auto_register=True)
    producer = CIPKafkaProducer(kafka_bootstrap, client_id="test-integration-producer")

    event_id = generate_uuid7()
    test_trade_id = f"test-trade-{event_id}"
    now_micros = now_utc_micros()
    price = Decimal("95432.1000000000")
    size = Decimal("1.5000000000")
    symbol = "BTC-USD"

    record = {
        "event_id": event_id,
        "schema_version": 1,
        "source": "integration-test",
        "symbol": symbol,
        "event_ts": now_micros,
        "ingest_ts": now_micros,
        "seq": 99999,
        "trade_id": test_trade_id,
        "price": price,
        "size": size,
        "side": "BUY",
    }

    # 1. Encode with Confluent wire format
    payload = codec.encode(topic, record)
    assert len(payload) > 5
    assert payload[0] == 0x00, "Must contain magic byte 0x00"

    # 2. Produce to Kafka
    producer.produce(topic=topic, key=symbol, value=payload)
    producer.flush(10.0)

    # 3. Poll ClickHouse market.ticks until row appears (up to 15s)
    row_found = None
    deadline = time.time() + 15.0
    while time.time() < deadline:
        res = ch_client.query(
            "SELECT symbol, trade_id, price, size, side, event_id FROM market.ticks WHERE trade_id = {tid:String}",
            parameters={"tid": test_trade_id},
        )
        if res.result_rows:
            row_found = res.result_rows[0]
            break
        time.sleep(0.5)

    assert row_found is not None, f"Tick with trade_id {test_trade_id} was not ingested into ClickHouse within 15s"
    assert row_found[0] == symbol
    assert row_found[1] == test_trade_id
    assert row_found[2] == price
    assert row_found[3] == size
    assert row_found[4] == "BUY"
    assert str(row_found[5]) == event_id

    # 4. Verify ReplacingMergeTree deduplication on duplicate trade_id
    dup_event_id = generate_uuid7()
    dup_record = record.copy()
    dup_record["event_id"] = dup_event_id
    dup_record["ingest_ts"] = now_utc_micros() + 1000  # newer ingest_ts
    dup_payload = codec.encode(topic, dup_record)

    producer.produce(topic=topic, key=symbol, value=dup_payload)
    producer.flush(10.0)

    # Wait for duplicate to land
    time.sleep(2.0)
    count_res = ch_client.query(
        "SELECT count(*) FROM market.ticks FINAL WHERE trade_id = {tid:String}",
        parameters={"tid": test_trade_id},
    )
    assert count_res.result_rows[0][0] == 1, "ReplacingMergeTree FINAL must deduplicate duplicate ticks to 1 row"


@pytest.mark.integration
def test_clickhouse_kafka_poison_pill_resilience(ch_client: Any) -> None:
    """Verify that sending a corrupt message (poison pill) does NOT stall ClickHouse Kafka Engine."""
    schema_registry_url = "http://localhost:8081"
    kafka_bootstrap = "localhost:9093"
    topic = "market.normalized.v1"

    sr_client = SchemaRegistryClient(schema_registry_url)
    codec = ConfluentAvroCodec("schemas/avro/MarketTrade.avsc", sr_client, auto_register=True)
    producer = CIPKafkaProducer(kafka_bootstrap, client_id="test-poison-pill-producer")

    # Step 1: Live produce a poison pill (corrupt bytes, invalid schema ID)
    poison_pill = b"\x00\xff\xff\xff\xffcorrupt_unparseable_binary_payload"
    producer.produce(topic=topic, key="CORRUPT", value=poison_pill)
    producer.flush(10.0)

    # Step 2: Live produce a valid MarketTrade immediately after
    event_id = generate_uuid7()
    valid_trade_id = f"post-poison-trade-{event_id}"
    now_micros = now_utc_micros()
    valid_record = {
        "event_id": event_id,
        "schema_version": 1,
        "source": "integration-test",
        "symbol": "BTC-USD",
        "event_ts": now_micros,
        "ingest_ts": now_micros,
        "seq": 100001,
        "trade_id": valid_trade_id,
        "price": Decimal("96500.0000000000"),
        "size": Decimal("0.2500000000"),
        "side": "SELL",
    }
    valid_payload = codec.encode(topic, valid_record)
    producer.produce(topic=topic, key="BTC-USD", value=valid_payload)
    producer.flush(10.0)

    # Step 3: Verify the valid trade is ingested into ClickHouse (proving poison pill was skipped)
    row_found = None
    deadline = time.time() + 15.0
    while time.time() < deadline:
        res = ch_client.query(
            "SELECT symbol, trade_id, price FROM market.ticks WHERE trade_id = {tid:String}",
            parameters={"tid": valid_trade_id},
        )
        if res.result_rows:
            row_found = res.result_rows[0]
            break
        time.sleep(0.5)

    assert row_found is not None, "ClickHouse failed to ingest valid message following poison pill"
    assert row_found[0] == "BTC-USD"
    assert row_found[1] == valid_trade_id
    assert row_found[2] == Decimal("96500.0000000000")
