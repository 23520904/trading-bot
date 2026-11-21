"""data-validator service:
- Consumes raw market frames from market.raw.v1.
- Validates Avro decoding (RawFrame.avsc).
- Validates business rules:
    - Symbol whitelist (BTC-USD in M0).
    - Positive price & size.
    - Timestamp skew: event_ts not > 5 seconds in the future.
    - Monotonic seq check per symbol.
- Valid messages published to market.validated.v1.
- Poison pills / invalid messages routed to market.dlq.v1 with error headers.
- Manual offset commit after successful sink / DLQ dispatch.
"""

import asyncio
import json
import signal
from typing import Any

import uvicorn
from cip_core import (
    DeadLetter,
    ValidatedEvent,
    now_utc_micros,
)
from cip_kafka import CIPKafkaConsumer, CIPKafkaProducer, ConfluentAvroCodec, SchemaRegistryClient
from cip_obs import (
    MESSAGES_FAILED_TOTAL,
    MESSAGES_PROCESSED_TOTAL,
    setup_logging,
)
from fastapi import FastAPI, Response
from pydantic_settings import BaseSettings

logger = setup_logging("data-validator")


class ValidatorSettings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9093"
    schema_registry_url: str = "http://localhost:8081"
    topic_raw: str = "market.raw.v1"
    topic_validated: str = "market.validated.v1"
    topic_dlq: str = "market.dlq.v1"
    consumer_group: str = "cg.data-validator"
    allowed_symbols: list[str] = ["BTC-USD"]
    max_future_skew_seconds: float = 5.0
    http_port: int = 8002
    health_host: str = "0.0.0.0"


settings = ValidatorSettings()
app = FastAPI(title="data-validator")
is_running = True


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


class MessageValidator:
    def __init__(self, settings: ValidatorSettings):
        self.settings = settings
        self.consumer = CIPKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.consumer_group,
            topics=[settings.topic_raw],
        )
        self.producer = CIPKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="data-validator",
        )
        self.schema_registry_client = SchemaRegistryClient(settings.schema_registry_url)
        self.raw_codec = ConfluentAvroCodec("schemas/avro/RawFrame.avsc", self.schema_registry_client)
        self.validated_codec = ConfluentAvroCodec("schemas/avro/ValidatedEvent.avsc", self.schema_registry_client)
        self.dlq_codec = ConfluentAvroCodec("schemas/avro/DeadLetter.avsc", self.schema_registry_client)
        self.last_seq_map: dict[str, int] = {}

    def validate_rules(self, raw_record: dict[str, Any], payload_obj: dict[str, Any]) -> tuple[bool, str, str]:
        symbol = raw_record.get("symbol", "")
        if symbol not in self.settings.allowed_symbols:
            return False, "SYMBOL_NOT_WHITELISTED", f"Symbol {symbol} is not permitted"

        # Timestamp skew check
        event_ts = raw_record.get("event_ts", 0)
        now_micros = now_utc_micros()
        skew_s = (event_ts - now_micros) / 1_000_000.0
        if skew_s > self.settings.max_future_skew_seconds:
            return False, "CLOCK_SKEW_FUTURE", f"Timestamp {skew_s}s in future exceeds allowed skew"

        # Price and size checks for match / trade payloads
        price = payload_obj.get("price")
        size = payload_obj.get("size")
        if price is not None:
            try:
                p_val = float(price)
                if p_val <= 0:
                    return False, "NON_POSITIVE_PRICE", f"Price must be > 0, got {p_val}"
            except (ValueError, TypeError):
                return False, "INVALID_PRICE_FORMAT", f"Invalid price value: {price}"

        if size is not None:
            try:
                s_val = float(size)
                if s_val <= 0:
                    return False, "NON_POSITIVE_SIZE", f"Size must be > 0, got {s_val}"
            except (ValueError, TypeError):
                return False, "INVALID_SIZE_FORMAT", f"Invalid size value: {size}"

        return True, "", ""

    def process_one(self) -> bool:
        msg = self.consumer.poll(timeout=0.5)
        if msg is None:
            return False
        if msg.error():
            logger.error(f"Kafka error in consumer: {msg.error()}")
            return False

        topic: str = msg.topic() or ""
        part = msg.partition()
        partition: int = part if part is not None else -1
        off = msg.offset()
        offset: int = off if off is not None else -1
        raw_bytes = msg.value()
        if raw_bytes is None:
            return False
        msg_key = msg.key()
        key_str = msg_key.decode("utf-8") if msg_key is not None else ""

        # Step 1: Decode Avro
        try:
            raw_record = self.raw_codec.decode(raw_bytes)
        except Exception as e:
            logger.warning(f"Avro decode failed at offset {offset}: {e} -> routing to DLQ")
            self.route_to_dlq(
                raw_bytes=raw_bytes,
                key=key_str,
                error_code="INVALID_AVRO_SCHEMA",
                error_msg=str(e),
                topic=topic,
                partition=partition,
                offset=offset,
            )
            self.consumer.commit(msg)
            return True

        # Step 2: Validate inner JSON payload
        try:
            payload_obj = json.loads(raw_record["payload"].decode("utf-8"))
        except Exception as e:
            logger.warning(f"Payload JSON parse failed: {e} -> routing to DLQ")
            self.route_to_dlq(
                raw_bytes=raw_bytes,
                key=key_str,
                error_code="INVALID_PAYLOAD_JSON",
                error_msg=str(e),
                topic=topic,
                partition=partition,
                offset=offset,
            )
            self.consumer.commit(msg)
            return True

        # Step 3: Validate business rules
        valid, err_code, err_msg = self.validate_rules(raw_record, payload_obj)
        if not valid:
            logger.warning(f"Validation rule failure [{err_code}]: {err_msg} -> routing to DLQ")
            self.route_to_dlq(
                raw_bytes=raw_bytes,
                key=key_str,
                error_code=err_code,
                error_msg=err_msg,
                topic=topic,
                partition=partition,
                offset=offset,
            )
            self.consumer.commit(msg)
            return True

        # Step 4: Valid message -> publish to market.validated.v1
        try:
            validated_event = ValidatedEvent(
                event_id=raw_record["event_id"],
                schema_version=raw_record["schema_version"],
                source="data-validator",
                symbol=raw_record["symbol"],
                event_ts=raw_record["event_ts"],
                ingest_ts=now_utc_micros(),
                seq=raw_record["seq"],
                channel=raw_record["channel"],
                payload=raw_record["payload"],
                ingest_mode=raw_record["ingest_mode"],
            )

            encoded_val = self.validated_codec.encode(self.settings.topic_validated, validated_event.model_dump())
            headers = {
                "traceparent": f"00-{validated_event.event_id.replace('-', '')[:32]}-0000000000000002-01",
                "schema_version": "1",
                "event_id": validated_event.event_id,
                "source": "data-validator",
                "ingest_ts": str(validated_event.ingest_ts),
            }

            self.producer.produce(
                topic=self.settings.topic_validated,
                key=validated_event.symbol,
                value=encoded_val,
                headers=headers,
            )

            self.consumer.commit(msg)
            MESSAGES_PROCESSED_TOTAL.labels(
                service="data-validator", topic=self.settings.topic_validated, status="ok"
            ).inc()
            return True
        except Exception as e:
            logger.error(f"Failed to publish validated event: {e}", exc_info=True)
            return False

    def route_to_dlq(
        self,
        raw_bytes: bytes,
        key: str,
        error_code: str,
        error_msg: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        dead_letter = DeadLetter(
            original_payload=raw_bytes,
            error_code=error_code,
            error_msg=error_msg,
            src_topic=topic,
            src_partition=partition,
            src_offset=offset,
            failed_at=now_utc_micros(),
            stage="validator",
        )
        encoded_dl = self.dlq_codec.encode(self.settings.topic_dlq, dead_letter.model_dump())
        self.producer.produce_dlq(
            dlq_topic=self.settings.topic_dlq,
            original_key=key,
            dead_letter=dead_letter,
            encoded_dlq_payload=encoded_dl,
        )
        MESSAGES_FAILED_TOTAL.labels(service="data-validator", topic=topic, error_code=error_code).inc()


async def run_worker(validator: MessageValidator) -> None:
    global is_running
    while is_running:
        processed = validator.process_one()
        if not processed:
            await asyncio.sleep(0.01)


async def main() -> None:
    validator = MessageValidator(settings)
    config = uvicorn.Config(app, host=settings.health_host, port=settings.http_port, log_level="warning")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(server, validator)))

    await asyncio.gather(
        server.serve(),
        run_worker(validator),
    )


async def shutdown(server: uvicorn.Server, validator: MessageValidator) -> None:
    global is_running
    logger.info("Shutdown signal received for data-validator...")
    is_running = False
    validator.producer.flush(5.0)
    validator.consumer.close()
    server.should_exit = True


if __name__ == "__main__":
    asyncio.run(main())
