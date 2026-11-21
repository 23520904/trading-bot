"""market-normalizer service:
- Consumes validated market events from market.validated.v1.
- Parses trade payloads into canonical MarketTrade model:
    - Symbol format (BASE-QUOTE e.g. BTC-USD).
    - Decimal(28, 10) price and size.
    - TradeSide enum (BUY / SELL).
    - UTC microsecond timestamps.
    - dedupe key trade_id.
- Encodes with Avro (MarketTrade.avsc).
- Publishes to market.normalized.v1.
- Manual offset commit after produce.
"""

import asyncio
import json
import signal
from decimal import Decimal

import uvicorn
from cip_core import (
    DeadLetter,
    MarketTrade,
    TradeSide,
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

logger = setup_logging("market-normalizer")


class NormalizerSettings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9093"
    schema_registry_url: str = "http://localhost:8081"
    topic_validated: str = "market.validated.v1"
    topic_normalized: str = "market.normalized.v1"
    topic_dlq: str = "market.dlq.v1"
    consumer_group: str = "cg.market-normalizer"
    http_port: int = 8003
    health_host: str = "0.0.0.0"


settings = NormalizerSettings()
app = FastAPI(title="market-normalizer")
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


class MarketNormalizer:
    def __init__(self, settings: NormalizerSettings):
        self.settings = settings
        self.consumer = CIPKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.consumer_group,
            topics=[settings.topic_validated],
        )
        self.producer = CIPKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="market-normalizer",
        )
        self.schema_registry_client = SchemaRegistryClient(settings.schema_registry_url)
        self.validated_codec = ConfluentAvroCodec("schemas/avro/ValidatedEvent.avsc", self.schema_registry_client)
        self.trade_codec = ConfluentAvroCodec("schemas/avro/MarketTrade.avsc", self.schema_registry_client)
        self.dlq_codec = ConfluentAvroCodec("schemas/avro/DeadLetter.avsc", self.schema_registry_client)

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

        try:
            val_record = self.validated_codec.decode(raw_bytes)
            payload_obj = json.loads(val_record["payload"].decode("utf-8"))

            trade_id = str(payload_obj.get("trade_id") or payload_obj.get("sequence") or val_record["seq"])
            price_dec = Decimal(str(payload_obj["price"]))
            size_dec = Decimal(str(payload_obj.get("size") or payload_obj.get("last_size", "0.0")))
            side_str = str(payload_obj.get("side", "buy")).upper()
            side = TradeSide.BUY if side_str == "BUY" else TradeSide.SELL

            market_trade = MarketTrade(
                event_id=val_record["event_id"],
                schema_version=1,
                source="market-normalizer",
                symbol=val_record["symbol"],
                event_ts=val_record["event_ts"],
                ingest_ts=now_utc_micros(),
                seq=val_record["seq"],
                trade_id=trade_id,
                price=price_dec,
                size=size_dec,
                side=side,
            )

            record = market_trade.model_dump()
            record["side"] = market_trade.side.value
            encoded = self.trade_codec.encode(self.settings.topic_normalized, record)

            headers = {
                "traceparent": f"00-{market_trade.event_id.replace('-', '')[:32]}-0000000000000003-01",
                "schema_version": "1",
                "event_id": market_trade.event_id,
                "source": "market-normalizer",
                "ingest_ts": str(market_trade.ingest_ts),
            }

            self.producer.produce(
                topic=self.settings.topic_normalized,
                key=market_trade.symbol,
                value=encoded,
                headers=headers,
            )

            self.consumer.commit(msg)
            MESSAGES_PROCESSED_TOTAL.labels(
                service="market-normalizer", topic=self.settings.topic_normalized, status="ok"
            ).inc()
            return True

        except Exception as e:
            logger.error(f"Failed to normalize trade event at offset {offset}: {e} -> DLQ", exc_info=True)
            dead_letter = DeadLetter(
                original_payload=raw_bytes,
                error_code="NORMALIZATION_FAILURE",
                error_msg=str(e),
                src_topic=topic,
                src_partition=partition,
                src_offset=offset,
                failed_at=now_utc_micros(),
                stage="normalizer",
            )
            encoded_dl = self.dlq_codec.encode(self.settings.topic_dlq, dead_letter.model_dump())
            self.producer.produce_dlq(
                dlq_topic=self.settings.topic_dlq,
                original_key=key_str,
                dead_letter=dead_letter,
                encoded_dlq_payload=encoded_dl,
            )
            self.consumer.commit(msg)
            MESSAGES_FAILED_TOTAL.labels(
                service="market-normalizer", topic=topic, error_code="NORMALIZATION_FAILURE"
            ).inc()
            return True


async def run_worker(normalizer: MarketNormalizer) -> None:
    global is_running
    while is_running:
        processed = normalizer.process_one()
        if not processed:
            await asyncio.sleep(0.01)


async def main() -> None:
    normalizer = MarketNormalizer(settings)
    config = uvicorn.Config(app, host=settings.health_host, port=settings.http_port, log_level="warning")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(server, normalizer)))

    await asyncio.gather(
        server.serve(),
        run_worker(normalizer),
    )


async def shutdown(server: uvicorn.Server, normalizer: MarketNormalizer) -> None:
    global is_running
    logger.info("Shutdown signal received for market-normalizer...")
    is_running = False
    normalizer.producer.flush(5.0)
    normalizer.consumer.close()
    server.should_exit = True


if __name__ == "__main__":
    asyncio.run(main())
