"""market-collector service:
- Subscribes to Coinbase WebSocket feed (ticker/matches) for configured symbols (default: BTC-USD).
- Generates UUIDv7 event_id, UTC microsecond timestamps (ingest_ts), and per-symbol monotonic sequence numbers.
- Serializes frames with Avro (RawFrame.avsc).
- Publishes to Kafka topic market.raw.v1 with key=symbol.
- Exposes /healthz, /readyz, /metrics.
"""

import asyncio
import json
import signal
from datetime import datetime

import uvicorn
import websockets
from cip_core import RawFrame, datetime_to_micros, generate_uuid7, now_utc_micros
from cip_kafka import CIPKafkaProducer, ConfluentAvroCodec, SchemaRegistryClient
from cip_obs import (
    LAST_EVENT_AGE_SECONDS,
    MESSAGES_PROCESSED_TOTAL,
    setup_logging,
)
from fastapi import FastAPI, Response
from pydantic_settings import BaseSettings

logger = setup_logging("market-collector")


class CollectorSettings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9093"
    schema_registry_url: str = "http://localhost:8081"
    topic_raw: str = "market.raw.v1"
    coinbase_wss_url: str = "wss://ws-feed.exchange.coinbase.com"
    symbols: list[str] = ["BTC-USD"]
    http_port: int = 8001
    health_host: str = "0.0.0.0"


settings = CollectorSettings()
app = FastAPI(title="market-collector")
is_running = True
is_connected = False


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(response: Response) -> dict[str, str]:
    if is_connected:
        return {"status": "ok", "feed": "connected"}
    response.status_code = 503
    return {"status": "not_ready", "feed": "connecting"}


@app.get("/metrics")
async def metrics() -> Response:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


class CoinbaseCollector:
    def __init__(self, settings: CollectorSettings):
        self.settings = settings
        self.producer = CIPKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="market-collector",
        )
        if settings.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient(settings.schema_registry_url)
        else:
            self.schema_registry_client = SchemaRegistryClient.in_memory()
        self.codec = ConfluentAvroCodec("schemas/avro/RawFrame.avsc", self.schema_registry_client)
        self.seq_map: dict[str, int] = {}

    def get_next_seq(self, symbol: str) -> int:
        seq = self.seq_map.get(symbol, 0) + 1
        self.seq_map[symbol] = seq
        return seq

    async def run(self) -> None:
        global is_running, is_connected
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": self.settings.symbols,
            "channels": ["matches", "ticker", "heartbeat"],
        }

        backoff = 1
        while is_running:
            try:
                logger.info(f"Connecting to Coinbase WSS: {self.settings.coinbase_wss_url}...")
                async with websockets.connect(
                    self.settings.coinbase_wss_url,
                    ping_interval=15,
                    ping_timeout=10,
                ) as ws:
                    is_connected = True
                    backoff = 1
                    logger.info("Connected to Coinbase WSS. Sending subscription...")
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw_msg in ws:
                        if not is_running:
                            break
                        self.handle_message(raw_msg)
            except Exception as e:
                is_connected = False
                logger.warning(f"Coinbase WSS disconnected/error: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def handle_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, str):
                payload_bytes = raw_msg.encode("utf-8")
                msg_obj = json.loads(raw_msg)
            else:
                payload_bytes = raw_msg
                msg_obj = json.loads(raw_msg.decode("utf-8"))

            msg_type = msg_obj.get("type")
            # We are interested in trade matches or ticker updates
            if msg_type not in ("match", "ticker", "last_match"):
                return

            symbol = msg_obj.get("product_id", "BTC-USD")
            seq = self.get_next_seq(symbol)
            event_id = generate_uuid7()

            # Time parsing from Coinbase (ISO-8601 with Z)
            time_str = msg_obj.get("time")
            if time_str:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                event_ts = datetime_to_micros(dt)
            else:
                event_ts = now_utc_micros()

            ingest_ts = now_utc_micros()

            raw_frame = RawFrame(
                event_id=event_id,
                schema_version=1,
                source="market-collector",
                symbol=symbol,
                event_ts=event_ts,
                ingest_ts=ingest_ts,
                seq=seq,
                channel=msg_type,
                payload=payload_bytes,
                ingest_mode="live",
            )

            encoded = self.codec.encode(self.settings.topic_raw, raw_frame.model_dump())
            headers = {
                "traceparent": f"00-{event_id.replace('-', '')[:32]}-0000000000000001-01",
                "schema_version": "1",
                "event_id": event_id,
                "source": "market-collector",
                "ingest_ts": str(ingest_ts),
            }

            self.producer.produce(
                topic=self.settings.topic_raw,
                key=symbol,
                value=encoded,
                headers=headers,
            )

            MESSAGES_PROCESSED_TOTAL.labels(
                service="market-collector", topic=self.settings.topic_raw, status="ok"
            ).inc()
            LAST_EVENT_AGE_SECONDS.labels(service="market-collector", symbol=symbol).set(
                (now_utc_micros() - event_ts) / 1_000_000.0
            )

        except Exception as ex:
            logger.error(f"Error handling Coinbase message: {ex}", exc_info=True)


async def main() -> None:
    collector = CoinbaseCollector(settings)
    config = uvicorn.Config(app, host=settings.health_host, port=settings.http_port, log_level="warning")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(server, collector)))

    await asyncio.gather(
        server.serve(),
        collector.run(),
    )


async def shutdown(server: uvicorn.Server, collector: CoinbaseCollector) -> None:
    global is_running
    logger.info("Shutdown signal received. Stopping collector...")
    is_running = False
    collector.producer.flush(5.0)
    server.should_exit = True


if __name__ == "__main__":
    asyncio.run(main())
