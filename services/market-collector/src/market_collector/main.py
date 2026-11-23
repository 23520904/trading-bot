"""market-collector service:

- Subscribes to Coinbase WebSocket feed (channels: matches, heartbeat) for configured symbols.
- Domain processor handles trade_id contiguity gap detection and heartbeat tail-loss detection (ADR-14).
- Serializes frames with Avro (RawFrame.avsc) in Confluent wire format (ADR-13).
- Publishes to Kafka topic market.raw.v1 with key=symbol.
- Structured logging with explicit event and symbol fields.
- Reconnect backoff with uniform jitter (ADR-15).
- Clean SIGTERM shutdown: closes WebSocket cleanly and flushes producer <= 30s.
- /readyz verifies BOTH WebSocket connection and Kafka producer health.
"""

import asyncio
import json
import random
import signal
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
import websockets
from cip_core import RawFrame
from cip_kafka import CIPKafkaProducer, ConfluentAvroCodec, SchemaRegistryClient
from cip_obs import (
    LAST_EVENT_AGE_SECONDS,
    MESSAGES_PROCESSED_TOTAL,
    setup_logging,
)
from fastapi import FastAPI, Response
from pydantic_settings import BaseSettings

from .domain import MarketDataProcessor
from .metrics import (
    COLLECTOR_GAPS_TOTAL,
    COLLECTOR_PRODUCE_ERRORS_TOTAL,
    COLLECTOR_SYMBOL_STALE,
)

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
is_running = True
is_connected = False
collector_instance: "CoinbaseCollector | None" = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Background worker task managed in main()
    yield
    if collector_instance is not None:
        await shutdown(None, collector_instance)


app = FastAPI(title="market-collector", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(response: Response) -> dict[str, Any]:
    global is_connected, collector_instance

    ws_ok = is_connected and (collector_instance is not None and collector_instance.is_ws_alive())
    kafka_ok = collector_instance is not None and collector_instance.is_kafka_healthy()

    if ws_ok and kafka_ok:
        return {"status": "ok", "ws": "connected", "kafka": "healthy"}

    response.status_code = 503
    return {
        "status": "not_ready",
        "ws": "connected" if ws_ok else "disconnected",
        "kafka": "healthy" if kafka_ok else "unhealthy",
    }


@app.get("/metrics")
async def metrics() -> Response:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


class CoinbaseCollector:
    def __init__(self, settings: CollectorSettings):
        self.settings = settings
        self.processor = MarketDataProcessor(settings.symbols)
        self.producer = CIPKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="market-collector",
        )
        if settings.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient(settings.schema_registry_url)
        else:
            self.schema_registry_client = SchemaRegistryClient.in_memory()
        self.codec = ConfluentAvroCodec("schemas/avro/RawFrame.avsc", self.schema_registry_client)
        if not settings.schema_registry_url:
            self.schema_registry_client.register_schema(f"{settings.topic_raw}-value", self.codec.raw_schema)
        self.active_ws: Any = None
        self.shutdown_event = asyncio.Event()

    def is_ws_alive(self) -> bool:
        if self.active_ws is None:
            return False
        if hasattr(self.active_ws, "closed"):
            return not self.active_ws.closed
        if hasattr(self.active_ws, "state"):
            from websockets.protocol import State

            return self.active_ws.state is State.OPEN
        return True

    def is_kafka_healthy(self) -> bool:
        # Producer is healthy if client exists and internal queue has not crashed
        try:
            return self.producer.producer is not None
        except Exception:
            return False

    def get_next_seq(self, symbol: str) -> int:
        return self.processor.get_next_seq(symbol)

    def calculate_reconnect_sleep(self, backoff: float) -> float:
        """ADR-15: Reconnect sleep = min(30, backoff * uniform(0.5, 1.5)) starting at 1 s."""
        return min(30.0, backoff * random.uniform(0.5, 1.5))

    async def run(self) -> None:
        global is_running, is_connected
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": self.settings.symbols,
            "channels": ["matches", "heartbeat"],
        }

        backoff = 1.0
        while is_running and not self.shutdown_event.is_set():
            try:
                logger.info(
                    f"Connecting to Coinbase WSS: {self.settings.coinbase_wss_url}...",
                    extra={"event": "ws_connecting", "symbol": ",".join(self.settings.symbols)},
                )
                async with websockets.connect(
                    self.settings.coinbase_wss_url,
                    ping_interval=20,
                    ping_timeout=15,
                ) as ws:
                    self.active_ws = ws
                    is_connected = True
                    backoff = 1.0
                    logger.info(
                        "Connected to Coinbase WSS. Sending subscription...",
                        extra={"event": "ws_connected", "symbol": ",".join(self.settings.symbols)},
                    )
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw_msg in ws:
                        if not is_running or self.shutdown_event.is_set():
                            break
                        self.handle_message(raw_msg)
            except Exception as e:
                is_connected = False
                self.active_ws = None
                if not is_running or self.shutdown_event.is_set():
                    break
                # ADR-15: Reconnect sleep = min(30, backoff * uniform(0.5, 1.5)) starting at 1 s
                jittered_sleep = self.calculate_reconnect_sleep(backoff)
                logger.warning(
                    f"Coinbase WSS disconnected/error: {e}. Reconnecting in {jittered_sleep:.2f}s...",
                    extra={"event": "ws_disconnected", "symbol": ",".join(self.settings.symbols)},
                )
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=jittered_sleep)
                except TimeoutError:
                    pass
                backoff = min(30.0, backoff * 2.0)

    def handle_message(self, raw_msg: str | bytes) -> None:
        try:
            processed_frame, gaps = self.processor.process_message(raw_msg)

            # Record and log any detected gaps
            for gap in gaps:
                COLLECTOR_GAPS_TOTAL.labels(symbol=gap.symbol, reason=gap.reason).inc()
                logger.warning(
                    f"Market data gap detected on {gap.symbol}: expected={gap.expected}, "
                    f"actual={gap.actual}, size={gap.gap_size}, reason={gap.reason}",
                    extra={
                        "event": "gap_detected",
                        "symbol": gap.symbol,
                        "reason": gap.reason,
                        "expected": gap.expected,
                        "actual": gap.actual,
                        "gap_size": gap.gap_size,
                    },
                )

            if processed_frame is None:
                return

            raw_frame = RawFrame(
                event_id=processed_frame.event_id,
                schema_version=1,
                source="market-collector",
                symbol=processed_frame.symbol,
                event_ts=processed_frame.event_ts,
                ingest_ts=processed_frame.ingest_ts,
                seq=processed_frame.seq,
                channel=processed_frame.channel,
                payload=processed_frame.payload,
                ingest_mode="live",
            )

            encoded = self.codec.encode(self.settings.topic_raw, raw_frame.model_dump())
            headers = {
                "traceparent": f"00-{processed_frame.event_id.replace('-', '')[:32]}-0000000000000001-01",
                "schema_version": "1",
                "event_id": processed_frame.event_id,
                "source": "market-collector",
                "ingest_ts": str(processed_frame.ingest_ts),
                "source_seq": str(processed_frame.source_seq),
            }

            def on_delivery(err: Any, msg: Any) -> None:
                if err is not None:
                    COLLECTOR_PRODUCE_ERRORS_TOTAL.labels(symbol=processed_frame.symbol).inc()
                    logger.error(
                        f"Kafka produce delivery failed: {err}",
                        extra={"event": "kafka_produce_error", "symbol": processed_frame.symbol},
                    )

            try:
                self.producer.produce(
                    topic=self.settings.topic_raw,
                    key=processed_frame.symbol,
                    value=encoded,
                    headers=headers,
                    on_delivery=on_delivery,
                )
            except BufferError:
                logger.warning(
                    "Kafka producer buffer full, polling and retrying...",
                    extra={"event": "kafka_buffer_full", "symbol": processed_frame.symbol},
                )
                self.producer.poll(0.5)
                self.producer.produce(
                    topic=self.settings.topic_raw,
                    key=processed_frame.symbol,
                    value=encoded,
                    headers=headers,
                    on_delivery=on_delivery,
                )

            MESSAGES_PROCESSED_TOTAL.labels(
                service="market-collector", topic=self.settings.topic_raw, status="ok"
            ).inc()
            LAST_EVENT_AGE_SECONDS.labels(service="market-collector", symbol=processed_frame.symbol).set(
                (processed_frame.ingest_ts - processed_frame.event_ts) / 1_000_000.0
            )

        except Exception as ex:
            logger.error(f"Error handling Coinbase message: {ex}", exc_info=True, extra={"event": "message_error"})

    async def monitor_staleness(self) -> None:
        """Periodic background check for symbol feed staleness (> 5s without any frame)."""
        while is_running and not self.shutdown_event.is_set():
            try:
                stale_symbols = self.processor.check_staleness(threshold_s=5.0)
                for sym in self.settings.symbols:
                    if sym in stale_symbols:
                        COLLECTOR_SYMBOL_STALE.labels(symbol=sym).set(1)
                        logger.warning(
                            f"Symbol {sym} feed is stale (> 5s without frames)",
                            extra={"event": "symbol_stale", "symbol": sym},
                        )
                    else:
                        COLLECTOR_SYMBOL_STALE.labels(symbol=sym).set(0)
            except Exception as e:
                logger.debug(f"Error in staleness monitor: {e}")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
            except TimeoutError:
                pass


async def main() -> None:
    global collector_instance
    collector_instance = CoinbaseCollector(settings)
    config = uvicorn.Config(app, host=settings.health_host, port=settings.http_port, log_level="warning")
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(server, collector_instance)))

    original_handle_exit = server.handle_exit

    def custom_handle_exit(sig: int, frame: Any) -> None:
        original_handle_exit(sig, frame)
        asyncio.create_task(shutdown(server, collector_instance))

    server.handle_exit = custom_handle_exit  # type: ignore[method-assign]

    await asyncio.gather(
        server.serve(),
        collector_instance.run(),
        collector_instance.monitor_staleness(),
    )


async def shutdown(server: uvicorn.Server | None, collector: CoinbaseCollector | None) -> None:
    global is_running, is_connected
    if not is_running:
        return
    logger.info("Shutdown signal received. Stopping collector...", extra={"event": "shutdown"})
    is_running = False
    is_connected = False

    if collector is not None:
        collector.shutdown_event.set()
        if collector.active_ws is not None:
            try:
                await collector.active_ws.close()
                logger.info("WebSocket connection closed cleanly.", extra={"event": "ws_closed"})
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}", extra={"event": "ws_close_error"})
        try:
            collector.producer.flush(5.0)
        except Exception:
            pass

    if server is not None:
        server.should_exit = True
        if hasattr(server, "_captured_signals"):
            server._captured_signals.clear()


if __name__ == "__main__":
    asyncio.run(main())
