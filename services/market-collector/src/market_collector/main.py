"""market-collector service:

- Subscribes to Coinbase WebSocket feed (channels: matches, heartbeat) for configured symbols.
- Domain processor handles trade_id contiguity gap detection and heartbeat tail-loss detection (ADR-14).
- Serializes frames with Avro (RawFrame.avsc) in Confluent wire format (ADR-13).
- Publishes to Kafka topic market.raw.v1 with key=symbol.
- Structured logging with explicit event and symbol fields.
- Reconnect backoff with uniform jitter and reset after stability or valid frame (ADR-15, Task A0b).
- Clean SIGTERM shutdown with application-level signal handling and bounded timeouts <= 30s (Task A0c, A0e).
- /readyz verifies BOTH WebSocket connection and Kafka producer health.
"""

import asyncio
import json
import os
import signal
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
import websockets
from cip_core import RawFrame
from cip_kafka import CIPKafkaProducer, ConfluentAvroCodec, SchemaRegistryClient
from cip_obs import setup_logging
from fastapi import FastAPI, Response
from pydantic_settings import BaseSettings

from .domain import BackoffManager, MarketDataProcessor, compute_reconnect_sleep
from .metrics import (
    COLLECTOR_GAPS_TOTAL,
    COLLECTOR_LAST_EVENT_AGE_SECONDS,
    COLLECTOR_MESSAGES_PROCESSED_TOTAL,
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
    global collector_instance
    if collector_instance is None:
        collector_instance = CoinbaseCollector(settings)

    collector_task = asyncio.create_task(collector_instance.run())
    staleness_task = asyncio.create_task(collector_instance.monitor_staleness())
    try:
        yield
    finally:
        # Task A0b: Uvicorn handles SIGTERM/SIGINT and triggers lifespan shutdown:
        # (1) stop receive loop -> (2) close WS (timeout 3s) -> (3) flush producer (bounded) -> exit
        await collector_instance.stop()
        collector_task.cancel()
        staleness_task.cancel()
        await asyncio.gather(collector_task, staleness_task, return_exceptions=True)


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


def flush_producer_bounded(producer: Any, timeout: float = 5.0) -> int:
    """Flush producer in a daemon thread so a hung flush cannot block process shutdown."""
    result = [-1]

    def _flush_worker() -> None:
        try:
            result[0] = producer.flush(timeout)
        except Exception:
            pass

    t = threading.Thread(target=_flush_worker, daemon=True)
    t.start()
    t.join(timeout=timeout + 0.5)
    return result[0]


class CoinbaseCollector:
    def __init__(self, settings: CollectorSettings):
        self.settings = settings
        self.processor = MarketDataProcessor(settings.symbols)
        self.backoff_mgr = BackoffManager()
        self.producer = CIPKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="market-collector",
        )
        if not settings.schema_registry_url:
            # Task A0d: In-memory registry is strictly a test-only helper (CIP_ENV=test)
            if os.getenv("CIP_ENV") == "test":
                self.schema_registry_client = SchemaRegistryClient.in_memory()
            else:
                raise ValueError(
                    "SCHEMA_REGISTRY_URL must be configured. In-memory Schema Registry is test-only (CIP_ENV=test)."
                )
        else:
            self.schema_registry_client = SchemaRegistryClient(settings.schema_registry_url)
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
        return compute_reconnect_sleep(backoff)

    async def run(self) -> None:
        global is_running, is_connected
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": self.settings.symbols,
            "channels": ["matches", "heartbeat"],
        }

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
                    close_timeout=3.0,
                ) as ws:
                    self.active_ws = ws
                    is_connected = True
                    self.backoff_mgr.on_connected()
                    logger.info(
                        "Connected to Coinbase WSS. Sending subscription...",
                        extra={"event": "ws_connected", "symbol": ",".join(self.settings.symbols)},
                    )
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw_msg in ws:
                        if not is_running or self.shutdown_event.is_set():
                            break
                        self.handle_message(raw_msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                is_connected = False
                self.active_ws = None
                if not is_running or self.shutdown_event.is_set():
                    break
                # ADR-15, Task A0b: BackoffManager handles jittered sleep and reset on stability
                jittered_sleep = self.backoff_mgr.on_disconnect()
                logger.warning(
                    f"Coinbase WSS disconnected/error: {e}. Reconnecting in {jittered_sleep:.2f}s...",
                    extra={"event": "ws_disconnected", "symbol": ",".join(self.settings.symbols)},
                )
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=jittered_sleep)
                except TimeoutError:
                    pass

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

            COLLECTOR_MESSAGES_PROCESSED_TOTAL.labels(
                service="market-collector", topic=self.settings.topic_raw, status="ok"
            ).inc()
            COLLECTOR_LAST_EVENT_AGE_SECONDS.labels(service="market-collector", symbol=processed_frame.symbol).set(
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
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Error in staleness monitor: {e}")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
            except TimeoutError:
                pass

    async def stop(self) -> None:
        """Perform graceful shutdown: stop loop, close WS (timeout 3s), flush producer (bounded)."""
        global is_running, is_connected
        if not is_running:
            return
        logger.info("Shutdown signal received. Stopping collector...", extra={"event": "shutdown"})
        is_running = False
        is_connected = False

        # 1. Stop receive loop
        self.shutdown_event.set()

        # 2. Close WebSocket with bounded timeout (3.0s)
        if self.active_ws is not None:
            try:
                await asyncio.wait_for(self.active_ws.close(), timeout=3.0)
                logger.info("WebSocket connection closed cleanly.", extra={"event": "ws_closed"})
            except TimeoutError:
                logger.warning(
                    "WebSocket close timed out after 3.0s, proceeding with shutdown",
                    extra={"event": "ws_close_timeout"},
                )
                try:
                    if hasattr(self.active_ws, "transport") and self.active_ws.transport is not None:
                        self.active_ws.transport.close()
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}", extra={"event": "ws_close_error"})

        # 3. Flush producer with bounded timeout (5.0s in daemon thread)
        try:
            rem = await asyncio.to_thread(flush_producer_bounded, self.producer, 5.0)
            if rem > 0:
                logger.warning(
                    f"Kafka producer flush timed out with {rem} message(s) un-delivered.",
                    extra={"event": "producer_flush_timeout", "remaining": rem},
                )
            elif rem == 0:
                logger.info("Kafka producer flushed cleanly.", extra={"event": "producer_flushed"})
            else:
                logger.warning(
                    "Kafka producer flush exceeded timeout boundary, proceeding with shutdown",
                    extra={"event": "producer_flush_timeout"},
                )
        except Exception as e:
            logger.warning(f"Error flushing producer: {e}", extra={"event": "producer_flush_error"})


async def shutdown(server: uvicorn.Server | None = None, collector: CoinbaseCollector | None = None) -> None:
    """Graceful shutdown helper."""
    if collector is not None:
        await collector.stop()
    if server is not None:
        server.should_exit = True


async def main() -> None:
    global collector_instance
    import sys

    # Ensure clean exit code 0 when uvicorn re-raises signal after graceful shutdown
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    collector_instance = CoinbaseCollector(settings)
    config = uvicorn.Config(app, host=settings.health_host, port=settings.http_port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
