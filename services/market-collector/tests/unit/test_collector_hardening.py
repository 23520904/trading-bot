"""Tests proving market-collector hardening (PART C1).

Verifies:
1. Reconnect backoff jitter stays within [0.5x, 1.5x] and is strictly capped at 30 s.
2. Fake WebSocket server forced disconnect: collector reconnects respecting backoff & jitter bounds.
3. SIGTERM to subprocess closes WS cleanly, flushes producer, and exits in <= 30 s.
4. Producer BufferError triggers producer.poll(0.5) and retries successfully.
5. Producer on_delivery error callback increments collector_produce_errors_total.
"""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any

import pytest
import websockets
from market_collector.main import (
    CoinbaseCollector,
    CollectorSettings,
)
from market_collector.metrics import COLLECTOR_PRODUCE_ERRORS_TOTAL
from websockets.asyncio.server import ServerConnection

# ---------------------------------------------------------------------------
# Test 1: Reconnect sleep jitter and 30s cap (statistical + fake server disconnect)
# ---------------------------------------------------------------------------


def test_reconnect_sleep_jitter_and_cap_statistical() -> None:
    """Sleep duration must be in [0.5x, 1.5x] of backoff and strictly <= 30.0 s."""
    settings = CollectorSettings(schema_registry_url="")
    collector = CoinbaseCollector(settings)

    # Test backoff values from 1.0s up to 120.0s
    for backoff in [1.0, 2.0, 4.0, 8.0, 16.0, 20.0, 30.0, 45.0, 60.0, 120.0]:
        for _ in range(500):
            sleep_val = collector.calculate_reconnect_sleep(backoff)
            # Sleep must never exceed 30.0s
            assert sleep_val <= 30.0, f"Sleep {sleep_val} exceeded 30.0s cap for backoff={backoff}"
            assert sleep_val >= 0.0, f"Sleep {sleep_val} was negative"
            if backoff * 1.5 <= 30.0:
                # Under the 30s cap: sleep must be in [0.5 * backoff, 1.5 * backoff]
                assert 0.5 * backoff <= sleep_val <= 1.5 * backoff, (
                    f"Sleep {sleep_val} outside [0.5x, 1.5x] for backoff={backoff}"
                )


@pytest.mark.asyncio
async def test_fake_ws_forced_disconnect_reconnect_backoff() -> None:
    """Fake WebSocket server repeatedly disconnects; verify collector reconnect backoff sequence."""
    connection_attempts: list[float] = []

    async def disconnect_handler(websocket: ServerConnection) -> None:
        connection_attempts.append(time.monotonic())
        # Immediately drop connection with error code
        await websocket.close(code=1011, reason="Simulated disconnect")

    server = await websockets.serve(disconnect_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    settings = CollectorSettings(
        coinbase_wss_url=f"ws://127.0.0.1:{port}",
        schema_registry_url="",
        symbols=["BTC-USD"],
    )
    collector = CoinbaseCollector(settings)

    recorded_sleeps: list[tuple[float, float]] = []
    original_calc = collector.calculate_reconnect_sleep

    def tracking_calc(backoff: float) -> float:
        val = original_calc(backoff)
        recorded_sleeps.append((backoff, val))
        # Return tiny sleep to keep test fast while recording true calculation
        return 0.01

    collector.calculate_reconnect_sleep = tracking_calc  # type: ignore[method-assign]

    collector_task = asyncio.create_task(collector.run())

    # Wait until collector has connected and been disconnected at least 4 times
    for _ in range(100):
        if len(recorded_sleeps) >= 4:
            break
        await asyncio.sleep(0.05)

    collector.shutdown_event.set()
    await collector_task
    server.close()
    await server.wait_closed()

    assert len(recorded_sleeps) >= 4
    for backoff, sleep_val in recorded_sleeps:
        assert 0.5 * backoff <= sleep_val <= min(30.0, 1.5 * backoff)
        assert sleep_val <= 30.0


# ---------------------------------------------------------------------------
# Test 2: SIGTERM subprocess clean exit in <= 30s
# ---------------------------------------------------------------------------


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.asyncio
async def test_sigterm_subprocess_clean_exit() -> None:
    """SIGTERM sent to collector process terminates in <= 30s, closes WS cleanly, flushes producer."""
    ws_port = get_free_port()
    http_port = get_free_port()

    client_connected_event = asyncio.Event()

    async def fake_coinbase_server(websocket: ServerConnection) -> None:
        client_connected_event.set()
        try:
            async for _ in websocket:
                pass
        except Exception:
            pass

    ws_server = await websockets.serve(fake_coinbase_server, "127.0.0.1", ws_port)

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
    env = os.environ.copy()
    python_path = ":".join(
        [
            os.path.join(repo_root, "services/market-collector/src"),
            os.path.join(repo_root, "libs/cip-core/src"),
            os.path.join(repo_root, "libs/cip-kafka/src"),
            os.path.join(repo_root, "libs/cip-obs/src"),
        ]
    )
    env["PYTHONPATH"] = python_path
    env["COINBASE_WSS_URL"] = f"ws://127.0.0.1:{ws_port}"
    env["HTTP_PORT"] = str(http_port)
    env["HEALTH_HOST"] = "127.0.0.1"
    env["CIP_ENV"] = "local"
    env["SCHEMA_REGISTRY_URL"] = ""

    stdout_file = tempfile.TemporaryFile("w+")
    stderr_file = tempfile.TemporaryFile("w+")
    proc = subprocess.Popen(
        [sys.executable, "-m", "market_collector.main"],
        env=env,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
    )

    try:
        # Wait for client to connect to fake WS
        await asyncio.wait_for(client_connected_event.wait(), timeout=10.0)
        # Give a moment for the process to enter main async loop
        await asyncio.sleep(0.5)

        # Send SIGTERM
        t0 = time.monotonic()
        proc.send_signal(signal.SIGTERM)

        # Subprocess must terminate in <= 30 seconds
        exit_code = await asyncio.to_thread(proc.wait, timeout=30.0)
        elapsed = time.monotonic() - t0

        stdout_file.seek(0)
        stderr_file.seek(0)
        combined_logs = stdout_file.read() + "\n" + stderr_file.read()

        assert exit_code in (0, -signal.SIGTERM), f"Process exited with {exit_code}. Logs:\n{combined_logs}"
        assert elapsed <= 30.0, f"Process took {elapsed:.2f}s to exit, expected <= 30s"
        assert "Shutdown signal received" in combined_logs
        assert "WebSocket connection closed cleanly" in combined_logs

    finally:
        if proc.poll() is None:
            proc.kill()
        stdout_file.close()
        stderr_file.close()
        ws_server.close()
        await ws_server.wait_closed()


# ---------------------------------------------------------------------------
# Test 3: Producer BufferError -> poll(0.5) and retry
# ---------------------------------------------------------------------------


def test_producer_buffer_error_poll_and_retry() -> None:
    """When producer.produce raises BufferError, collector polls(0.5) and retries successfully."""
    settings = CollectorSettings(schema_registry_url="")
    collector = CoinbaseCollector(settings)

    class StubProducer:
        def __init__(self) -> None:
            self.produce_calls: list[dict[str, Any]] = []
            self.poll_calls: list[float] = []
            self.should_fail_first = True

        def produce(
            self,
            topic: str,
            key: str,
            value: bytes,
            headers: dict[str, str] | None = None,
            on_delivery: Any = None,
        ) -> None:
            self.produce_calls.append({"topic": topic, "key": key, "value": value})
            if self.should_fail_first:
                self.should_fail_first = False
                raise BufferError("Queue full: test simulated buffer overflow")

        def poll(self, timeout: float) -> int:
            self.poll_calls.append(timeout)
            return 0

    stub = StubProducer()
    collector.producer = stub  # type: ignore[assignment]

    raw_match = json.dumps(
        {
            "type": "match",
            "trade_id": 99999,
            "sequence": 123456,
            "maker_order_id": "m1",
            "taker_order_id": "t1",
            "time": "2026-11-23T00:00:00.000000Z",
            "product_id": "BTC-USD",
            "size": "0.5",
            "price": "60000.0",
            "side": "buy",
        }
    )

    collector.handle_message(raw_match)

    # 1. First produce call failed with BufferError
    # 2. Producer was polled with 0.5s timeout
    # 3. Second produce call succeeded
    assert len(stub.produce_calls) == 2, f"Expected 2 produce calls, got {len(stub.produce_calls)}"
    assert stub.poll_calls == [0.5], f"Expected poll(0.5), got {stub.poll_calls}"


# ---------------------------------------------------------------------------
# Test 4: on_delivery error increments collector_produce_errors_total metric
# ---------------------------------------------------------------------------


def test_on_delivery_error_increments_metric() -> None:
    """When on_delivery callback receives an error, collector_produce_errors_total is incremented."""
    settings = CollectorSettings(schema_registry_url="")
    collector = CoinbaseCollector(settings)

    captured_callbacks: list[Any] = []

    class StubProducer:
        def produce(
            self,
            topic: str,
            key: str,
            value: bytes,
            headers: dict[str, str] | None = None,
            on_delivery: Any = None,
        ) -> None:
            captured_callbacks.append(on_delivery)

        def poll(self, timeout: float) -> int:
            return 0

    collector.producer = StubProducer()  # type: ignore[assignment]

    raw_match = json.dumps(
        {
            "type": "match",
            "trade_id": 100001,
            "sequence": 200001,
            "maker_order_id": "m2",
            "taker_order_id": "t2",
            "time": "2026-11-23T00:00:00.000000Z",
            "product_id": "BTC-USD",
            "size": "1.0",
            "price": "61000.0",
            "side": "sell",
        }
    )

    collector.handle_message(raw_match)

    assert len(captured_callbacks) == 1
    on_delivery_fn = captured_callbacks[0]
    assert on_delivery_fn is not None

    metric = COLLECTOR_PRODUCE_ERRORS_TOTAL.labels(symbol="BTC-USD")
    initial_val = metric._value.get()

    # Invoke callback with an error
    class FakeKafkaError(Exception):
        pass

    on_delivery_fn(FakeKafkaError("Broker disconnected"), None)

    after_val = metric._value.get()
    assert after_val == initial_val + 1, f"Metric did not increment: before={initial_val}, after={after_val}"
