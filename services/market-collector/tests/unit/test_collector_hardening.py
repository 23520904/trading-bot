"""Tests proving market-collector hardening and C1 follow-ups (Task A0 / A0b).

Verifies:
1. Production function compute_reconnect_sleep stays in [0.5x, 1.5x] and capped at 30.0s.
2. BackoffManager: grows on repeated failures, capped at 30s, resets ONLY after >= 60s stability.
   Includes flapping connection test (connect -> frame -> drop repeated: backoff keeps growing <= 30s).
3. Clean SIGTERM subprocess exit in <= 30s with exit code 0 (native uvicorn lifespan shutdown).
4. Production fails fast without SCHEMA_REGISTRY_URL (when CIP_ENV != 'test').
5. Worst-case SIGTERM real subprocess test: Kafka on closed port + WS server ignoring close request.
   Asserts total exit <= 30s, exit code 0, and that flush timeout & WS close timeout fire.
6. Collector metrics follow consistent cip_<domain>_<name>_<unit> naming scheme.
7. Producer BufferError triggers poll(0.5) and retries successfully.
8. on_delivery error increments cip_collector_produce_errors_total.
"""

import asyncio
import base64
import hashlib
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
from market_collector.domain import (
    BackoffManager,
    compute_reconnect_sleep,
)
from market_collector.main import (
    CoinbaseCollector,
    CollectorSettings,
)
from market_collector.metrics import (
    COLLECTOR_PRODUCE_ERRORS_TOTAL,
)
from websockets.asyncio.server import ServerConnection

# ---------------------------------------------------------------------------
# 1. Production compute_reconnect_sleep function (ADR-15)
# ---------------------------------------------------------------------------


def test_production_compute_reconnect_sleep_statistical() -> None:
    """Production function compute_reconnect_sleep stays in [0.5x, 1.5x] and capped at 30.0s."""
    # Deterministic test with explicit uniform sample
    assert compute_reconnect_sleep(1.0, uniform_sample=0.5) == 0.5
    assert compute_reconnect_sleep(1.0, uniform_sample=1.0) == 1.0
    assert compute_reconnect_sleep(1.0, uniform_sample=1.5) == 1.5
    assert compute_reconnect_sleep(10.0, uniform_sample=1.5) == 15.0
    # Capped at 30.0s
    assert compute_reconnect_sleep(25.0, uniform_sample=1.5) == 30.0
    assert compute_reconnect_sleep(60.0, uniform_sample=1.0) == 30.0

    # Statistical test across various backoff tiers
    for backoff in [1.0, 2.0, 4.0, 8.0, 16.0, 20.0, 30.0, 45.0, 60.0, 120.0]:
        for _ in range(500):
            sleep_val = compute_reconnect_sleep(backoff)
            assert sleep_val <= 30.0, f"Sleep {sleep_val} exceeded 30.0s cap for backoff={backoff}"
            assert sleep_val >= 0.0, f"Sleep {sleep_val} was negative"
            if backoff * 1.5 <= 30.0:
                assert 0.5 * backoff <= sleep_val <= 1.5 * backoff


# ---------------------------------------------------------------------------
# 2. BackoffManager: growth, 30s cap, reset ONLY on >= 60s stability, flapping test
# ---------------------------------------------------------------------------


def test_backoff_manager_growth_cap_and_reset_only_on_stability() -> None:
    """BackoffManager grows on failures, caps at 30s, and resets ONLY after >= 60s stability."""
    fake_time = 1000.0

    def get_time() -> float:
        return fake_time

    mgr = BackoffManager(
        initial_backoff=1.0,
        max_backoff=30.0,
        stability_threshold_s=60.0,
        time_fn=get_time,
    )

    # Initial state
    assert mgr.current_backoff == 1.0

    # 1. Repeated disconnects -> exponential growth
    assert mgr.on_disconnect(uniform_sample=1.0) == 1.0
    assert mgr.current_backoff == 2.0

    assert mgr.on_disconnect(uniform_sample=1.0) == 2.0
    assert mgr.current_backoff == 4.0

    assert mgr.on_disconnect(uniform_sample=1.0) == 4.0
    assert mgr.current_backoff == 8.0

    assert mgr.on_disconnect(uniform_sample=1.0) == 8.0
    assert mgr.current_backoff == 16.0

    assert mgr.on_disconnect(uniform_sample=1.0) == 16.0
    assert mgr.current_backoff == 30.0

    # 2. Capped strictly at 30.0s
    assert mgr.on_disconnect(uniform_sample=1.0) == 30.0
    assert mgr.current_backoff == 30.0

    # 3. Connection established for < 60s (unstable) -> backoff must NOT reset
    mgr.on_connected()
    fake_time += 30.0  # Only 30s
    assert mgr.on_disconnect(uniform_sample=1.0) == 30.0
    assert mgr.current_backoff == 30.0

    # 4. Connection established for >= 60s (stable) -> backoff resets to 1.0s
    mgr.on_connected()
    fake_time += 65.0  # 65s >= 60s
    s_after_stability = mgr.on_disconnect(uniform_sample=1.0)
    assert s_after_stability == 1.0, f"Expected 1.0 after stability reset, got {s_after_stability}"
    assert mgr.current_backoff == 2.0


def test_backoff_manager_flapping_connection() -> None:
    """A flapping connection (connect -> frames -> drop, repeated) must keep growing and stay <= 30s.

    Receiving frames must NOT reset backoff; it resets ONLY after >= 60s continuous stability.
    """
    fake_time = 1000.0

    def get_time() -> float:
        return fake_time

    mgr = BackoffManager(
        initial_backoff=1.0,
        max_backoff=30.0,
        stability_threshold_s=60.0,
        time_fn=get_time,
    )

    assert mgr.current_backoff == 1.0

    # Flapping loop: connected for only 5s each time, then dropped
    expected_backoffs = [2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]
    for expected in expected_backoffs:
        mgr.on_connected()
        fake_time += 5.0  # 5s of connectivity (short/flapping)
        sleep_val = mgr.on_disconnect(uniform_sample=1.0)
        assert mgr.current_backoff == expected
        assert sleep_val <= 30.0

    # Finally connection stays stable for 65s (>= 60s)
    mgr.on_connected()
    fake_time += 65.0
    sleep_after_stable = mgr.on_disconnect(uniform_sample=1.0)
    assert sleep_after_stable == 1.0
    assert mgr.current_backoff == 2.0


# ---------------------------------------------------------------------------
# 3. Clean SIGTERM subprocess exit in <= 30s with exit code 0
# ---------------------------------------------------------------------------


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.asyncio
async def test_sigterm_subprocess_clean_exit() -> None:
    """SIGTERM sent to collector process terminates in <= 30s, closes WS cleanly, and exits with code 0."""
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
    env["CIP_ENV"] = "test"
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
        await asyncio.wait_for(client_connected_event.wait(), timeout=10.0)
        await asyncio.sleep(0.5)

        t0 = time.monotonic()
        proc.send_signal(signal.SIGTERM)

        exit_code = await asyncio.to_thread(proc.wait, timeout=30.0)
        elapsed = time.monotonic() - t0

        stdout_file.seek(0)
        stderr_file.seek(0)
        combined_logs = stdout_file.read() + "\n" + stderr_file.read()

        assert exit_code == 0, f"Process exited with {exit_code}, expected 0. Logs:\n{combined_logs}"
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
# 4. Production fails fast without SCHEMA_REGISTRY_URL
# ---------------------------------------------------------------------------


def test_production_fails_fast_without_schema_registry_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production environment (CIP_ENV != 'test') must fail fast if SCHEMA_REGISTRY_URL is empty."""
    monkeypatch.setenv("CIP_ENV", "production")
    settings = CollectorSettings(schema_registry_url="")

    with pytest.raises(ValueError, match="SCHEMA_REGISTRY_URL must be configured"):
        CoinbaseCollector(settings)


# ---------------------------------------------------------------------------
# 5. Worst-case SIGTERM as a REAL subprocess test (closed Kafka + hung WS close)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worst_case_sigterm_exit_subprocess() -> None:
    """Worst-case real subprocess test:

    - Kafka bootstrap points to closed port (127.0.0.1:59998).
    - WS server ignores client close request.
    - Proves bounded timeouts in code ensure total exit <= 30s, exit code 0,
      and that both the WS close timeout and flush timeout actually fire.
    """
    ws_port = get_free_port()
    http_port = get_free_port()
    closed_kafka_port = 59998

    client_connected_event = asyncio.Event()
    server_stop_event = asyncio.Event()

    async def raw_hanging_ws_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Accepts WebSocket handshake and then hangs, ignoring close frames until test ends."""
        try:
            data = await reader.read(4096)
            key = None
            for line in data.decode(errors="ignore").split("\r\n"):
                if line.lower().startswith("sec-websocket-key:"):
                    key = line.split(":", 1)[1].strip()
                    break
            if key:
                accept = base64.b64encode(
                    hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
                ).decode()
                resp = (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                )
                writer.write(resp.encode())
                await writer.drain()
                client_connected_event.set()

            # Ignore any close frames from the client; wait until test cleanup
            await server_stop_event.wait()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(raw_hanging_ws_server, "127.0.0.1", ws_port)

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
    env["KAFKA_BOOTSTRAP_SERVERS"] = f"127.0.0.1:{closed_kafka_port}"
    env["HTTP_PORT"] = str(http_port)
    env["HEALTH_HOST"] = "127.0.0.1"
    env["CIP_ENV"] = "test"
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
        # Wait for client to connect and finish handshake
        await asyncio.wait_for(client_connected_event.wait(), timeout=10.0)
        await asyncio.sleep(0.5)

        # Send SIGTERM to initiate graceful shutdown under worst-case conditions
        t0 = time.monotonic()
        proc.send_signal(signal.SIGTERM)

        exit_code = await asyncio.to_thread(proc.wait, timeout=30.0)
        elapsed = time.monotonic() - t0

        stdout_file.seek(0)
        stderr_file.seek(0)
        combined_logs = stdout_file.read() + "\n" + stderr_file.read()

        # Assertions per Task A0b.3:
        assert exit_code == 0, f"Worst-case process exited with {exit_code}, expected 0. Logs:\n{combined_logs}"
        assert elapsed <= 30.0, f"Worst-case shutdown took {elapsed:.2f}s, expected <= 30s"
        # Verify bounded timeouts in code actually fired:
        assert "ws_close_timeout" in combined_logs or "close timed out" in combined_logs, (
            f"Expected ws_close_timeout log in output:\n{combined_logs}"
        )
        assert "producer_flush" in combined_logs or "flushed" in combined_logs, (
            f"Expected producer flush handling log in output:\n{combined_logs}"
        )

    finally:
        server_stop_event.set()
        if proc.poll() is None:
            proc.kill()
        stdout_file.close()
        stderr_file.close()
        server.close()
        await server.wait_closed()


# ---------------------------------------------------------------------------
# 6. Collector metrics naming scheme: cip_<domain>_<name>_<unit>
# ---------------------------------------------------------------------------


def test_collector_metrics_naming_scheme() -> None:
    """All collector metrics must follow cip_collector_<name>_<unit> naming scheme."""
    from prometheus_client import generate_latest

    metrics_text = generate_latest().decode()
    expected_metrics = [
        "cip_collector_gaps_total",
        "cip_collector_produce_errors_total",
        "cip_collector_symbol_stale_bool",
        "cip_collector_messages_processed_total",
        "cip_collector_last_event_age_seconds",
    ]

    for metric_name in expected_metrics:
        assert f"# TYPE {metric_name}" in metrics_text, f"Metric {metric_name} not found in prometheus output"


# ---------------------------------------------------------------------------
# 7. BufferError and 8. on_delivery metric tests
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
    assert len(stub.produce_calls) == 2, f"Expected 2 produce calls, got {len(stub.produce_calls)}"
    assert stub.poll_calls == [0.5], f"Expected poll(0.5), got {stub.poll_calls}"


def test_on_delivery_error_increments_metric() -> None:
    """When on_delivery callback receives an error, cip_collector_produce_errors_total is incremented."""
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

    class FakeKafkaError(Exception):
        pass

    on_delivery_fn(FakeKafkaError("Broker disconnected"), None)

    after_val = metric._value.get()
    assert after_val == initial_val + 1, f"Metric did not increment: before={initial_val}, after={after_val}"
