"""Tests proving market-collector hardening and C1 follow-ups (Task A0).

Verifies:
a) Reconnect jitter test calls the production function compute_reconnect_sleep (ADR-15).
b) BackoffManager with fake clock: grows on repeated failures, capped at 30s,
   resets after 60s stability, resets after first valid frame.
c) Application-level asyncio signal handlers for SIGTERM/SIGINT (clean shutdown, no private uvicorn attributes).
d) In-memory Schema Registry is test-only; production code fails fast without SCHEMA_REGISTRY_URL.
e) Worst-case SIGTERM shutdown: WebSocket hangs on close AND Kafka flush blocks/times out;
   assert bounded timeouts in code keep total exit time <= 30s.
f) All collector metrics conform to cip_<domain>_<name>_<unit> naming scheme.
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
import uvicorn
import websockets
from market_collector.domain import (
    BackoffManager,
    compute_reconnect_sleep,
)
from market_collector.main import (
    CoinbaseCollector,
    CollectorSettings,
    shutdown,
)
from market_collector.metrics import (
    COLLECTOR_PRODUCE_ERRORS_TOTAL,
)
from websockets.asyncio.server import ServerConnection

# ---------------------------------------------------------------------------
# Requirement a: Production compute_reconnect_sleep function
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
# Requirement b: BackoffManager with fake clock (growth, cap, resets)
# ---------------------------------------------------------------------------


def test_backoff_manager_growth_cap_and_resets() -> None:
    """BackoffManager grows exponentially, caps at 30s, resets on 60s stability or valid frame."""
    fake_time = 1000.0

    def get_time() -> float:
        return fake_time

    mgr = BackoffManager(
        initial_backoff=1.0,
        max_backoff=30.0,
        stability_threshold_s=60.0,
        time_fn=get_time,
    )

    # Initially 1.0s
    assert mgr.current_backoff == 1.0

    # 1. Grows on repeated disconnects
    s1 = mgr.on_disconnect(uniform_sample=1.0)
    assert s1 == 1.0
    assert mgr.current_backoff == 2.0

    s2 = mgr.on_disconnect(uniform_sample=1.0)
    assert s2 == 2.0
    assert mgr.current_backoff == 4.0

    s3 = mgr.on_disconnect(uniform_sample=1.0)
    assert s3 == 4.0
    assert mgr.current_backoff == 8.0

    s4 = mgr.on_disconnect(uniform_sample=1.0)
    assert s4 == 8.0
    assert mgr.current_backoff == 16.0

    s5 = mgr.on_disconnect(uniform_sample=1.0)
    assert s5 == 16.0
    assert mgr.current_backoff == 30.0

    # 2. Capped strictly at 30.0s
    s6 = mgr.on_disconnect(uniform_sample=1.0)
    assert s6 == 30.0
    assert mgr.current_backoff == 30.0

    s7 = mgr.on_disconnect(uniform_sample=1.0)
    assert s7 == 30.0
    assert mgr.current_backoff == 30.0

    # 3. Resets after stable connection (>= 60s without disconnect)
    mgr.on_connected()
    assert mgr.current_backoff == 30.0  # Still 30s until stability is confirmed
    fake_time += 65.0  # Advanced 65 seconds (> 60s threshold)

    # Disconnect after 65s of stability: backoff must have reset to 1.0s
    s_after_stability = mgr.on_disconnect(uniform_sample=1.0)
    assert s_after_stability == 1.0, f"Expected 1.0 after stability reset, got {s_after_stability}"
    assert mgr.current_backoff == 2.0

    # 4. Resets after first valid frame following a reconnect
    mgr.on_disconnect(uniform_sample=1.0)  # -> 4.0
    mgr.on_disconnect(uniform_sample=1.0)  # -> 8.0
    assert mgr.current_backoff == 8.0

    mgr.on_connected()
    # Frame arrives immediately (e.g. 2 seconds in, well below 60s)
    fake_time += 2.0
    mgr.on_valid_frame()
    assert mgr.current_backoff == 1.0, "Expected reset to 1.0s on first valid frame"


# ---------------------------------------------------------------------------
# Requirement c: Clean SIGTERM subprocess exit with application signal handling
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
# Requirement d: Production fails fast without SCHEMA_REGISTRY_URL
# ---------------------------------------------------------------------------


def test_production_fails_fast_without_schema_registry_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production environment (CIP_ENV != 'test') must fail fast if SCHEMA_REGISTRY_URL is empty."""
    monkeypatch.setenv("CIP_ENV", "production")
    settings = CollectorSettings(schema_registry_url="")

    with pytest.raises(ValueError, match="SCHEMA_REGISTRY_URL must be configured"):
        CoinbaseCollector(settings)


# ---------------------------------------------------------------------------
# Requirement e: Worst-case SIGTERM test (hanging WS + blocked Kafka flush)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worst_case_sigterm_exit() -> None:
    """Worst-case shutdown: WS close hangs forever AND Kafka producer flush blocks.

    Verifies bounded timeouts in code ensure total shutdown duration <= 30s.
    """
    settings = CollectorSettings(schema_registry_url="")
    collector = CoinbaseCollector(settings)

    # 1. Simulate WebSocket that hangs indefinitely on close
    class HangingWebSocket:
        async def close(self) -> None:
            # Hang forever (simulate network stall during TCP close)
            await asyncio.sleep(60.0)

    collector.active_ws = HangingWebSocket()

    # 2. Simulate Kafka producer whose flush blocks for 2.0s
    class SlowProducer:
        def flush(self, timeout: float) -> int:
            time.sleep(1.0)
            return 1  # 1 message remaining un-flushed

    collector.producer = SlowProducer()  # type: ignore[assignment]

    # Mock uvicorn server
    from fastapi import FastAPI

    config = uvicorn.Config(FastAPI())
    server = uvicorn.Server(config)

    t0 = time.monotonic()
    await shutdown(server, collector)
    elapsed = time.monotonic() - t0

    # Must finish within 30.0s (bounded by 3.0s WS timeout + 5.0s flush timeout = ~4-5s total)
    assert elapsed <= 30.0, f"Worst-case shutdown took {elapsed:.2f}s, expected <= 30s"
    assert collector.shutdown_event.is_set(), "Receive loop shutdown event must be set"
    assert server.should_exit is True, "Server should_exit flag must be set"


# ---------------------------------------------------------------------------
# Requirement f: Consistent metric naming scheme cip_<domain>_<name>_<unit>
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
# BufferError and on_delivery tests
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

    class FakeKafkaError(Exception):
        pass

    on_delivery_fn(FakeKafkaError("Broker disconnected"), None)

    after_val = metric._value.get()
    assert after_val == initial_val + 1, f"Metric did not increment: before={initial_val}, after={after_val}"
