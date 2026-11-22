"""Unit tests for market-collector replaying recorded Coinbase raw frames fixture."""

import json
from pathlib import Path

from market_collector.domain import GapEvent, MarketDataProcessor, ProcessedFrame

FIXTURE_PATH = Path("tests/fixtures/coinbase_raw_frames.json")


def test_replay_coinbase_recorded_fixture() -> None:
    """Replay 20 live-captured Coinbase frames through MarketDataProcessor.

    Verifies subscriptions handling, last_match baseline initialization,
    match frame extraction, and heartbeat tail-loss continuity.
    """
    assert FIXTURE_PATH.exists(), f"Fixture file not found: {FIXTURE_PATH}"
    frames_data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert len(frames_data) == 20

    processor = MarketDataProcessor(["BTC-USD"])
    processed_count = 0
    all_gaps: list[GapEvent] = []

    for frame in frames_data:
        raw_bytes = json.dumps(frame).encode("utf-8")
        proc_frame, gaps = processor.process_message(raw_bytes)
        all_gaps.extend(gaps)

        if proc_frame is not None:
            processed_count += 1
            assert isinstance(proc_frame, ProcessedFrame)
            assert proc_frame.symbol == "BTC-USD"
            assert proc_frame.seq > 0
            assert len(proc_frame.event_id) == 36
            assert proc_frame.event_ts > 0
            assert proc_frame.ingest_ts > 0

    # In clean contiguous capture, there should be zero gaps
    assert len(all_gaps) == 0, f"Expected 0 gaps in clean live fixture, got: {all_gaps}"
    assert processed_count > 0, "Must have processed match frames"


def test_market_processor_injected_trade_gap() -> None:
    """Verify that an omitted match message is immediately detected as a trade gap."""
    processor = MarketDataProcessor(["BTC-USD"])

    # Trade 100 arrives
    msg1 = {"type": "match", "product_id": "BTC-USD", "trade_id": 100, "sequence": 1000}
    _, gaps1 = processor.process_message(json.dumps(msg1))
    assert len(gaps1) == 0

    # Trade 101 is dropped over the wire; trade 102 arrives
    msg2 = {"type": "match", "product_id": "BTC-USD", "trade_id": 102, "sequence": 1020}
    proc_frame, gaps2 = processor.process_message(json.dumps(msg2))

    assert len(gaps2) == 1
    gap = gaps2[0]
    assert gap.symbol == "BTC-USD"
    assert gap.reason == "trade_gap"
    assert gap.expected == 101
    assert gap.actual == 102
    assert gap.gap_size == 2
    assert proc_frame is not None


def test_market_processor_heartbeat_tail_loss() -> None:
    """Verify that heartbeat.last_trade_id detects tail loss when no subsequent match arrives."""
    processor = MarketDataProcessor(["BTC-USD"])

    # Trade 500 arrives
    msg_match = {"type": "match", "product_id": "BTC-USD", "trade_id": 500, "sequence": 5000}
    processor.process_message(json.dumps(msg_match))

    # Heartbeat arrives reporting last_trade_id 502 (meaning trades 501 and 502 were lost)
    msg_hb = {"type": "heartbeat", "product_id": "BTC-USD", "last_trade_id": 502, "sequence": 5025}
    _, gaps = processor.process_message(json.dumps(msg_hb))

    assert len(gaps) == 1
    gap = gaps[0]
    assert gap.symbol == "BTC-USD"
    assert gap.reason == "heartbeat_tail_loss"
    assert gap.expected == 501
    assert gap.actual == 502
    assert gap.gap_size == 2


def test_market_processor_symbol_staleness() -> None:
    """Verify that check_staleness identifies symbols exceeding the 5s threshold (ADR-15)."""
    processor = MarketDataProcessor(["BTC-USD", "ETH-USD"])

    # Update BTC-USD at t=100.0
    processor.process_message(json.dumps({"type": "heartbeat", "product_id": "BTC-USD", "sequence": 1}))

    # At t=102.0, BTC-USD is not stale (2s elapsed < 5s)
    stale_2s = processor.check_staleness(now=processor.last_frame_ts["BTC-USD"] + 2.0, threshold_s=5.0)
    assert "BTC-USD" not in stale_2s

    # At t=106.0, BTC-USD is stale (6s elapsed > 5s)
    stale_6s = processor.check_staleness(now=processor.last_frame_ts["BTC-USD"] + 6.0, threshold_s=5.0)
    assert "BTC-USD" in stale_6s
