"""I/O-free domain logic for market-collector:

- Coinbase WebSocket message parsing and normalization
- Trade gap detection via trade_id contiguity (ADR-14)
- Tail loss detection via heartbeat.last_trade_id (ADR-14)
- Symbol staleness monitoring (ADR-15)
- Internal sequence number management (Page 07)
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cip_core import datetime_to_micros, generate_uuid7, now_utc_micros


@dataclass(frozen=True)
class GapEvent:
    symbol: str
    reason: str  # "trade_gap" or "heartbeat_tail_loss"
    expected: int
    actual: int
    gap_size: int
    ts: int


@dataclass(frozen=True)
class ProcessedFrame:
    event_id: str
    symbol: str
    event_ts: int
    ingest_ts: int
    seq: int
    channel: str
    payload: bytes
    source_seq: int


class MarketDataProcessor:
    """Pure domain processor for WebSocket market frames with gap and staleness detection."""

    def __init__(self, symbols: list[str]):
        self.symbols = set(symbols)
        self.seq_map: dict[str, int] = {}
        self.last_seen_trade_id: dict[str, int] = {}
        self.last_frame_ts: dict[str, float] = {}

    def get_next_seq(self, symbol: str) -> int:
        seq = self.seq_map.get(symbol, 0) + 1
        self.seq_map[symbol] = seq
        return seq

    def parse_event_time(self, time_val: str | None) -> int:
        if not time_val:
            return now_utc_micros()
        try:
            dt = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
            return datetime_to_micros(dt)
        except Exception:
            return now_utc_micros()

    def process_message(self, raw_msg: str | bytes) -> tuple[ProcessedFrame | None, list[GapEvent]]:
        """Process incoming raw message string or bytes, returning processed frame and any detected gaps."""
        if isinstance(raw_msg, str):
            payload_bytes = raw_msg.encode("utf-8")
            obj: dict[str, Any] = json.loads(raw_msg)
        else:
            payload_bytes = raw_msg
            obj = json.loads(raw_msg.decode("utf-8"))

        msg_type = obj.get("type")
        symbol = obj.get("product_id") or "BTC-USD"
        now_ts = time.time()
        self.last_frame_ts[symbol] = now_ts
        gaps: list[GapEvent] = []

        # 1. Handle subscriptions handshake
        if msg_type == "subscriptions":
            return None, []

        # 2. Handle initial last_match (establishes baseline prior trade without emitting duplicate)
        if msg_type == "last_match":
            if "trade_id" in obj:
                tid = int(obj["trade_id"])
                self.last_seen_trade_id[symbol] = tid
            return None, []

        # 3. Handle heartbeat channel (tail loss detection via last_trade_id)
        if msg_type == "heartbeat":
            hb_trade_id = obj.get("last_trade_id")
            if hb_trade_id is not None and symbol in self.last_seen_trade_id:
                last_tid = self.last_seen_trade_id[symbol]
                hb_tid = int(hb_trade_id)
                if hb_tid > last_tid:
                    gap_size = hb_tid - last_tid
                    gaps.append(
                        GapEvent(
                            symbol=symbol,
                            reason="heartbeat_tail_loss",
                            expected=last_tid + 1,
                            actual=hb_tid,
                            gap_size=gap_size,
                            ts=now_utc_micros(),
                        )
                    )
                    self.last_seen_trade_id[symbol] = hb_tid
            elif hb_trade_id is not None:
                self.last_seen_trade_id[symbol] = int(hb_trade_id)
            return None, gaps

        # 4. Handle trade match and ticker updates
        if msg_type in ("match", "ticker"):
            if "trade_id" in obj:
                tid = int(obj["trade_id"])
                if symbol in self.last_seen_trade_id:
                    expected = self.last_seen_trade_id[symbol] + 1
                    if tid > expected:
                        gap_size = tid - expected + 1
                        gaps.append(
                            GapEvent(
                                symbol=symbol,
                                reason="trade_gap",
                                expected=expected,
                                actual=tid,
                                gap_size=gap_size,
                                ts=now_utc_micros(),
                            )
                        )
                self.last_seen_trade_id[symbol] = tid

            seq = self.get_next_seq(symbol)
            event_id = generate_uuid7()
            source_seq = int(obj.get("sequence", 0))
            event_ts = self.parse_event_time(obj.get("time"))
            ingest_ts = now_utc_micros()

            frame = ProcessedFrame(
                event_id=event_id,
                symbol=symbol,
                event_ts=event_ts,
                ingest_ts=ingest_ts,
                seq=seq,
                channel=msg_type,
                payload=payload_bytes,
                source_seq=source_seq,
            )
            return frame, gaps

        return None, []

    def check_staleness(self, now: float | None = None, threshold_s: float = 5.0) -> list[str]:
        """Return list of symbols that have received no frame for > threshold_s (ADR-15)."""
        current_time = now if now is not None else time.time()
        stale_symbols: list[str] = []
        for sym in self.symbols:
            last_ts = self.last_frame_ts.get(sym)
            if last_ts is not None and (current_time - last_ts) > threshold_s:
                stale_symbols.append(sym)
        return stale_symbols
