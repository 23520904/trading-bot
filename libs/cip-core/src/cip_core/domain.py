"""cip-core: Core domain types, UUIDv7 generation, timestamp helpers, and Avro schema codecs."""

import io
import json
import os
import threading
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import fastavro
from pydantic import BaseModel, ConfigDict, Field

_uuid7_lock = threading.Lock()
_last_v7_timestamp_ms: int = 0
_v7_counter: int = 0


def generate_uuid7() -> str:
    """Generate an RFC 9562 compliant UUIDv7 with sub-millisecond monotonicity."""
    global _last_v7_timestamp_ms, _v7_counter

    if hasattr(uuid, "uuid7"):
        return str(uuid.uuid7())  # type: ignore[attr-defined,unused-ignore]

    with _uuid7_lock:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        if now_ms > _last_v7_timestamp_ms:
            _last_v7_timestamp_ms = now_ms
            # Start counter with random 10-bit integer to allow headroom for 12-bit counter
            _v7_counter = int.from_bytes(os.urandom(2), "big") & 0x03FF
        else:
            _v7_counter += 1
            if _v7_counter > 0x0FFF:
                _last_v7_timestamp_ms += 1
                _v7_counter = 0
            now_ms = _last_v7_timestamp_ms

        time_bytes = now_ms.to_bytes(6, byteorder="big")
        byte6 = 0x70 | ((_v7_counter >> 8) & 0x0F)
        byte7 = _v7_counter & 0xFF
        rand_b = os.urandom(8)
        byte8 = 0x80 | (rand_b[0] & 0x3F)

        b = time_bytes + bytes([byte6, byte7, byte8]) + rand_b[1:]
        return str(uuid.UUID(bytes=b))


def _reset_uuid7_state_for_test(last_ts_ms: int = 0, counter: int = 0) -> None:
    """Internal test-only helper to reset UUIDv7 state across isolated test cases."""
    global _last_v7_timestamp_ms, _v7_counter
    with _uuid7_lock:
        _last_v7_timestamp_ms = last_ts_ms
        _v7_counter = counter


def now_utc_micros() -> int:
    """Get current UTC timestamp in microseconds."""
    return int(datetime.now(UTC).timestamp() * 1_000_000)


def datetime_to_micros(dt: datetime) -> int:
    """Convert a UTC datetime to microseconds."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1_000_000)


def micros_to_datetime(micros: int) -> datetime:
    """Convert microseconds since epoch to UTC datetime."""
    return datetime.fromtimestamp(micros / 1_000_000.0, tz=UTC)


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class IngestMode(str, Enum):
    LIVE = "live"
    BACKFILL = "backfill"
    REPLAY = "replay"


class Envelope(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str = Field(default_factory=generate_uuid7)
    schema_version: int = 1
    source: str
    symbol: str
    event_ts: int
    ingest_ts: int = Field(default_factory=now_utc_micros)
    seq: int = 0


class RawFrame(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str = Field(default_factory=generate_uuid7)
    schema_version: int = 1
    source: str
    symbol: str
    event_ts: int
    ingest_ts: int = Field(default_factory=now_utc_micros)
    seq: int = 0
    channel: str
    payload: bytes
    ingest_mode: str = "live"


class ValidatedEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str
    schema_version: int = 1
    source: str
    symbol: str
    event_ts: int
    ingest_ts: int
    seq: int
    channel: str
    payload: bytes
    ingest_mode: str = "live"


class MarketTrade(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str
    schema_version: int = 1
    source: str
    symbol: str
    event_ts: int
    ingest_ts: int
    seq: int
    trade_id: str
    price: Decimal
    size: Decimal
    side: TradeSide


class DeadLetter(BaseModel):
    model_config = ConfigDict(frozen=True)

    original_payload: bytes
    error_code: str
    error_msg: str
    src_topic: str
    src_partition: int
    src_offset: int
    failed_at: int = Field(default_factory=now_utc_micros)
    stage: str = "validator"


class AvroCodec:
    """Helper to parse Avro schemas and serialize/deserialize messages."""

    def __init__(self, schema_path: str | Path):
        with open(schema_path, encoding="utf-8") as f:
            self._raw_schema = json.load(f)
        self.parsed_schema = fastavro.parse_schema(self._raw_schema)

    def encode(self, record: dict[str, Any]) -> bytes:
        out = io.BytesIO()
        fastavro.schemaless_writer(out, self.parsed_schema, record)
        return out.getvalue()

    def decode(self, data: bytes) -> dict[str, Any]:
        inp = io.BytesIO(data)
        res = fastavro.schemaless_reader(inp, self.parsed_schema)
        return res if isinstance(res, dict) else {}
