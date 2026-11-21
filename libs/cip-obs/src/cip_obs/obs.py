"""cip-obs: Observability package (OpenTelemetry tracing, Prometheus metrics, structured JSON logging)."""

import json
import logging
import sys
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON objects matching CIP specifications."""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "service": self.service_name,
            "message": record.getMessage(),
            "logger": record.name,
        }

        # Extra metadata if provided
        for attr in ("trace_id", "span_id", "symbol", "event", "error_code"):
            if hasattr(record, attr):
                log_obj[attr] = getattr(record, attr)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


def setup_logging(service_name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure root logger to output structured JSON to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter(service_name))

    root = logging.getLogger()
    root.setLevel(level)
    # Remove existing handlers to prevent duplicate lines
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)

    logger = logging.getLogger(service_name)
    logger.setLevel(level)
    return logger


# Standard metrics definitions across CIP pipelines
MESSAGES_PROCESSED_TOTAL = Counter(
    "cip_messages_processed_total",
    "Total messages processed by this service",
    ["service", "topic", "status"],
)

MESSAGES_FAILED_TOTAL = Counter(
    "cip_messages_failed_total",
    "Total messages failed and routed to DLQ or dropped",
    ["service", "topic", "error_code"],
)

PROCESSING_DURATION_SECONDS = Histogram(
    "cip_processing_duration_seconds",
    "Duration of message processing in seconds",
    ["service", "stage"],
    buckets=[0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
)

PIPELINE_E2E_SECONDS = Histogram(
    "cip_pipeline_e2e_seconds",
    "End-to-end latency from exchange event_ts to completion",
    ["service", "symbol"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

LAST_EVENT_AGE_SECONDS = Gauge(
    "cip_ingest_last_event_age_seconds",
    "Age of last event ingested in seconds",
    ["service", "symbol"],
)
