"""Prometheus metrics for market-collector.

Following CIP consistent naming scheme: cip_<domain>_<name>_<unit> (Task A0f).
Domain: collector
"""

from prometheus_client import Counter, Gauge

# Total market feed gaps detected by collector
COLLECTOR_GAPS_TOTAL = Counter(
    "cip_collector_gaps_total",
    "Total market feed gaps detected by collector",
    ["symbol", "reason"],
)

# Total Kafka producer errors in market-collector
COLLECTOR_PRODUCE_ERRORS_TOTAL = Counter(
    "cip_collector_produce_errors_total",
    "Total Kafka producer errors in market-collector",
    ["symbol"],
)

# Gauge indicating whether symbol feed is stale (> 5s without frames)
COLLECTOR_SYMBOL_STALE = Gauge(
    "cip_collector_symbol_stale_bool",
    "Gauge (1=stale, 0=live) indicating whether symbol feed is stale (> 5s without frames)",
    ["symbol"],
)

# Total messages processed by market-collector
COLLECTOR_MESSAGES_PROCESSED_TOTAL = Counter(
    "cip_collector_messages_processed_total",
    "Total messages processed by market-collector",
    ["service", "topic", "status"],
)

# Age of last event ingested in seconds
COLLECTOR_LAST_EVENT_AGE_SECONDS = Gauge(
    "cip_collector_last_event_age_seconds",
    "Age of last event ingested in seconds",
    ["service", "symbol"],
)
