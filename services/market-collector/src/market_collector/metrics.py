"""Prometheus metrics for market-collector."""

from prometheus_client import Counter, Gauge

COLLECTOR_GAPS_TOTAL = Counter(
    "cip_market_collector_gaps_total",
    "Total market feed gaps detected by collector",
    ["symbol", "reason"],
)

COLLECTOR_PRODUCE_ERRORS_TOTAL = Counter(
    "cip_market_collector_produce_errors_total",
    "Total Kafka producer errors in market-collector",
    ["symbol"],
)

COLLECTOR_SYMBOL_STALE = Gauge(
    "cip_market_collector_symbol_stale",
    "Gauge indicating whether symbol feed is stale (> 5s without frames)",
    ["symbol"],
)
