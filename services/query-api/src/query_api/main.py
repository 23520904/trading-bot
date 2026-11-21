"""query-api service:
- Read-only CQRS API (FastAPI) for market data queries (ticks, candles).
- Direct query to ClickHouse market.ticks table.
- Strict read-only semantics.
- RFC 7807 problem+json error formatting.
- Cursor pagination.
- Exposes /healthz, /readyz, /metrics.
"""

from datetime import datetime
from typing import Any

import clickhouse_connect
from cip_obs import setup_logging
from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings

logger = setup_logging("query-api")


class QueryApiSettings(BaseSettings):
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "market"
    http_port: int = 8000
    health_host: str = "0.0.0.0"


settings = QueryApiSettings()
app = FastAPI(title="query-api", version="1.0.0")

# ClickHouse Client
ch_client: Any = None


def get_ch_client() -> Any:
    global ch_client
    if ch_client is None:
        ch_client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
    return ch_client


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(response: Response) -> dict[str, str]:
    try:
        client = get_ch_client()
        res = client.command("SELECT 1")
        if res == 1:
            return {"status": "ok", "database": "connected"}
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
    response.status_code = 503
    return {"status": "not_ready", "database": "disconnected"}


@app.get("/metrics")
async def metrics() -> Response:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


class TickResponse(BaseModel):
    symbol: str
    event_ts: str
    trade_id: str
    price: str
    size: str
    side: str
    ingest_ts: str
    event_id: str


class TicksPageResponse(BaseModel):
    items: list[TickResponse]
    next_cursor: str | None = None


@app.get("/v1/ticks", response_model=TicksPageResponse)
async def get_ticks(
    symbol: str = Query(..., description="Market symbol, e.g. BTC-USD"),
    from_ts: datetime | None = Query(None, alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    limit: int = Query(100, ge=1, le=10000),
    cursor: str | None = Query(None),
) -> Any:
    try:
        client = get_ch_client()
        query = """
            SELECT symbol, event_ts, trade_id, price, size, side, ingest_ts, event_id
            FROM market.ticks FINAL
            WHERE symbol = %(symbol)s
        """
        params: dict[str, Any] = {"symbol": symbol}

        if from_ts:
            query += " AND event_ts >= %(from_ts)s"
            params["from_ts"] = from_ts

        if to_ts:
            query += " AND event_ts <= %(to_ts)s"
            params["to_ts"] = to_ts

        if cursor:
            query += " AND event_ts < %(cursor)s"
            params["cursor"] = cursor

        query += " ORDER BY event_ts DESC LIMIT %(limit)s"
        params["limit"] = limit

        result = client.query(query, parameters=params)
        items: list[TickResponse] = []
        next_cursor = None

        for row in result.result_rows:
            # row: symbol, event_ts, trade_id, price, size, side, ingest_ts, event_id
            evt_ts_str = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])
            ing_ts_str = row[6].isoformat() if hasattr(row[6], "isoformat") else str(row[6])
            side_str = "BUY" if row[5] == 1 or row[5] == "BUY" else "SELL"

            items.append(
                TickResponse(
                    symbol=row[0],
                    event_ts=evt_ts_str,
                    trade_id=str(row[2]),
                    price=str(row[3]),
                    size=str(row[4]),
                    side=side_str,
                    ingest_ts=ing_ts_str,
                    event_id=str(row[7]),
                )
            )

        if items and len(items) == limit:
            next_cursor = items[-1].event_ts

        return TicksPageResponse(items=items, next_cursor=next_cursor)

    except Exception as e:
        logger.error(f"Error executing ticks query: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "type": "about:blank",
                "title": "Internal Query Error",
                "status": 500,
                "detail": str(e),
            },
            media_type="application/problem+json",
        )
