"""Read-only consumer API for the equity warehouse.

This is the stable contract downstream consumers call instead of hitting market
APIs at query time. The MIDAS multi-agent app's Indian-stock worker can back its
`get_quote` / historical lookups with these functions (its fundamentals — P/E,
market cap, growth — are not in the NSE bhavcopy and remain its own concern).

Design notes:
  * Reads only from the analytics layer (views / function / snapshot), never the
    physical fact/dimension tables, so consumers stay decoupled from the schema.
  * Returns plain JSON-native types (Decimal -> float, date -> ISO string).
  * Each function accepts an optional open connection; if omitted it opens and
    closes its own. Consumers that call frequently should pass a pooled/cached
    connection (MIDAS already wraps calls in a 1-hour TTL cache).

Example:
    python consumer_api.py            # prints a sample quote + top movers
"""
from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Optional

from db.connection import get_connection


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


@contextmanager
def _cursor(conn):
    owns = conn is None
    connection = conn or get_connection()
    try:
        with connection.cursor() as cur:
            yield cur
    finally:
        if owns:
            connection.close()


def _rows_to_dicts(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, row)} for row in cur.fetchall()]


def get_quote(ticker: str, conn=None) -> Optional[dict]:
    """Latest snapshot for a ticker (price, 52w range, 30d return) or None.

    Reads the pre-aggregated mv_stock_snapshot for a fast single-row lookup.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            SELECT ticker, company_name, sector, latest_date, latest_close,
                   latest_vwap, latest_volume, high_52w, low_52w, return_30d_pct
            FROM mv_stock_snapshot
            WHERE ticker = %s
            """,
            (ticker.upper(),),
        )
        rows = _rows_to_dicts(cur)
    return rows[0] if rows else None


def get_history(ticker: str, from_date: dt.date | None = None,
                to_date: dt.date | None = None, conn=None) -> list[dict]:
    """OHLCV + VWAP + 7d/30d moving averages for a ticker over a date range."""
    ticker = ticker.upper()
    to_date = to_date or dt.date.today()
    from_date = from_date or (to_date - dt.timedelta(days=90))
    with _cursor(conn) as cur:
        cur.execute("SELECT 1 FROM dim_stock WHERE ticker = %s", (ticker,))
        if cur.fetchone() is None:
            return []
        cur.execute(
            """
            SELECT trade_date, open, high, low, close, vwap, volume, ma_7d, ma_30d
            FROM fn_ticker_report(%s, %s, %s)
            """,
            (ticker, from_date, to_date),
        )
        return _rows_to_dicts(cur)


def get_top_movers(limit: int = 10, conn=None) -> list[dict]:
    """Top gainers/losers over the trailing 30 days (one row per ticker)."""
    with _cursor(conn) as cur:
        cur.execute(
            """
            SELECT ticker, company_name, sector, close_30d_ago, close_today, return_30d_pct
            FROM vw_top_movers_30d
            ORDER BY return_30d_pct DESC
            LIMIT %s
            """,
            (limit,),
        )
        return _rows_to_dicts(cur)


def get_sector_performance(conn=None) -> list[dict]:
    """Average daily return and volume by sector and ISO week."""
    with _cursor(conn) as cur:
        cur.execute(
            """
            SELECT sector, year, week_number, avg_daily_return_pct,
                   total_volume, stocks_in_sector
            FROM vw_sector_weekly_performance
            """
        )
        return _rows_to_dicts(cur)


if __name__ == "__main__":
    import json

    print("get_quote('RELIANCE'):")
    print(json.dumps(get_quote("RELIANCE"), indent=2))
    print("\nget_top_movers(limit=3):")
    print(json.dumps(get_top_movers(limit=3), indent=2))
