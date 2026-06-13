"""Stage 4 — Load.

Bulk-inserts clean rows into fact_prices and rejected rows into error_log.

`fact_prices` uses ON CONFLICT (stock_id, date_id) DO NOTHING so the pipeline is
idempotent: re-running for a date that already has data silently skips the
duplicates instead of erroring. This is essential for a scheduled job that may be
manually re-triggered.

Insert counts come from RETURNING + fetch=True rather than cur.rowcount, because
psycopg2's execute_values runs in batches and rowcount only reflects the final
batch — which would undercount any load larger than one page.
"""
from __future__ import annotations

import math

import pandas as pd
import psycopg2.extras
from psycopg2.extras import Json

_FACT_COLUMNS = ["stock_id", "date_id", "open", "high", "low", "close", "vwap", "volume"]

_INSERT_FACT_SQL = """
    INSERT INTO fact_prices (stock_id, date_id, open, high, low, close, vwap, volume)
    VALUES %s
    ON CONFLICT (stock_id, date_id) DO NOTHING
    RETURNING price_id
"""

_INSERT_ERROR_SQL = """
    INSERT INTO error_log
        (pipeline_run_id, ticker, trade_date, raw_data, error_type, error_detail)
    VALUES %s
"""


def _native(value):
    """Coerce numpy/NaN values to plain Python types psycopg2 can adapt."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar
        return value.item()
    return value


def load(df: pd.DataFrame, conn) -> int:
    """Insert fact rows; return the number of rows actually inserted."""
    if df.empty:
        return 0
    records = [tuple(_native(v) for v in row) for row in df[_FACT_COLUMNS].itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        returned = psycopg2.extras.execute_values(cur, _INSERT_FACT_SQL, records, fetch=True)
    conn.commit()
    return len(returned)


def insert_errors(conn, errors: list[dict]) -> int:
    """Insert rejected rows into error_log; return the number inserted."""
    if not errors:
        return 0
    records = [
        (
            e["pipeline_run_id"],
            e["ticker"],
            e["trade_date"],
            Json(e["raw_data"]),
            e["error_type"],
            e["error_detail"],
        )
        for e in errors
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _INSERT_ERROR_SQL, records)
    conn.commit()
    return len(errors)
