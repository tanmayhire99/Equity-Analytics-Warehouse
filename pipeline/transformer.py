"""Stage 3 — Transform.

Resolves the dimension foreign keys (ticker -> stock_id, trade date -> date_id)
and reshapes the clean DataFrame into exactly the columns `fact_prices` expects.

Lookups are fetched once from the database (not per row) and applied as vectorised
Pandas maps. Any row whose ticker or date cannot be resolved is dropped with a
warning — in normal operation this never happens because dim_stock is seeded from
the same watchlist we fetch and dim_date spans 2020-2026.
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

_FACT_COLUMNS = ["stock_id", "date_id", "open", "high", "low", "close", "vwap", "volume"]


def get_stock_id_map(conn) -> dict[str, int]:
    """Return {ticker: stock_id} from dim_stock."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, stock_id FROM dim_stock")
        return {ticker: stock_id for ticker, stock_id in cur.fetchall()}


def get_date_id_map(conn) -> dict[str, int]:
    """Return {'YYYY-MM-DD': date_id} from dim_date."""
    with conn.cursor() as cur:
        cur.execute("SELECT full_date, date_id FROM dim_date")
        return {full_date.strftime("%Y-%m-%d"): date_id for full_date, date_id in cur.fetchall()}


def transform(df: pd.DataFrame, conn) -> pd.DataFrame:
    """Map FKs and return a DataFrame with the columns fact_prices needs."""
    if df.empty:
        return pd.DataFrame(columns=_FACT_COLUMNS)

    stock_map = get_stock_id_map(conn)
    date_map = get_date_id_map(conn)

    out = df.copy()
    out["stock_id"] = out["ticker"].map(stock_map)
    out["date_id"] = out["Date"].dt.strftime("%Y-%m-%d").map(date_map)

    unresolved = out["stock_id"].isna() | out["date_id"].isna()
    if unresolved.any():
        logger.warning(
            "Dropping {} row(s) with unresolved stock_id/date_id (ticker or date "
            "not in dimensions)", int(unresolved.sum())
        )
        out = out[~unresolved]

    if out.empty:
        return pd.DataFrame(columns=_FACT_COLUMNS)

    out = out.rename(columns={
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume",
    })
    out["stock_id"] = out["stock_id"].astype(int)
    out["date_id"] = out["date_id"].astype(int)
    out["volume"] = out["volume"].astype("int64")

    return out[_FACT_COLUMNS].reset_index(drop=True)
