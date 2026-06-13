"""Stage 1 — Fetch.

Pulls daily OHLCV data for the configured NSE watchlist from the NSE bhavcopy
via jugaad-data. Returns one tidy DataFrame with canonical column names that the
rest of the pipeline expects:

    ticker, Date, Open, High, Low, Close, prev_close, vwap, Volume

Two source-specific quirks are handled here, at the boundary:

1. Cache race: jugaad-data caches responses and creates its cache directory with
   os.makedirs() (no exist_ok) from parallel threads, which can raise
   FileExistsError. We point its cache at a project-local dir and pre-create it.

2. Date offset: NSE returns CH_TIMESTAMP as IST-midnight encoded as UTC
   (e.g. '2026-06-11T18:30:00.000Z' is really trade date 2026-06-12). Left
   uncorrected, every price lands on the previous day and some fall on
   weekends. We add 5h30m and normalise to recover the true trade date.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
from loguru import logger

from config.settings import JUGAAD_CACHE_DIR, TICKERS

# Point jugaad-data's cache at a project-local directory and PRE-CREATE the
# app subdirectory so its worker threads never race on os.makedirs().
os.environ.setdefault("J_CACHE_DIR", str(JUGAAD_CACHE_DIR))
for _app in ("nsehistory-stock", "nsehistory-index", "nsehistory-derivatives"):
    (JUGAAD_CACHE_DIR / _app).mkdir(parents=True, exist_ok=True)

from jugaad_data.nse import stock_df  # noqa: E402  (import after cache env is set)

# IST offset applied to NSE's UTC-encoded midnight timestamps.
_IST_OFFSET = pd.Timedelta(hours=5, minutes=30)


def correct_trade_dates(dates: pd.Series) -> pd.Series:
    """Recover true NSE trade dates from UTC-encoded IST-midnight timestamps.

    NSE returns e.g. '2026-06-11T18:30:00Z', which is really trade date
    2026-06-12 (IST midnight). Adding 5h30m and normalising recovers it.
    """
    return (dates + _IST_OFFSET).dt.normalize()

# Map jugaad-data's column names to the pipeline's canonical names.
_COLUMN_MAP = {
    "DATE": "Date",
    "OPEN": "Open",
    "HIGH": "High",
    "LOW": "Low",
    "CLOSE": "Close",
    "PREV. CLOSE": "prev_close",
    "VWAP": "vwap",
    "VOLUME": "Volume",
    "SYMBOL": "ticker",
}
_OUTPUT_COLUMNS = ["ticker", "Date", "Open", "High", "Low", "Close", "prev_close", "vwap", "Volume"]


def fetch_ticker(symbol: str, from_date: date, to_date: date) -> pd.DataFrame:
    """Fetch one symbol's OHLCV history and return it with canonical columns."""
    raw = stock_df(symbol=symbol, from_date=from_date, to_date=to_date, series="EQ")
    if raw is None or raw.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    df = raw.rename(columns=_COLUMN_MAP)
    # Recover the true NSE trade date from the UTC-encoded IST midnight.
    df["Date"] = correct_trade_dates(df["Date"])
    return df[_OUTPUT_COLUMNS]


def fetch_all(tickers: list[str] | None = None, lookback_days: int = 7) -> pd.DataFrame:
    """Fetch every ticker over the last `lookback_days` and concatenate.

    A failure on one ticker is logged and skipped so a single bad symbol never
    kills the whole run.
    """
    tickers = tickers if tickers is not None else TICKERS
    to_date = date.today()
    from_date = to_date - timedelta(days=lookback_days)

    frames: list[pd.DataFrame] = []
    for symbol in tickers:
        try:
            df = fetch_ticker(symbol, from_date, to_date)
            logger.info("Fetched {} rows for {}", len(df), symbol)
            if not df.empty:
                frames.append(df)
        except Exception as exc:  # noqa: BLE001 — isolate per-ticker failures
            logger.warning("Failed to fetch {}: {}", symbol, exc)

    if not frames:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)
    return pd.concat(frames, ignore_index=True)
