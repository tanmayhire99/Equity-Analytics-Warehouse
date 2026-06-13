"""Central configuration for the equity pipeline.

Loads environment variables from `.env` and defines the constants used across
the pipeline: the database URL, fetch window, batch size, log level, and the
NSE watchlist (with company + sector metadata used to seed `dim_stock`).
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Project root = parent of this file's directory (config/ -> repo root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env from the project root (no-op if the file is absent).
load_dotenv(PROJECT_ROOT / ".env")


# --- Database -------------------------------------------------------------
# Environment-driven target: DB_TARGET=local (default) uses DATABASE_URL;
# DB_TARGET=supabase uses DATABASE_URL_SUPABASE. Switching cloud<->local is a
# one-line env change with no code edits. Credentials are never hard-coded.
DB_TARGET: str = os.getenv("DB_TARGET", "local").lower()

_LOCAL_DEFAULT = "postgresql://equity_user:equity_pass@localhost:5433/equity_db"

if DB_TARGET == "supabase":
    DATABASE_URL: str = os.getenv("DATABASE_URL_SUPABASE") or os.getenv("DATABASE_URL", _LOCAL_DEFAULT)
else:
    DATABASE_URL = os.getenv("DATABASE_URL", _LOCAL_DEFAULT)


# --- Pipeline behaviour ---------------------------------------------------
# Number of calendar days of history to pull on a normal (incremental) run.
# A small window is enough for the daily job; the backfill script overrides it.
FETCH_LOOKBACK_DAYS: int = int(os.getenv("FETCH_LOOKBACK_DAYS", "7"))

# Number of calendar days to pull during a one-off historical backfill.
BACKFILL_DAYS: int = int(os.getenv("BACKFILL_DAYS", "90"))

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# Daily schedule (IST). Defaults to 16:00 — shortly after the 15:30 NSE close.
# Same-day EOD data published later is still captured on subsequent runs because
# each run re-scans FETCH_LOOKBACK_DAYS and the loader is idempotent.
SCHEDULE_HOUR: int = int(os.getenv("SCHEDULE_HOUR", "16"))
SCHEDULE_MINUTE: int = int(os.getenv("SCHEDULE_MINUTE", "0"))
TIMEZONE: str = os.getenv("TIMEZONE", "Asia/Kolkata")


# --- jugaad-data cache ----------------------------------------------------
# jugaad-data caches NSE responses on disk. We point it at a project-local
# directory and PRE-CREATE it so its parallel download threads don't race to
# call os.makedirs() (a known FileExistsError bug in jugaad_data.util).
JUGAAD_CACHE_DIR: Path = PROJECT_ROOT / ".cache"


# --- NSE watchlist --------------------------------------------------------
# Plain NSE symbols (no ".NS" suffix — that is a yfinance convention).
# company_name and sector populate dim_stock; jugaad-data does not provide
# them, so they are maintained here as reference data.
STOCKS: list[dict[str, str]] = [
    {"symbol": "RELIANCE",   "company_name": "Reliance Industries Ltd",   "sector": "Energy"},
    {"symbol": "TCS",        "company_name": "Tata Consultancy Services",  "sector": "Information Technology"},
    {"symbol": "INFY",       "company_name": "Infosys Ltd",                "sector": "Information Technology"},
    {"symbol": "WIPRO",      "company_name": "Wipro Ltd",                  "sector": "Information Technology"},
    {"symbol": "HDFCBANK",   "company_name": "HDFC Bank Ltd",              "sector": "Financials"},
    {"symbol": "ICICIBANK",  "company_name": "ICICI Bank Ltd",             "sector": "Financials"},
    {"symbol": "SBIN",       "company_name": "State Bank of India",        "sector": "Financials"},
    {"symbol": "BAJFINANCE", "company_name": "Bajaj Finance Ltd",          "sector": "Financials"},
    {"symbol": "HINDUNILVR", "company_name": "Hindustan Unilever Ltd",     "sector": "Consumer Staples"},
    {"symbol": "ITC",        "company_name": "ITC Ltd",                    "sector": "Consumer Staples"},
]

# All securities tracked here trade on the NSE.
EXCHANGE: str = "NSE"

# Convenience list of just the symbols.
TICKERS: list[str] = [s["symbol"] for s in STOCKS]
