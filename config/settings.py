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


# --- Data-quality monitoring ----------------------------------------------
# Thresholds for pipeline/quality.py (used by the data-quality DAG, the
# standalone run_quality.py entrypoint, and the post-ingest scheduler check).
# Tolerate a long weekend / holiday stretch before freshness is a failure.
MAX_STALENESS_DAYS: int = int(os.getenv("MAX_STALENESS_DAYS", "5"))
# Sanity floor: a healthy NIFTY-50 warehouse has thousands of rows; far fewer
# means it was never backfilled or got truncated.
MIN_FACT_ROWS: int = int(os.getenv("MIN_FACT_ROWS", "100"))
# Above this many error_log rows the quality run emits a (non-fatal) WARN.
QUALITY_ERROR_WARN_THRESHOLD: int = int(os.getenv("QUALITY_ERROR_WARN_THRESHOLD", "100"))
# Optional Slack-compatible incoming webhook; when set, a FAILing quality run
# POSTs an alert. Unset = log-only (the DAG still turns red).
QUALITY_ALERT_WEBHOOK: str = os.getenv("QUALITY_ALERT_WEBHOOK", "")


# --- jugaad-data cache ----------------------------------------------------
# jugaad-data caches NSE responses on disk. We point it at a project-local
# directory and PRE-CREATE it so its parallel download threads don't race to
# call os.makedirs() (a known FileExistsError bug in jugaad_data.util).
# Overridable via JUGAAD_CACHE_DIR for environments where the project root is
# not writable (e.g. inside the Airflow container, point it at /tmp).
JUGAAD_CACHE_DIR: Path = Path(os.getenv("JUGAAD_CACHE_DIR", str(PROJECT_ROOT / ".cache")))


# --- NSE watchlist --------------------------------------------------------
# Plain NSE symbols (no ".NS" suffix — that is a yfinance convention).
# company_name and sector populate dim_stock; jugaad-data does not provide
# them, so they are maintained here as reference data.
#
# Watchlist: NIFTY 50 (representative — index membership changes over time).
# Symbols are plain NSE tickers. A handful with special characters (e.g. "M&M")
# may fail to fetch via the NSE endpoint; the fetcher logs and skips them so a
# single bad symbol never breaks a run.
STOCKS: list[dict[str, str]] = [
    {"symbol": "RELIANCE",   "company_name": "Reliance Industries Ltd",        "sector": "Energy"},
    {"symbol": "ONGC",       "company_name": "Oil & Natural Gas Corp Ltd",     "sector": "Energy"},
    {"symbol": "NTPC",       "company_name": "NTPC Ltd",                       "sector": "Energy"},
    {"symbol": "POWERGRID",  "company_name": "Power Grid Corp of India Ltd",   "sector": "Energy"},
    {"symbol": "COALINDIA",  "company_name": "Coal India Ltd",                 "sector": "Energy"},
    {"symbol": "BPCL",       "company_name": "Bharat Petroleum Corp Ltd",      "sector": "Energy"},
    {"symbol": "TCS",        "company_name": "Tata Consultancy Services Ltd",  "sector": "Information Technology"},
    {"symbol": "INFY",       "company_name": "Infosys Ltd",                    "sector": "Information Technology"},
    {"symbol": "HCLTECH",    "company_name": "HCL Technologies Ltd",           "sector": "Information Technology"},
    {"symbol": "WIPRO",      "company_name": "Wipro Ltd",                      "sector": "Information Technology"},
    {"symbol": "TECHM",      "company_name": "Tech Mahindra Ltd",              "sector": "Information Technology"},
    {"symbol": "LTIM",       "company_name": "LTIMindtree Ltd",                "sector": "Information Technology"},
    {"symbol": "HDFCBANK",   "company_name": "HDFC Bank Ltd",                  "sector": "Financials"},
    {"symbol": "ICICIBANK",  "company_name": "ICICI Bank Ltd",                 "sector": "Financials"},
    {"symbol": "SBIN",       "company_name": "State Bank of India",            "sector": "Financials"},
    {"symbol": "KOTAKBANK",  "company_name": "Kotak Mahindra Bank Ltd",        "sector": "Financials"},
    {"symbol": "AXISBANK",   "company_name": "Axis Bank Ltd",                  "sector": "Financials"},
    {"symbol": "INDUSINDBK", "company_name": "IndusInd Bank Ltd",              "sector": "Financials"},
    {"symbol": "BAJFINANCE", "company_name": "Bajaj Finance Ltd",              "sector": "Financials"},
    {"symbol": "BAJAJFINSV", "company_name": "Bajaj Finserv Ltd",              "sector": "Financials"},
    {"symbol": "SBILIFE",    "company_name": "SBI Life Insurance Co Ltd",      "sector": "Financials"},
    {"symbol": "HDFCLIFE",   "company_name": "HDFC Life Insurance Co Ltd",     "sector": "Financials"},
    {"symbol": "SHRIRAMFIN", "company_name": "Shriram Finance Ltd",            "sector": "Financials"},
    {"symbol": "HINDUNILVR", "company_name": "Hindustan Unilever Ltd",         "sector": "Consumer Staples"},
    {"symbol": "ITC",        "company_name": "ITC Ltd",                        "sector": "Consumer Staples"},
    {"symbol": "NESTLEIND",  "company_name": "Nestle India Ltd",               "sector": "Consumer Staples"},
    {"symbol": "BRITANNIA",  "company_name": "Britannia Industries Ltd",       "sector": "Consumer Staples"},
    {"symbol": "TATACONSUM", "company_name": "Tata Consumer Products Ltd",     "sector": "Consumer Staples"},
    {"symbol": "ASIANPAINT", "company_name": "Asian Paints Ltd",               "sector": "Consumer Discretionary"},
    {"symbol": "TITAN",      "company_name": "Titan Company Ltd",              "sector": "Consumer Discretionary"},
    {"symbol": "TRENT",      "company_name": "Trent Ltd",                      "sector": "Consumer Discretionary"},
    {"symbol": "MARUTI",     "company_name": "Maruti Suzuki India Ltd",        "sector": "Automobile"},
    {"symbol": "M&M",        "company_name": "Mahindra & Mahindra Ltd",        "sector": "Automobile"},
    {"symbol": "TATAMOTORS", "company_name": "Tata Motors Ltd",                "sector": "Automobile"},
    {"symbol": "BAJAJ-AUTO", "company_name": "Bajaj Auto Ltd",                 "sector": "Automobile"},
    {"symbol": "EICHERMOT",  "company_name": "Eicher Motors Ltd",              "sector": "Automobile"},
    {"symbol": "HEROMOTOCO", "company_name": "Hero MotoCorp Ltd",              "sector": "Automobile"},
    {"symbol": "SUNPHARMA",  "company_name": "Sun Pharmaceutical Inds Ltd",    "sector": "Healthcare"},
    {"symbol": "DRREDDY",    "company_name": "Dr. Reddy's Laboratories Ltd",   "sector": "Healthcare"},
    {"symbol": "CIPLA",      "company_name": "Cipla Ltd",                      "sector": "Healthcare"},
    {"symbol": "APOLLOHOSP", "company_name": "Apollo Hospitals Enterprise Ltd","sector": "Healthcare"},
    {"symbol": "TATASTEEL",  "company_name": "Tata Steel Ltd",                 "sector": "Materials"},
    {"symbol": "JSWSTEEL",   "company_name": "JSW Steel Ltd",                  "sector": "Materials"},
    {"symbol": "HINDALCO",   "company_name": "Hindalco Industries Ltd",        "sector": "Materials"},
    {"symbol": "ULTRACEMCO", "company_name": "UltraTech Cement Ltd",           "sector": "Materials"},
    {"symbol": "GRASIM",     "company_name": "Grasim Industries Ltd",          "sector": "Materials"},
    {"symbol": "LT",         "company_name": "Larsen & Toubro Ltd",            "sector": "Industrials"},
    {"symbol": "ADANIPORTS", "company_name": "Adani Ports & SEZ Ltd",          "sector": "Industrials"},
    {"symbol": "ADANIENT",   "company_name": "Adani Enterprises Ltd",          "sector": "Industrials"},
    {"symbol": "BEL",        "company_name": "Bharat Electronics Ltd",         "sector": "Industrials"},
    {"symbol": "BHARTIARTL", "company_name": "Bharti Airtel Ltd",              "sector": "Telecom"},
]

# All securities tracked here trade on the NSE.
EXCHANGE: str = "NSE"

# Convenience list of just the symbols.
TICKERS: list[str] = [s["symbol"] for s in STOCKS]
