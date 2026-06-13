"""One-off historical backfill.

Pulls BACKFILL_DAYS of history (default 90) for the full watchlist so the
warehouse has enough data for the 30-day window views and moving averages. Safe
to re-run — the loader skips rows that already exist.

Usage:
    python run_backfill.py            # uses BACKFILL_DAYS from settings/.env
    BACKFILL_DAYS=365 python run_backfill.py
"""
from __future__ import annotations

from loguru import logger

from config.settings import BACKFILL_DAYS
from pipeline.runner import run_pipeline

if __name__ == "__main__":
    logger.info("Starting backfill over the last {} days...", BACKFILL_DAYS)
    run_pipeline(lookback_days=BACKFILL_DAYS)
