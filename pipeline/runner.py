"""Pipeline orchestration — the shared ETL run used by run_once, run_backfill,
and the scheduler.

Implements two correctness fixes from the plan:
  * C-3: every counter is initialised before the try block, so a failure during
    fetch can never raise NameError while writing the audit row.
  * C-4: exactly one audit row per run — create_run_placeholder() up front,
    update_run() at the end. No phantom RUNNING row left behind.

Run status semantics written to pipeline_runs:
  * SUCCESS — completed with zero rejected rows
  * PARTIAL — completed, but the validator rejected one or more rows
  * FAILED  — an exception aborted the run
"""
from __future__ import annotations

import time

from loguru import logger

from config.settings import FETCH_LOOKBACK_DAYS, TICKERS
from db.connection import get_connection
from pipeline.auditor import create_run_placeholder, update_run
from pipeline.fetcher import fetch_all
from pipeline.loader import insert_errors, load
from pipeline.transformer import transform
from pipeline.validator import validate


def run_pipeline(lookback_days: int | None = None, tickers: list[str] | None = None) -> dict:
    """Execute the full fetch -> validate -> transform -> load -> audit cycle."""
    start = time.time()
    tickers = tickers if tickers is not None else TICKERS
    lookback_days = lookback_days if lookback_days is not None else FETCH_LOOKBACK_DAYS

    # C-3: initialise all counters and run_id before the try block.
    run_id: int | None = None
    rows_fetched = 0
    rows_inserted = 0
    rows_rejected = 0
    status = "SUCCESS"
    error_message: str | None = None

    conn = get_connection()
    try:
        run_id = create_run_placeholder(conn)  # C-4: single audit row, created up front

        raw = fetch_all(tickers, lookback_days=lookback_days)
        rows_fetched = len(raw)

        clean, errors = validate(raw, run_id)
        rows_rejected = len(errors)
        if errors:
            insert_errors(conn, errors)

        ready = transform(clean, conn)
        rows_inserted = load(ready, conn)

        status = "PARTIAL" if rows_rejected else "SUCCESS"

    except Exception as exc:  # noqa: BLE001 — record failure in the audit trail
        status = "FAILED"
        error_message = str(exc)
        logger.exception("Pipeline run failed")

    finally:
        duration = round(time.time() - start, 2)
        run_id = update_run(
            conn, run_id, len(tickers), rows_fetched, rows_inserted,
            rows_rejected, duration, status, error_message,
        )
        conn.close()

    logger.info(
        "Run {} {} — fetched={} inserted={} rejected={} in {}s",
        run_id, status, rows_fetched, rows_inserted, rows_rejected, duration,
    )
    return {
        "run_id": run_id,
        "status": status,
        "rows_fetched": rows_fetched,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "duration_seconds": duration,
    }
