"""On-demand backfill / sync DAG.

Manual-trigger only. Use "Trigger DAG w/ config" in the UI to choose how many
days of history to (re)load — the loader is idempotent, so this safely fills
gaps or extends history without duplicating rows.

Demonstrates Airflow Params (a rendered trigger form) and the TaskFlow API.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger("airflow.task")


@dag(
    dag_id="equity_backfill",
    description="On-demand backfill/sync of NSE history (choose days at trigger time)",
    schedule=None,            # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["equity", "backfill", "manual"],
    params={
        "backfill_days": Param(
            90,
            type="integer",
            minimum=1,
            maximum=3650,
            title="Backfill days",
            description="How many calendar days of history to (re)load for the full watchlist.",
        ),
    },
)
def equity_backfill():
    @task
    def backfill(**context) -> dict:
        from pipeline.runner import run_pipeline

        days = int(context["params"]["backfill_days"])
        log.info("Backfilling %s days of history...", days)
        result = run_pipeline(lookback_days=days)
        if result["status"] == "FAILED":
            raise RuntimeError(f"Backfill failed: {result}")
        return result

    @task
    def summarize(result: dict) -> None:
        log.info(
            "Backfill complete — fetched=%s inserted=%s rejected=%s in %ss (run_id=%s, %s)",
            result["rows_fetched"], result["rows_inserted"], result["rows_rejected"],
            result["duration_seconds"], result["run_id"], result["status"],
        )

    summarize(backfill())


equity_backfill()
