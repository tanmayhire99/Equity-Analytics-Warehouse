"""Data-quality monitor DAG.

Runs the warehouse's health checks and fails (turns red in the UI) on real
problems, so Airflow doubles as an alerting surface. The check logic itself
lives in :mod:`pipeline.quality` — the single source of truth shared with the
standalone ``run_quality.py`` entrypoint and the daily scheduler — so behaviour
is identical everywhere and is unit-tested without Airflow.

Scheduled after the close on weekdays, and triggerable on demand.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger("airflow.task")


@dag(
    dag_id="equity_data_quality",
    description="Freshness, completeness, duplicate, price-sanity and error-rate checks",
    schedule="0 17 * * 1-5",   # weekdays after the close; also triggerable manually
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["equity", "quality", "monitoring"],
)
def equity_data_quality():
    @task
    def run_checks() -> dict:
        from config import settings
        from db.connection import get_connection
        from pipeline import quality

        conn = get_connection()
        try:
            report = quality.run_quality_checks(conn)
        finally:
            conn.close()

        quality.emit_alert(report, webhook_url=settings.QUALITY_ALERT_WEBHOOK or None)
        log.info("\n%s", quality.format_report(report))

        if not report.ok:
            reasons = "; ".join(f"{r.name}: {r.detail}" for r in report.failed)
            raise RuntimeError(f"Data-quality checks FAILED — {reasons}")
        return {"worst_status": report.worst_status,
                "checks": {r.name: r.status for r in report.results}}

    run_checks()


equity_data_quality()
