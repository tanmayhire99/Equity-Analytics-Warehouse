"""Data-quality monitor DAG.

Runs the warehouse's health checks and fails (turns red in the UI) on real
problems, so Airflow doubles as an alerting surface for "validate and
troubleshoot data". Scheduled after the close on weekdays, and triggerable on
demand.

Checks:
  * freshness     -> latest trade date must be recent (<= MAX_STALENESS_DAYS)
  * completeness  -> every active stock must have fact rows
  * error_report  -> summarise recent error_log activity (informational)
"""
from __future__ import annotations

import logging
from datetime import date, datetime

from airflow.decorators import dag, task

log = logging.getLogger("airflow.task")

MAX_STALENESS_DAYS = 5  # tolerate a long weekend / holiday stretch


@dag(
    dag_id="equity_data_quality",
    description="Freshness, completeness, and error-rate checks on the warehouse",
    schedule="0 17 * * 1-5",   # weekdays after the close; also triggerable manually
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["equity", "quality", "monitoring"],
)
def equity_data_quality():
    @task
    def check_freshness() -> str:
        from db.connection import get_connection

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(d.full_date) FROM fact_prices fp "
                    "JOIN dim_date d ON fp.date_id = d.date_id"
                )
                latest = cur.fetchone()[0]
        finally:
            conn.close()
        if latest is None:
            raise RuntimeError("Freshness check failed: warehouse has no data")
        staleness = (date.today() - latest).days
        if staleness > MAX_STALENESS_DAYS:
            raise RuntimeError(
                f"Freshness check failed: latest trade date {latest} is {staleness} days old"
            )
        log.info("Freshness OK — latest trade date %s (%s days old)", latest, staleness)
        return str(latest)

    @task
    def check_completeness() -> None:
        from db.connection import get_connection

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.ticker
                    FROM dim_stock s
                    LEFT JOIN fact_prices fp ON s.stock_id = fp.stock_id
                    WHERE s.is_active
                    GROUP BY s.ticker
                    HAVING COUNT(fp.price_id) = 0
                    ORDER BY s.ticker
                    """
                )
                missing = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
        if missing:
            raise RuntimeError(f"Completeness check failed: no data for {missing}")
        log.info("Completeness OK — every active stock has fact rows")

    @task
    def error_report() -> None:
        from db.connection import get_connection

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM error_log")
                total = cur.fetchone()[0]
                cur.execute(
                    "SELECT error_type, COUNT(*) FROM error_log "
                    "GROUP BY error_type ORDER BY COUNT(*) DESC"
                )
                by_type = cur.fetchall()
        finally:
            conn.close()
        log.info("error_log total rows: %s", total)
        for etype, count in by_type:
            log.info("  %-15s %s", etype, count)

    fresh = check_freshness()
    fresh >> check_completeness() >> error_report()


equity_data_quality()
