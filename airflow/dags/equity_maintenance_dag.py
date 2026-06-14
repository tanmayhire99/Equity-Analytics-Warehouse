"""On-demand maintenance DAG.

Manual-trigger only. Orchestrates the database maintenance stored procedures:
  * refresh_reporting   -> CALL sp_refresh_reporting()  (rebuild mv_stock_snapshot)
  * purge_old_errors    -> CALL sp_purge_old_errors(N)  (data-retention cleanup)

The error-log retention window is a Param set at trigger time. sp_purge_old_errors
COMMITs internally, so that task runs on an autocommit connection.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger("airflow.task")


@dag(
    dag_id="equity_maintenance",
    description="On-demand reporting refresh + error-log retention",
    schedule=None,            # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["equity", "maintenance", "manual"],
    params={
        "error_retention_days": Param(
            90,
            type="integer",
            minimum=1,
            maximum=3650,
            title="Error-log retention (days)",
            description="Delete error_log rows older than this many days.",
        ),
    },
)
def equity_maintenance():
    @task
    def refresh_reporting() -> None:
        from db.connection import get_connection

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("CALL sp_refresh_reporting()")
            conn.commit()
            log.info("mv_stock_snapshot refreshed")
        finally:
            conn.close()

    @task
    def purge_old_errors(**context) -> None:
        from db.connection import get_connection

        days = int(context["params"]["error_retention_days"])
        conn = get_connection()
        conn.autocommit = True  # the procedure manages its own COMMIT
        try:
            with conn.cursor() as cur:
                cur.execute("CALL sp_purge_old_errors(%s)", (days,))
            log.info("Purged error_log rows older than %s days", days)
        finally:
            conn.close()

    refresh_reporting() >> purge_old_errors()


equity_maintenance()
