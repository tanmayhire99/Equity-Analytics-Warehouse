"""Airflow DAG that orchestrates the equity ETL pipeline.

Tasks (linear):
    ingest          -> run the full fetch/validate/transform/load/audit/refresh
    quality_gate    -> fail the run if the latest pipeline_runs row is FAILED
    report_snapshot -> log the current top movers from the reporting layer

The callables import the project's own pipeline modules (the repo is mounted at
/opt/airflow/project and on PYTHONPATH), so Airflow is purely the orchestrator —
the ETL logic lives in one place and is reused by the scheduler and run scripts.
Imports are inside the callables so DAG *parsing* never depends on the pipeline's
runtime libraries.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.task")


def _ingest(**_):
    from pipeline.runner import run_pipeline

    result = run_pipeline()
    log.info("Pipeline result: %s", result)
    if result["status"] == "FAILED":
        raise RuntimeError(f"Pipeline run failed: {result}")
    return result


def _quality_gate(**_):
    from db.connection import get_connection

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, rows_inserted FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("No pipeline_runs row found")
            status, rows_inserted = row
            if status == "FAILED":
                raise RuntimeError("Latest pipeline run is FAILED")
            cur.execute("SELECT COUNT(*) FROM fact_prices")
            total = cur.fetchone()[0]
            if total == 0:
                raise RuntimeError("fact_prices is empty")
            log.info("Quality gate OK — status=%s, fact_prices rows=%s", status, total)
    finally:
        conn.close()


def _report_snapshot(**_):
    from db.connection import get_connection

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, return_30d_pct
                FROM mv_stock_snapshot
                ORDER BY return_30d_pct DESC NULLS LAST
                LIMIT 5
                """
            )
            for ticker, ret in cur.fetchall():
                log.info("TOP MOVER  %-12s  %s%%", ticker, ret)
    finally:
        conn.close()


with DAG(
    dag_id="equity_pipeline",
    description="Daily NSE equity ingestion + quality gate + reporting refresh",
    schedule="0 16 * * 1-5",  # 16:00 on weekdays
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["equity", "etl", "nse"],
) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
    quality_gate = PythonOperator(task_id="quality_gate", python_callable=_quality_gate)
    report_snapshot = PythonOperator(task_id="report_snapshot", python_callable=_report_snapshot)

    ingest >> quality_gate >> report_snapshot
