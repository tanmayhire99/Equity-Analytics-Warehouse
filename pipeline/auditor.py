"""Stage 5 — Audit.

Writes exactly one row per pipeline execution to pipeline_runs.

The flow is: create_run_placeholder() inserts a RUNNING row up front (so its
run_id can link error_log rows), and update_run() fills in the final metrics at
the end. This is the C-4 fix from the plan — a single logical row per run rather
than a phantom RUNNING row plus a second final row.

update_run() rolls back any in-flight/aborted transaction before writing, so it
still succeeds even when the pipeline failed mid-transaction. It also tolerates
run_id being None (a failure before the placeholder was created) by inserting a
fresh row instead.
"""
from __future__ import annotations

from psycopg2.extensions import connection as PgConnection


def create_run_placeholder(conn: PgConnection) -> int:
    """Insert a RUNNING row and return its run_id."""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO pipeline_runs (status) VALUES ('RUNNING') RETURNING run_id")
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def update_run(
    conn: PgConnection,
    run_id: int | None,
    tickers_fetched: int,
    rows_fetched: int,
    rows_inserted: int,
    rows_rejected: int,
    duration_seconds: float,
    status: str,
    error_message: str | None = None,
) -> int:
    """Finalise the audit row for this run (or insert one if run_id is None)."""
    # Clear any aborted/in-flight transaction left by a failed stage. Prior
    # successful writes were already committed by their own functions, so this
    # only discards an incomplete transaction.
    conn.rollback()

    metrics = (
        tickers_fetched, rows_fetched, rows_inserted,
        rows_rejected, duration_seconds, status, error_message,
    )

    with conn.cursor() as cur:
        if run_id is None:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (tickers_fetched, rows_fetched, rows_inserted, rows_rejected,
                     duration_seconds, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING run_id
                """,
                metrics,
            )
            run_id = cur.fetchone()[0]
        else:
            cur.execute(
                """
                UPDATE pipeline_runs
                   SET tickers_fetched  = %s,
                       rows_fetched     = %s,
                       rows_inserted    = %s,
                       rows_rejected    = %s,
                       duration_seconds = %s,
                       status           = %s,
                       error_message    = %s
                 WHERE run_id = %s
                """,
                metrics + (run_id,),
            )
    conn.commit()
    return run_id
