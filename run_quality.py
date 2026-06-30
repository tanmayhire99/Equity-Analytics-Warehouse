"""Run the warehouse data-quality checks once and alert on failure.

Standalone (no Airflow): point it at the configured target (local or Supabase
via DB_TARGET) and it runs every check in ``pipeline.quality``, logs the report,
optionally POSTs to ``QUALITY_ALERT_WEBHOOK`` on failure, and exits non-zero so
cron / CI / a Supabase scheduled job can react.

Usage:
    python run_quality.py                 # check the configured DB_TARGET
    DB_TARGET=supabase python run_quality.py
"""
from __future__ import annotations

import sys

from config import settings
from db.connection import get_connection
from pipeline import quality


def main() -> int:
    conn = get_connection()
    try:
        report = quality.run_quality_checks(conn)
    finally:
        conn.close()
    quality.emit_alert(report, webhook_url=settings.QUALITY_ALERT_WEBHOOK or None)
    print(quality.format_report(report))
    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
