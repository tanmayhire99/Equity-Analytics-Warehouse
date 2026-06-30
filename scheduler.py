"""Daily scheduler — runs the pipeline every weekday after the NSE close.

Scheduling lives in the codebase (not a system crontab) so it is version
controlled and visible to anyone reviewing the project. Blocks the foreground;
run it under a process manager or `nohup` in production.

Usage:
    python scheduler.py
"""
from __future__ import annotations

from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger

from config import settings
from config.settings import SCHEDULE_HOUR, SCHEDULE_MINUTE, TIMEZONE
from pipeline.runner import run_pipeline


def daily_job() -> None:
    """Ingest, then run data-quality checks and alert on failure.

    The quality step is best-effort: a failure is logged + alerted (and the
    webhook fires if configured) but never crashes the scheduler, so a bad data
    day doesn't take the daily job process down.
    """
    run_pipeline()
    try:
        from db.connection import get_connection
        from pipeline import quality

        conn = get_connection()
        try:
            report = quality.run_quality_checks(conn)
        finally:
            conn.close()
        quality.emit_alert(report, webhook_url=settings.QUALITY_ALERT_WEBHOOK or None)
        if not report.ok:
            logger.warning("Post-ingest data-quality checks FAILED: {}",
                           "; ".join(f"{r.name}: {r.detail}" for r in report.failed))
    except Exception:
        logger.exception("Post-ingest data-quality step errored (ingestion already completed)")


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=ZoneInfo(TIMEZONE))
    scheduler.add_job(
        daily_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        id="daily_equity_pipeline",
        name="Daily NSE equity ingestion",
        misfire_grace_time=3600,  # tolerate up to 1h of scheduler downtime
        coalesce=True,            # collapse missed runs into a single catch-up
    )
    return scheduler


if __name__ == "__main__":
    scheduler = build_scheduler()
    logger.info(
        "Scheduler started — running Mon-Fri at {:02d}:{:02d} {}. Ctrl+C to stop.",
        SCHEDULE_HOUR, SCHEDULE_MINUTE, TIMEZONE,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
