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

from config.settings import SCHEDULE_HOUR, SCHEDULE_MINUTE, TIMEZONE
from pipeline.runner import run_pipeline


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=ZoneInfo(TIMEZONE))
    scheduler.add_job(
        run_pipeline,
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
