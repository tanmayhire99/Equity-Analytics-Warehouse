"""Data-quality checks + alerting for the warehouse.

Previously these checks lived inline inside the Airflow data-quality DAG, where
they couldn't be reused (standalone / cron / CI), unit-tested, or alert anywhere
beyond "the DAG turns red". This module is the single source of truth:

* **pure evaluators** (``_evaluate_*``) hold the decision logic and are unit
  tested with no DB, mirroring the validator's style;
* thin **DB wrappers** (``check_*``) run one query each and delegate to an
  evaluator;
* :func:`run_quality_checks` runs them all and returns a :class:`QualityReport`;
* :func:`emit_alert` logs the report and, on FAIL, optionally POSTs to a Slack
  -compatible webhook (``QUALITY_ALERT_WEBHOOK``).

The Airflow DAG, the standalone ``run_quality.py`` entrypoint, and the daily
scheduler all call into here, so the checks behave identically everywhere.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

log = logging.getLogger("equity.quality")

# Status ranking so a report can report its worst outcome.
PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
_RANK = {PASS: 0, WARN: 1, FAIL: 2}


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    results: List[CheckResult] = field(default_factory=list)

    @property
    def worst_status(self) -> str:
        return max((r.status for r in self.results), key=lambda s: _RANK[s], default=PASS)

    @property
    def ok(self) -> bool:
        """True when no check FAILed (WARN is tolerated)."""
        return all(r.status != FAIL for r in self.results)

    @property
    def failed(self) -> List[CheckResult]:
        return [r for r in self.results if r.status == FAIL]

    @property
    def exit_code(self) -> int:
        return 0 if self.ok else 1


# ---------------------------------------------------------------------------
# Pure evaluators — the decision logic, unit-tested with no DB
# ---------------------------------------------------------------------------
def _evaluate_freshness(latest: Optional[date], today: date, max_staleness_days: int) -> CheckResult:
    if latest is None:
        return CheckResult("freshness", FAIL, "warehouse has no fact data", {"latest": None})
    staleness = (today - latest).days
    metrics = {"latest": str(latest), "staleness_days": staleness}
    if staleness > max_staleness_days:
        return CheckResult("freshness", FAIL,
                           f"latest trade date {latest} is {staleness} days old "
                           f"(> {max_staleness_days})", metrics)
    return CheckResult("freshness", PASS,
                       f"latest trade date {latest} ({staleness}d old)", metrics)


def _evaluate_completeness(missing: List[str]) -> CheckResult:
    if missing:
        preview = ", ".join(missing[:10]) + (" …" if len(missing) > 10 else "")
        return CheckResult("completeness", FAIL,
                           f"{len(missing)} active stock(s) have no fact rows: {preview}",
                           {"missing_count": len(missing), "missing": missing})
    return CheckResult("completeness", PASS, "every active stock has fact rows",
                       {"missing_count": 0})


def _evaluate_duplicates(dupe_count: int) -> CheckResult:
    if dupe_count > 0:
        return CheckResult("duplicates", FAIL,
                           f"{dupe_count} duplicate (stock_id, date_id) group(s) — "
                           "the UNIQUE constraint may have been dropped",
                           {"duplicate_groups": dupe_count})
    return CheckResult("duplicates", PASS, "no duplicate (stock_id, date_id) rows",
                       {"duplicate_groups": 0})


def _evaluate_price_sanity(bad_count: int) -> CheckResult:
    if bad_count > 0:
        return CheckResult("price_sanity", FAIL,
                           f"{bad_count} row(s) with non-positive close or high < low",
                           {"bad_rows": bad_count})
    return CheckResult("price_sanity", PASS, "all prices positive and high >= low",
                       {"bad_rows": 0})


def _evaluate_row_volume(total_rows: int, min_rows: int) -> CheckResult:
    metrics = {"total_rows": total_rows, "min_rows": min_rows}
    if total_rows < min_rows:
        return CheckResult("row_volume", FAIL,
                           f"only {total_rows} fact rows (< floor {min_rows}) — "
                           "warehouse may be empty or truncated", metrics)
    return CheckResult("row_volume", PASS, f"{total_rows} fact rows", metrics)


def _evaluate_error_rate(total_errors: int, warn_threshold: int) -> CheckResult:
    metrics = {"error_log_rows": total_errors, "warn_threshold": warn_threshold}
    if total_errors > warn_threshold:
        return CheckResult("error_rate", WARN,
                           f"error_log has {total_errors} rows (> {warn_threshold})", metrics)
    return CheckResult("error_rate", PASS, f"error_log has {total_errors} rows", metrics)


# ---------------------------------------------------------------------------
# DB wrappers — one query each, delegating to an evaluator
# ---------------------------------------------------------------------------
def _scalar(conn, sql: str) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None


def check_freshness(conn, max_staleness_days: int, today: Optional[date] = None) -> CheckResult:
    latest = _scalar(conn,
        "SELECT MAX(d.full_date) FROM fact_prices fp JOIN dim_date d ON fp.date_id = d.date_id")
    return _evaluate_freshness(latest, today or date.today(), max_staleness_days)


def check_completeness(conn) -> CheckResult:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.ticker FROM dim_stock s "
            "LEFT JOIN fact_prices fp ON s.stock_id = fp.stock_id "
            "WHERE s.is_active GROUP BY s.ticker HAVING COUNT(fp.price_id) = 0 ORDER BY s.ticker")
        missing = [r[0] for r in cur.fetchall()]
    return _evaluate_completeness(missing)


def check_duplicates(conn) -> CheckResult:
    n = _scalar(conn,
        "SELECT COUNT(*) FROM (SELECT stock_id, date_id FROM fact_prices "
        "GROUP BY stock_id, date_id HAVING COUNT(*) > 1) dups")
    return _evaluate_duplicates(int(n or 0))


def check_price_sanity(conn) -> CheckResult:
    n = _scalar(conn,
        "SELECT COUNT(*) FROM fact_prices "
        "WHERE close <= 0 OR (high IS NOT NULL AND low IS NOT NULL AND high < low)")
    return _evaluate_price_sanity(int(n or 0))


def check_row_volume(conn, min_rows: int) -> CheckResult:
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_prices")
    return _evaluate_row_volume(int(n or 0), min_rows)


def check_error_rate(conn, warn_threshold: int) -> CheckResult:
    n = _scalar(conn, "SELECT COUNT(*) FROM error_log")
    return _evaluate_error_rate(int(n or 0), warn_threshold)


def run_quality_checks(
    conn,
    *,
    max_staleness_days: Optional[int] = None,
    min_rows: Optional[int] = None,
    error_warn_threshold: Optional[int] = None,
    today: Optional[date] = None,
) -> QualityReport:
    """Run every check against ``conn`` and return a structured report.

    Defaults come from ``config.settings`` so callers (DAG / CLI / scheduler)
    stay config-free; tests pass explicit values.
    """
    from config import settings

    max_staleness_days = settings.MAX_STALENESS_DAYS if max_staleness_days is None else max_staleness_days
    min_rows = settings.MIN_FACT_ROWS if min_rows is None else min_rows
    error_warn_threshold = (settings.QUALITY_ERROR_WARN_THRESHOLD
                            if error_warn_threshold is None else error_warn_threshold)

    return QualityReport(results=[
        check_freshness(conn, max_staleness_days, today),
        check_row_volume(conn, min_rows),
        check_completeness(conn),
        check_duplicates(conn),
        check_price_sanity(conn),
        check_error_rate(conn, error_warn_threshold),
    ])


# ---------------------------------------------------------------------------
# Reporting + alerting
# ---------------------------------------------------------------------------
_ICON = {PASS: "OK  ", WARN: "WARN", FAIL: "FAIL"}


def format_report(report: QualityReport) -> str:
    lines = [f"Warehouse data-quality report — {report.worst_status}"]
    for r in report.results:
        lines.append(f"  [{_ICON[r.status]}] {r.name}: {r.detail}")
    return "\n".join(lines)


def emit_alert(report: QualityReport, webhook_url: Optional[str] = None) -> None:
    """Log the report; on FAIL, optionally POST a Slack-compatible message.

    Never raises — alerting must not crash the caller (scheduler/DAG/CLI). The
    webhook is best-effort; failures are logged.
    """
    text = format_report(report)
    (log.error if not report.ok else log.info)(text)

    if report.ok or not webhook_url:
        return
    payload = {"text": f":rotating_light: equity-pipeline data quality FAILED\n```{text}```"}
    try:
        import requests

        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code >= 300:
            log.warning("quality alert webhook returned HTTP %s", resp.status_code)
    except Exception:  # pragma: no cover - network/dep best-effort
        log.exception("quality alert webhook failed")


__all__ = [
    "CheckResult", "QualityReport", "run_quality_checks",
    "format_report", "emit_alert",
    "check_freshness", "check_completeness", "check_duplicates",
    "check_price_sanity", "check_row_volume", "check_error_rate",
]
