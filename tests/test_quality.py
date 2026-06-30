"""Unit tests for pipeline.quality — the data-quality checks, the report
aggregation, alerting, and the thin DB wrappers (via a scripted fake conn).

Pure and offline: the decision logic is tested directly, and DB access is
exercised with a fake cursor so no PostgreSQL is required.
"""
import sys
import types
from datetime import date

import pytest

from pipeline import quality
from pipeline.quality import (
    FAIL, PASS, WARN, CheckResult, QualityReport, format_report,
)


# ---------------------------------------------------------------------------
# Pure evaluators
# ---------------------------------------------------------------------------
def test_freshness_pass_within_window():
    r = quality._evaluate_freshness(date(2026, 6, 26), date(2026, 6, 30), 5)
    assert r.status == PASS and r.metrics["staleness_days"] == 4


def test_freshness_fail_when_stale():
    r = quality._evaluate_freshness(date(2026, 6, 1), date(2026, 6, 30), 5)
    assert r.status == FAIL


def test_freshness_fail_when_no_data():
    r = quality._evaluate_freshness(None, date(2026, 6, 30), 5)
    assert r.status == FAIL and r.metrics["latest"] is None


def test_completeness_pass_and_fail():
    assert quality._evaluate_completeness([]).status == PASS
    bad = quality._evaluate_completeness(["TCS", "INFY"])
    assert bad.status == FAIL and bad.metrics["missing_count"] == 2


def test_duplicates():
    assert quality._evaluate_duplicates(0).status == PASS
    assert quality._evaluate_duplicates(3).status == FAIL


def test_price_sanity():
    assert quality._evaluate_price_sanity(0).status == PASS
    assert quality._evaluate_price_sanity(7).status == FAIL


def test_row_volume():
    assert quality._evaluate_row_volume(13000, 100).status == PASS
    assert quality._evaluate_row_volume(12, 100).status == FAIL


def test_error_rate_warns_not_fails():
    assert quality._evaluate_error_rate(5, 100).status == PASS
    assert quality._evaluate_error_rate(500, 100).status == WARN


# ---------------------------------------------------------------------------
# Report aggregation
# ---------------------------------------------------------------------------
def test_report_ok_tolerates_warn_but_not_fail():
    ok = QualityReport([CheckResult("a", PASS, ""), CheckResult("b", WARN, "")])
    assert ok.ok and ok.worst_status == WARN and ok.exit_code == 0
    bad = QualityReport([CheckResult("a", PASS, ""), CheckResult("b", FAIL, "boom")])
    assert not bad.ok and bad.worst_status == FAIL and bad.exit_code == 1
    assert [r.name for r in bad.failed] == ["b"]


def test_format_report_lists_each_check():
    rep = QualityReport([CheckResult("freshness", PASS, "latest ok"),
                         CheckResult("row_volume", FAIL, "too few")])
    text = format_report(rep)
    assert "freshness" in text and "row_volume" in text and "FAIL" in text


# ---------------------------------------------------------------------------
# Alerting (webhook is best-effort; mock the requests module)
# ---------------------------------------------------------------------------
def _mock_requests(monkeypatch, sink):
    fake = types.SimpleNamespace(
        post=lambda url, json, timeout: sink.append((url, json))
        or types.SimpleNamespace(status_code=200))
    monkeypatch.setitem(sys.modules, "requests", fake)


def test_emit_alert_posts_on_fail(monkeypatch):
    sink = []
    _mock_requests(monkeypatch, sink)
    quality.emit_alert(QualityReport([CheckResult("x", FAIL, "bad")]), webhook_url="http://hook")
    assert len(sink) == 1 and sink[0][0] == "http://hook"
    assert "FAILED" in sink[0][1]["text"]


def test_emit_alert_no_post_when_ok(monkeypatch):
    sink = []
    _mock_requests(monkeypatch, sink)
    quality.emit_alert(QualityReport([CheckResult("x", PASS, "ok")]), webhook_url="http://hook")
    assert sink == []


def test_emit_alert_no_webhook_is_safe():
    quality.emit_alert(QualityReport([CheckResult("x", FAIL, "bad")]), webhook_url=None)  # no raise


# ---------------------------------------------------------------------------
# DB wrappers via a scripted fake connection (SQL-substring -> canned result)
# ---------------------------------------------------------------------------
class _ScriptedCursor:
    def __init__(self, rules):
        self._rules, self._one, self._all = rules, None, []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        for sub, one, all_ in self._rules:
            if sub in sql:
                self._one, self._all = one, all_
                return
        self._one, self._all = None, []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _ScriptedConn:
    def __init__(self, rules):
        self._rules = rules

    def cursor(self):
        return _ScriptedCursor(self._rules)


def test_check_freshness_wrapper_reads_conn():
    conn = _ScriptedConn([("MAX(d.full_date)", (date(2026, 6, 29),), None)])
    r = quality.check_freshness(conn, max_staleness_days=5, today=date(2026, 6, 30))
    assert r.status == PASS


def test_run_quality_checks_all_green(monkeypatch):
    # A healthy warehouse: fresh, complete, no dupes, sane prices, plenty of rows.
    rules = [
        ("MAX(d.full_date)", (date(2026, 6, 29),), None),       # freshness
        ("COUNT(fp.price_id) = 0", None, []),                   # completeness: none missing
        ("HAVING COUNT(*) > 1", (0,), None),                    # duplicates
        ("close <= 0", (0,), None),                             # price_sanity
        ("FROM error_log", (3,), None),                         # error_rate
        ("FROM fact_prices", (13000,), None),                   # row_volume (matched last)
    ]
    report = quality.run_quality_checks(
        _ScriptedConn(rules), max_staleness_days=5, min_rows=100,
        error_warn_threshold=100, today=date(2026, 6, 30))
    assert report.ok and report.worst_status == PASS
    assert {r.name for r in report.results} == {
        "freshness", "completeness", "duplicates", "price_sanity", "row_volume", "error_rate"}


def test_run_quality_checks_flags_stale_and_empty():
    rules = [
        ("MAX(d.full_date)", (date(2026, 5, 1),), None),        # freshness: stale
        ("COUNT(fp.price_id) = 0", None, [("TCS",)]),           # completeness: missing
        ("HAVING COUNT(*) > 1", (0,), None),
        ("close <= 0", (0,), None),
        ("FROM error_log", (0,), None),
        ("FROM fact_prices", (5,), None),                       # row_volume: below floor
    ]
    report = quality.run_quality_checks(
        _ScriptedConn(rules), max_staleness_days=5, min_rows=100,
        error_warn_threshold=100, today=date(2026, 6, 30))
    assert not report.ok
    failed = {r.name for r in report.failed}
    assert {"freshness", "completeness", "row_volume"} <= failed


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
