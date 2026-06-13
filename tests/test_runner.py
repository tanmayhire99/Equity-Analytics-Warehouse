"""Unit tests for the orchestration logic in pipeline.runner.

All I/O dependencies (DB, fetch) are monkeypatched so these verify the status
transitions (SUCCESS / PARTIAL / FAILED) and the C-3 guarantee (counters are
safe even when fetch raises) without touching Postgres or the network.
"""
import pandas as pd

import pipeline.runner as runner


class _FakeConn:
    def close(self): ...
    def rollback(self): ...
    def commit(self): ...


def _patch_db(monkeypatch):
    monkeypatch.setattr(runner, "get_connection", lambda: _FakeConn())
    monkeypatch.setattr(runner, "create_run_placeholder", lambda conn: 1)
    # update_run returns the run_id it was given (positional arg index 1)
    monkeypatch.setattr(runner, "update_run", lambda *a, **k: a[1] or 1)
    monkeypatch.setattr(runner, "transform", lambda clean, conn: clean)
    monkeypatch.setattr(runner, "load", lambda ready, conn: len(ready))
    monkeypatch.setattr(runner, "insert_errors", lambda conn, errors: len(errors))


def test_status_success_when_no_rejects(monkeypatch):
    _patch_db(monkeypatch)
    df = pd.DataFrame([{"ticker": "X"}])
    monkeypatch.setattr(runner, "fetch_all", lambda tickers, lookback_days: df)
    monkeypatch.setattr(runner, "validate", lambda raw, run_id: (raw, []))

    res = runner.run_pipeline(tickers=["X"], lookback_days=5)
    assert res["status"] == "SUCCESS"
    assert res["rows_inserted"] == 1
    assert res["rows_rejected"] == 0


def test_status_partial_when_rows_rejected(monkeypatch):
    _patch_db(monkeypatch)
    df = pd.DataFrame([{"ticker": "X"}, {"ticker": "Y"}])
    monkeypatch.setattr(runner, "fetch_all", lambda tickers, lookback_days: df)
    # one clean row, one rejected
    monkeypatch.setattr(
        runner, "validate",
        lambda raw, run_id: (raw.iloc[:1], [{"pipeline_run_id": run_id}]),
    )

    res = runner.run_pipeline(tickers=["X"], lookback_days=5)
    assert res["status"] == "PARTIAL"
    assert res["rows_rejected"] == 1


def test_status_failed_and_counters_safe_when_fetch_raises(monkeypatch):
    _patch_db(monkeypatch)

    def boom(*a, **k):
        raise RuntimeError("simulated NSE outage")

    monkeypatch.setattr(runner, "fetch_all", boom)

    res = runner.run_pipeline(tickers=["X"], lookback_days=5)
    assert res["status"] == "FAILED"
    # C-3: counters initialised before try, so no NameError and they stay 0
    assert res["rows_fetched"] == 0
    assert res["rows_inserted"] == 0
