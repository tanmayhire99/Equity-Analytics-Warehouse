"""Integration tests for the consumer API.

These hit the warehouse, so they skip automatically when no database is
reachable (e.g. in CI without a Postgres service) — keeping the suite green
everywhere while still exercising the real contract locally.
"""
import datetime as dt

import pytest

import consumer_api
from db.connection import get_connection


@pytest.fixture(scope="module")
def conn():
    try:
        c = get_connection()
        with c.cursor() as cur:
            cur.execute("SELECT 1 FROM mv_stock_snapshot LIMIT 1")
    except Exception:
        pytest.skip("warehouse not available (no DB / not set up)")
    yield c
    c.close()


def test_get_quote_returns_jsonable_dict_or_none(conn):
    q = consumer_api.get_quote("RELIANCE", conn=conn)
    if q is not None:
        assert q["ticker"] == "RELIANCE"
        assert isinstance(q["latest_close"], (float, type(None)))
        assert isinstance(q["latest_date"], (str, type(None)))


def test_get_quote_unknown_ticker_is_none(conn):
    assert consumer_api.get_quote("NOSUCHTICKER", conn=conn) is None


def test_get_history_returns_rows_with_expected_keys(conn):
    rows = consumer_api.get_history("TCS", conn=conn)
    if rows:
        assert {"trade_date", "close", "ma_7d", "ma_30d"} <= set(rows[0])
        assert isinstance(rows[0]["close"], float)


def test_get_history_unknown_ticker_is_empty(conn):
    assert consumer_api.get_history("NOSUCHTICKER", conn=conn) == []


def test_get_technicals_shape_or_none(conn):
    t = consumer_api.get_technicals("RELIANCE", conn=conn)
    if t is not None:
        assert t["ticker"] == "RELIANCE"
        assert "sma_20" in t and "max_drawdown_pct" in t
        assert "annualized_volatility_pct" in t
        assert isinstance(t["data_points"], int)


def test_get_technicals_unknown_ticker_is_none(conn):
    assert consumer_api.get_technicals("NOSUCHTICKER", conn=conn) is None


def test_get_top_movers_respects_limit_and_order(conn):
    movers = consumer_api.get_top_movers(limit=3, conn=conn)
    assert len(movers) <= 3
    returns = [m["return_30d_pct"] for m in movers if m["return_30d_pct"] is not None]
    assert returns == sorted(returns, reverse=True)
