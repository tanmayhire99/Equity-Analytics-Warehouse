"""Unit tests for pipeline.technicals.compute_technicals — pure, no DB/network."""
from pipeline.technicals import compute_technicals


def _series(closes):
    return [{"trade_date": f"2026-01-{i+1:02d}", "close": c} for i, c in enumerate(closes)]


def test_empty_series():
    out = compute_technicals([])
    assert out["data_points"] == 0 and out["latest_close"] is None


def test_increasing_series_smas_and_trend():
    out = compute_technicals(_series(list(range(1, 201))))  # closes 1..200
    assert out["data_points"] == 200
    assert out["latest_close"] == 200
    assert out["sma_20"] == 190.5
    assert out["sma_50"] == 175.5
    assert out["sma_200"] == 100.5
    assert out["trend_vs_sma_50"] == "above"
    assert out["max_drawdown_pct"] == 0.0          # monotonic increasing
    assert out["return_1m_pct"] is not None and out["return_1m_pct"] > 0
    assert out["annualized_volatility_pct"] is not None


def test_drawdown_is_peak_to_trough():
    out = compute_technicals(_series([100, 110, 120, 90, 95]))
    assert out["latest_close"] == 95
    assert out["max_drawdown_pct"] == -25.0        # 90 vs peak 120
    assert out["sma_20"] is None                   # not enough history
    assert out["sma_50"] is None and out["sma_200"] is None


def test_partial_history_sets_available_smas_only():
    out = compute_technicals(_series([float(x) for x in range(1, 31)]))  # 30 points
    assert out["sma_20"] is not None
    assert out["sma_50"] is None and out["sma_200"] is None
    assert out["trend_vs_sma_50"] is None          # depends on sma_50


def test_null_closes_skipped():
    out = compute_technicals([{"close": None}, {"close": 10.0}, {"close": 12.0}])
    assert out["data_points"] == 2 and out["latest_close"] == 12.0


def test_flat_series_zero_volatility_and_drawdown():
    out = compute_technicals(_series([50.0] * 10))
    assert out["annualized_volatility_pct"] == 0.0
    assert out["max_drawdown_pct"] == 0.0
    assert out["trend_vs_sma_50"] is None          # <50 points, no sma_50
