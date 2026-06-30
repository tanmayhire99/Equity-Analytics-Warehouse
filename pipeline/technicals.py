"""Derived technical analytics from warehoused EOD prices.

Phase B note: the project deliberately sources only NSE bhavcopy (see
``requirements.txt`` — jugaad-data replaces yfinance) and treats *fundamentals*
(P/E, market cap — which need earnings / shares-outstanding the bhavcopy lacks)
as out of scope for the warehouse (see ``consumer_api`` docstring). So rather
than bolt on a rejected external source, this adds value from the data we
already hold: moving averages, trailing returns, annualised volatility, and max
drawdown — everything derivable from the close series.

``compute_technicals`` is a **pure function** over a chronological price series,
so it is fully unit-tested with no DB. ``consumer_api.get_technicals`` feeds it
the series from the analytics layer (``fn_ticker_report`` via ``get_history``),
keeping the consumer decoupled from the physical tables.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

# Trading-day lookbacks (approx calendar: ~21/63/126 trading days = 1m/3m/6m).
_RETURN_WINDOWS = {"return_1m_pct": 21, "return_3m_pct": 63, "return_6m_pct": 126}
_SMA_WINDOWS = (20, 50, 200)
_TRADING_DAYS = 252


def _closes(series: Sequence[dict]) -> List[float]:
    """Extract the chronological close series (skipping null closes)."""
    out: List[float] = []
    for row in series:
        c = row.get("close")
        if c is not None:
            out.append(float(c))
    return out


def compute_technicals(series: Sequence[dict], *, windows: Sequence[int] = _SMA_WINDOWS) -> Dict[str, Any]:
    """Compute technical indicators from a chronological (oldest-first) series.

    ``series`` rows use the ``get_history`` shape (need at least ``close``).
    Indicators that lack enough history are returned as ``None`` rather than
    guessed.
    """
    closes = _closes(series)
    n = len(closes)
    result: Dict[str, Any] = {"data_points": n, "latest_close": closes[-1] if closes else None}
    if n == 0:
        return result

    for w in windows:
        result[f"sma_{w}"] = round(sum(closes[-w:]) / w, 4) if n >= w else None

    sma_50 = result.get("sma_50")
    result["trend_vs_sma_50"] = (
        None if sma_50 is None else ("above" if closes[-1] >= sma_50 else "below")
    )

    for key, k in _RETURN_WINDOWS.items():
        if n > k and closes[-1 - k] != 0:
            result[key] = round((closes[-1] / closes[-1 - k] - 1) * 100, 2)
        else:
            result[key] = None

    daily = [closes[i] / closes[i - 1] - 1 for i in range(1, n) if closes[i - 1] != 0]
    if len(daily) >= 2:
        mean = sum(daily) / len(daily)
        var = sum((r - mean) ** 2 for r in daily) / (len(daily) - 1)
        result["annualized_volatility_pct"] = round(math.sqrt(var) * math.sqrt(_TRADING_DAYS) * 100, 2)
    else:
        result["annualized_volatility_pct"] = None

    peak, mdd = closes[0], 0.0
    for c in closes:
        peak = max(peak, c)
        if peak > 0:
            mdd = min(mdd, c / peak - 1)
    result["max_drawdown_pct"] = round(mdd * 100, 2)

    return result


__all__ = ["compute_technicals"]
