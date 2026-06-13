"""Unit tests for the validation layer — the four data-quality checks plus the
shape of the error records. Pure (no DB, no network), so they run fast and
deterministically.
"""
import datetime as dt

import pandas as pd

from pipeline.validator import validate


def _row(**overrides) -> dict:
    base = {
        "ticker": "TCS",
        "Date": pd.Timestamp("2026-01-15"),
        "Open": 92.0, "High": 100.0, "Low": 90.0,
        "Close": 95.0, "prev_close": 94.0, "vwap": 95.0, "Volume": 10000,
    }
    base.update(overrides)
    return base


def _df(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def test_valid_row_passes():
    clean, errors = validate(_df(_row()), run_id=1)
    assert len(clean) == 1
    assert errors == []


def test_null_close_rejected():
    clean, errors = validate(_df(_row(Close=None)), run_id=1)
    assert len(clean) == 0
    assert errors[0]["error_type"] == "NULL_CLOSE"


def test_zero_close_rejected():
    clean, errors = validate(_df(_row(Close=0.0)), run_id=1)
    assert len(clean) == 0
    assert errors[0]["error_type"] == "INVALID_PRICE"


def test_negative_close_rejected():
    clean, errors = validate(_df(_row(Close=-5.0)), run_id=1)
    assert errors[0]["error_type"] == "INVALID_PRICE"


def test_ohlc_invalid_rejected():
    clean, errors = validate(_df(_row(High=85.0, Low=90.0)), run_id=1)
    assert errors[0]["error_type"] == "OHLC_INVALID"


def test_price_anomaly_rejected():
    # prev_close=100, close=200 -> 100% move, beyond the 50% threshold
    clean, errors = validate(_df(_row(Close=200.0, prev_close=100.0)), run_id=1)
    assert errors[0]["error_type"] == "PRICE_ANOMALY"


def test_anomaly_not_flagged_when_prev_close_zero():
    # A zero previous close must not trigger a divide-by-zero or false anomaly.
    clean, errors = validate(_df(_row(prev_close=0.0)), run_id=1)
    assert len(clean) == 1
    assert errors == []


def test_each_row_attributed_to_first_failing_check():
    # null close AND high<low -> should be reported once, as NULL_CLOSE
    clean, errors = validate(_df(_row(Close=None, High=10.0, Low=20.0)), run_id=1)
    assert len(errors) == 1
    assert errors[0]["error_type"] == "NULL_CLOSE"


def test_multiple_rows_mixed():
    clean, errors = validate(_df(_row(), _row(Close=None)), run_id=1)
    assert len(clean) == 1
    assert len(errors) == 1


def test_error_record_payload_is_json_native():
    _, errors = validate(_df(_row(Close=None)), run_id=42)
    e = errors[0]
    assert e["pipeline_run_id"] == 42
    assert e["ticker"] == "TCS"
    assert e["error_detail"]
    assert isinstance(e["trade_date"], dt.date)
    # numpy/Timestamp values must be coerced to JSON-native types
    assert e["raw_data"]["Open"] == 92.0
    assert e["raw_data"]["Close"] is None


def test_empty_dataframe_is_safe():
    clean, errors = validate(pd.DataFrame(), run_id=1)
    assert len(clean) == 0
    assert errors == []
