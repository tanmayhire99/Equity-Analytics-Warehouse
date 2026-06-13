"""Unit test for the NSE trade-date correction.

This guards the most dangerous silent bug in the pipeline: NSE returns
CH_TIMESTAMP as IST-midnight encoded in UTC (previous day 18:30), so without
correction every price lands on the wrong day and some on weekends (which have
no dim_date trading rows, causing silent FK drops).
"""
import pandas as pd

from pipeline.fetcher import correct_trade_dates


def test_corrects_utc_encoded_ist_midnight():
    raw = pd.Series(pd.to_datetime([
        "2026-06-11 18:30:00",  # -> 2026-06-12 (Friday)
        "2026-06-07 18:30:00",  # -> 2026-06-08 (Monday)
    ]))
    fixed = correct_trade_dates(raw)
    assert fixed.iloc[0] == pd.Timestamp("2026-06-12")
    assert fixed.iloc[1] == pd.Timestamp("2026-06-08")


def test_correction_produces_no_weekend_dates():
    # A week of consecutive NSE timestamps (Mon-Fri trading) must stay on weekdays.
    raw = pd.Series(pd.to_datetime([
        "2026-06-07 18:30:00",  # Mon
        "2026-06-08 18:30:00",  # Tue
        "2026-06-09 18:30:00",  # Wed
        "2026-06-10 18:30:00",  # Thu
        "2026-06-11 18:30:00",  # Fri
    ]))
    fixed = correct_trade_dates(raw)
    assert not fixed.dt.day_name().isin(["Saturday", "Sunday"]).any()


def test_correction_normalises_to_midnight():
    raw = pd.Series(pd.to_datetime(["2026-06-11 18:30:00"]))
    fixed = correct_trade_dates(raw)
    assert (fixed.dt.hour == 0).all()
    assert (fixed.dt.minute == 0).all()
