"""Stage 2 — Validate.

Runs four data-quality checks on every fetched row *before* anything touches the
warehouse. Rows that fail are returned as structured error records (destined for
the error_log table); rows that pass are returned as a clean DataFrame. Nothing
is ever silently dropped.

Detection is vectorised with boolean masks (Pandas evaluates these in C), so the
logic scales to millions of rows. Only the construction of error records
iterates — and only over the small set of rows that actually failed.

Checks (each row is attributed to its first failing check):
    1. NULL_CLOSE     — close price is null
    2. INVALID_PRICE  — close <= 0
    3. OHLC_INVALID   — high < low (physically impossible)
    4. PRICE_ANOMALY  — single-day move > 50% vs previous close
                        (Indian circuit breakers cap at 20%, so 50% is clearly
                        bad data rather than a real move)
"""
from __future__ import annotations

import pandas as pd

# A single-day move beyond this fraction is treated as bad data, not a real move.
PRICE_ANOMALY_THRESHOLD = 0.5


def _row_to_jsonable(row: pd.Series) -> dict:
    """Convert a row to a dict of JSON-native types for the error_log payload."""
    out: dict = {}
    for key, value in row.items():
        if pd.isna(value):
            out[key] = None
        elif isinstance(value, pd.Timestamp):
            out[key] = value.strftime("%Y-%m-%d")
        elif hasattr(value, "item"):  # numpy scalar -> python scalar
            out[key] = value.item()
        else:
            out[key] = value
    return out


def _trade_date(row: pd.Series):
    value = row.get("Date")
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _make_error(row: pd.Series, run_id: int, error_type: str, detail: str) -> dict:
    return {
        "pipeline_run_id": run_id,
        "ticker": row.get("ticker"),
        "trade_date": _trade_date(row),
        "raw_data": _row_to_jsonable(row),
        "error_type": error_type,
        "error_detail": detail,
    }


def validate(df: pd.DataFrame, run_id: int) -> tuple[pd.DataFrame, list[dict]]:
    """Split `df` into (clean_rows, error_records).

    Returns a clean DataFrame (rows that passed every check) and a list of error
    dicts ready to insert into error_log.
    """
    if df.empty:
        return df.copy(), []

    errors: list[dict] = []
    bad = pd.Series(False, index=df.index)

    # Check 1: null close --------------------------------------------------
    null_close = df["Close"].isna()
    for idx in df.index[null_close]:
        errors.append(_make_error(df.loc[idx], run_id, "NULL_CLOSE", "Close price is null"))
    bad |= null_close

    # Check 2: invalid price (<= 0) ---------------------------------------
    invalid_price = (df["Close"] <= 0) & ~bad
    for idx in df.index[invalid_price]:
        row = df.loc[idx]
        errors.append(_make_error(row, run_id, "INVALID_PRICE", f"Close price is {row['Close']}"))
    bad |= invalid_price

    # Check 3: OHLC invalid (high < low) ----------------------------------
    ohlc_invalid = (df["High"] < df["Low"]) & ~bad
    for idx in df.index[ohlc_invalid]:
        row = df.loc[idx]
        errors.append(
            _make_error(row, run_id, "OHLC_INVALID", f"High ({row['High']}) < Low ({row['Low']})")
        )
    bad |= ohlc_invalid

    # Check 4: price anomaly (> 50% single-day move) ----------------------
    if "prev_close" in df.columns:
        prev = df["prev_close"]
        movable = prev.notna() & (prev != 0)
        move = (df["Close"] / prev - 1).abs()
        anomaly = movable & (move > PRICE_ANOMALY_THRESHOLD) & ~bad
        for idx in df.index[anomaly]:
            row = df.loc[idx]
            errors.append(
                _make_error(
                    row, run_id, "PRICE_ANOMALY",
                    f"Close moved {move[idx] * 100:.1f}% from previous close",
                )
            )
        bad |= anomaly

    clean_df = df[~bad].reset_index(drop=True)
    return clean_df, errors
