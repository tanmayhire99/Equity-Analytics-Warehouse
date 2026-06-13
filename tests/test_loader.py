"""Unit tests for the loader's value coercion.

psycopg2 cannot adapt numpy scalar types (numpy.int64/float64) or NaN, so the
loader normalises them first. These tests pin that behaviour.
"""
import numpy as np

from pipeline.loader import _native


def test_none_stays_none():
    assert _native(None) is None


def test_nan_becomes_none():
    assert _native(float("nan")) is None
    assert _native(np.nan) is None


def test_numpy_int_becomes_python_int():
    value = _native(np.int64(42))
    assert value == 42
    assert isinstance(value, int)


def test_numpy_float_becomes_python_float():
    value = _native(np.float64(3.5))
    assert value == 3.5
    assert isinstance(value, float)


def test_plain_values_pass_through():
    assert _native("RELIANCE") == "RELIANCE"
    assert _native(7) == 7
