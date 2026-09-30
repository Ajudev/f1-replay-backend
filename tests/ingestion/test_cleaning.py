"""Tests for ingestion cleaning helpers."""

from datetime import timedelta

import pandas as pd

from app.ingestion.cleaning import clean_int, is_missing, timedelta_to_ms


def test_missing_none_nan_nat_and_empty_string() -> None:
    assert is_missing(None)
    assert is_missing(float("nan"))
    assert is_missing(pd.NA)
    assert is_missing(pd.NaT)
    assert is_missing("")
    assert is_missing("   ")
    assert not is_missing(0)
    assert not is_missing("VER")


def test_timedelta_to_ms_rounds_non_negative() -> None:
    assert timedelta_to_ms(timedelta(seconds=1.2346)) == 1235
    assert timedelta_to_ms(pd.Timedelta(value=1500, unit="ms")) == 1500
    assert timedelta_to_ms(None) is None
    assert timedelta_to_ms(pd.NaT) is None


def test_negative_timedelta_returns_none() -> None:
    assert timedelta_to_ms(timedelta(seconds=-1)) is None
    assert timedelta_to_ms(pd.Timedelta(value=-5, unit="ms")) is None


def test_float_driver_number_becomes_int() -> None:
    assert clean_int(44.0) == 44
    assert clean_int(1.0) == 1
    assert clean_int(float("nan")) is None
    assert clean_int(None) is None
