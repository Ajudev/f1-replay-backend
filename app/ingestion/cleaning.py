"""NaN/NaT/timedelta cleaning helpers for FastF1/pandas values.

pandas may be imported in this module only within the ingestion package.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd


def is_missing(value: Any) -> bool:
    """Return True for None, NaN, NaT, and empty/whitespace strings."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return False


def clean_optional_str(value: Any) -> str | None:
    """Return a stripped string, or None when missing."""
    if is_missing(value):
        return None
    text = str(value).strip()
    return text if text else None


def clean_bool(value: Any) -> bool | None:
    """Return a bool, or None when missing."""
    if is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    return bool(value)


def clean_int(value: Any) -> int | None:
    """Convert whole-number floats (e.g. 44.0) to int; missing → None.

    Never returns float NaN.
    """
    if is_missing(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN guard
            return None
        if value.is_integer():
            return int(value)
        return int(round(value))
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return None
    if as_float != as_float:
        return None
    if as_float.is_integer():
        return int(as_float)
    return int(round(as_float))


def timedelta_to_ms(value: Any) -> int | None:
    """Convert a timedelta / pandas Timedelta to non-negative int milliseconds.

    Uses ``round(total_seconds * 1000)``. Negative or missing → None.
    """
    if is_missing(value):
        return None

    total_seconds: float | None = None
    if isinstance(value, (pd.Timedelta, timedelta)):
        total_seconds = value.total_seconds()
    else:
        try:
            td = pd.to_timedelta(value)
        except (TypeError, ValueError):
            return None
        if is_missing(td):
            return None
        total_seconds = td.total_seconds()

    if total_seconds is None or total_seconds != total_seconds:
        return None
    if total_seconds < 0:
        return None
    return int(round(total_seconds * 1000))


def to_aware_datetime(value: Any) -> datetime | None:
    """Convert a value to a timezone-aware datetime when possible."""
    if is_missing(value):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value
        return value.replace(tzinfo=datetime.now().astimezone().tzinfo)
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if is_missing(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


def to_date(value: Any) -> date | None:
    """Convert a value to ``datetime.date`` when possible, else None."""
    if is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if is_missing(ts):
        return None
    return ts.date()
