"""Shared pace utilities: robust statistics and the clean-lap filter.

A *clean* lap is one that says something about the car's pace: it has a time, is not
deleted, is not lap 1 (standing start), is not an in or out lap, was driven entirely
in conditions that are not neutralised (safety car, virtual safety car, red flag and,
configurably, yellow flags), and is not one of the first laps after such a period
(restart effects). A lap whose track status cannot be established is *not* clean:
missing information is never read as "green".

Per-driver lap histories are kept by the detectors in their own memory, keyed by stint:
a new stint (different stint number, a pit-out lap, or younger tyres) empties it, so
pace is only ever compared within one set of tyres. The race state's ``recent_laps`` is
too short (10 laps) for that.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from enum import StrEnum

from pydantic import BaseModel, Field

from app.detection.models import DetectorInput
from app.detection.track_status import WindowCondition
from app.race_state.models import DriverState, LapRecord

#: Scale factor making the MAD comparable to a standard deviation for normal data.
MAD_SCALE = 1.4826


def median(values: Sequence[float]) -> float:
    return float(statistics.median(values))


def mad(values: Sequence[float], center: float | None = None) -> float:
    """Median absolute deviation around ``center`` (default: the median)."""
    mid = median(values) if center is None else center
    return median([abs(v - mid) for v in values])


def robust_z(value: float, center: float, mad_value: float, *, mad_floor: float) -> float:
    """``(value - center) / (1.4826 * max(MAD, floor))``; the floor keeps a near-zero
    MAD (very consistent laps) from turning small deviations into huge scores."""
    return (value - center) / (MAD_SCALE * max(mad_value, mad_floor))


class LapExclusion(StrEnum):
    NO_LAP_TIME = "NO_LAP_TIME"
    DELETED = "DELETED"
    FIRST_LAP = "FIRST_LAP"
    PIT_IN = "PIT_IN"
    PIT_OUT = "PIT_OUT"
    TRACK_STATUS = "TRACK_STATUS"
    TRACK_STATUS_UNKNOWN = "TRACK_STATUS_UNKNOWN"
    RESTART = "RESTART"
    #: Not a new lap for the history (already seen, or older than the newest seen).
    ALREADY_SEEN = "ALREADY_SEEN"


class CleanLap(BaseModel):
    lap_number: int
    lap_time_ms: int
    race_time_ms: int
    stint_number: int | None = None
    compound: str | None = None
    tyre_age_laps: int | None = None


class DriverPaceHistory(BaseModel):
    """Bounded clean-lap history of one driver within one stint."""

    stint_number: int | None = None
    last_lap: int | None = None
    #: Last lap whose window overlapped an excluded track status.
    neutral_lap: int | None = None
    laps: list[CleanLap] = Field(default_factory=list)

    def reset_stint(self) -> None:
        self.laps = []

    def add(self, record: LapRecord, *, keep: int) -> CleanLap:
        assert record.lap_time_ms is not None
        lap = CleanLap(
            lap_number=record.lap_number,
            lap_time_ms=record.lap_time_ms,
            race_time_ms=record.race_time_ms,
            stint_number=record.stint_number,
            compound=record.compound,
            tyre_age_laps=record.tyre_age_laps,
        )
        self.laps.append(lap)
        del self.laps[:-keep]
        return lap


def lap_window_condition(
    data: DetectorInput, driver: DriverState, record: LapRecord
) -> WindowCondition:
    """Track condition over the whole lap ``record`` (not just at its end)."""
    start: int | None = None
    if record.lap_time_ms is not None:
        start = record.race_time_ms - record.lap_time_ms
    else:
        previous = data.lap_record(driver, record.lap_number - 1)
        if previous is not None:
            start = previous.race_time_ms
    return data.track_history.classify_window(
        start,
        record.race_time_ms,
        data.config.pace.abnormal_statuses,
        lap_status=record.track_status,
    )


def observe_lap(
    history: DriverPaceHistory,
    data: DetectorInput,
    driver: DriverState,
    record: LapRecord,
    *,
    check_restart: bool = True,
) -> LapExclusion | None:
    """Register ``record`` with ``history`` and say why it is not clean (``None`` = clean).

    Handles stint changes and neutralisation bookkeeping; it does not add the lap to
    ``history.laps`` (the caller decides, e.g. an anomalous lap stays out).
    """
    if history.last_lap is not None and record.lap_number <= history.last_lap:
        return LapExclusion.ALREADY_SEEN

    new_stint = (
        record.stint_number is not None
        and history.stint_number is not None
        and record.stint_number != history.stint_number
    )
    younger_tyres = (
        record.tyre_age_laps is not None
        and bool(history.laps)
        and history.laps[-1].tyre_age_laps is not None
        and record.tyre_age_laps < (history.laps[-1].tyre_age_laps or 0)
    )
    if new_stint or younger_tyres or record.is_pit_out_lap is True:
        history.reset_stint()
    if record.stint_number is not None:
        history.stint_number = record.stint_number
    history.last_lap = record.lap_number

    condition = lap_window_condition(data, driver, record)
    if condition is WindowCondition.ABNORMAL:
        history.neutral_lap = record.lap_number

    if record.lap_time_ms is None:
        return LapExclusion.NO_LAP_TIME
    if record.is_deleted is True:
        return LapExclusion.DELETED
    if record.lap_number <= 1:
        return LapExclusion.FIRST_LAP
    if record.is_pit_in_lap is True:
        return LapExclusion.PIT_IN
    if record.is_pit_out_lap is True:
        return LapExclusion.PIT_OUT
    if condition is WindowCondition.ABNORMAL:
        return LapExclusion.TRACK_STATUS
    if condition is WindowCondition.UNKNOWN:
        return LapExclusion.TRACK_STATUS_UNKNOWN
    if (
        check_restart
        and history.neutral_lap is not None
        and 0 < record.lap_number - history.neutral_lap <= data.config.pace.restart_laps
    ):
        return LapExclusion.RESTART
    return None


def is_contiguous_enough(laps: Sequence[CleanLap], needed: int, max_excluded: int) -> bool:
    """The newest ``needed`` clean laps span at most ``needed + max_excluded`` lap numbers."""
    if len(laps) < needed:
        return False
    window = laps[-needed:]
    return window[-1].lap_number - window[0].lap_number + 1 <= needed + max_excluded
