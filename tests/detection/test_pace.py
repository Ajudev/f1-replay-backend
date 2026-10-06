"""Robust statistics, the clean-lap filter's building blocks and the track status history."""

from __future__ import annotations

import pytest

from app.detection.config import PaceConfig
from app.detection.pace import MAD_SCALE, mad, median, robust_z
from app.detection.track_status import TrackStatusHistory, WindowCondition
from app.domain.enums import TrackStatus

ABNORMAL = PaceConfig().abnormal_statuses


def test_median_and_mad() -> None:
    assert median([3, 1, 2]) == 2
    assert median([1, 2, 3, 4]) == 2.5
    assert mad([90, 92, 94, 96, 98]) == 2
    assert mad([1, 1, 1, 1]) == 0


def test_robust_z_uses_the_scaled_mad_and_a_floor() -> None:
    assert robust_z(110, 100, 5, mad_floor=1) == pytest.approx(10 / (MAD_SCALE * 5))
    # A zero MAD would divide by zero; the floor bounds the score.
    assert robust_z(110, 100, 0, mad_floor=10) == pytest.approx(10 / (MAD_SCALE * 10))
    assert robust_z(90, 100, 5, mad_floor=1) < 0


def history(*changes: tuple[int, TrackStatus]) -> TrackStatusHistory:
    h = TrackStatusHistory()
    for time, status in changes:
        h.record(status, time, limit=100)
    return h


def test_history_records_only_changes_and_is_bounded() -> None:
    h = TrackStatusHistory()
    assert h.record(TrackStatus.GREEN, 0, limit=3)
    assert not h.record(TrackStatus.GREEN, 10, limit=3)
    assert not h.record(None, 10, limit=3)
    for time, status in [
        (20, TrackStatus.YELLOW),
        (30, TrackStatus.GREEN),
        (40, TrackStatus.RED_FLAG),
    ]:
        h.record(status, time, limit=3)
    assert [c.race_time_ms for c in h.changes] == [20, 30, 40]


def test_status_at_returns_the_status_in_effect() -> None:
    h = history((0, TrackStatus.GREEN), (100, TrackStatus.SAFETY_CAR))
    assert h.status_at(-1) is None
    assert h.status_at(99) is TrackStatus.GREEN
    assert h.status_at(100) is TrackStatus.SAFETY_CAR


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (10, 90, WindowCondition.GREEN),
        (10, 110, WindowCondition.ABNORMAL),  # SC starts inside the window
        (150, 250, WindowCondition.ABNORMAL),  # SC active at the start
        (180, 260, WindowCondition.ABNORMAL),  # SC ends inside the window
        (210, 290, WindowCondition.GREEN),
        (120, 140, WindowCondition.ABNORMAL),  # entirely under SC
    ],
)
def test_a_window_is_abnormal_when_any_part_of_it_was(
    start: int, end: int, expected: WindowCondition
) -> None:
    h = history((0, TrackStatus.GREEN), (100, TrackStatus.SAFETY_CAR), (200, TrackStatus.GREEN))

    assert h.classify_window(start, end, ABNORMAL) is expected


def test_yellow_is_abnormal_only_when_configured() -> None:
    h = history((0, TrackStatus.GREEN), (50, TrackStatus.YELLOW), (60, TrackStatus.GREEN))

    assert h.classify_window(0, 100, PaceConfig().abnormal_statuses) is WindowCondition.ABNORMAL
    relaxed = PaceConfig(exclude_yellow=False).abnormal_statuses
    assert h.classify_window(0, 100, relaxed) is WindowCondition.GREEN


def test_a_window_the_history_does_not_cover_is_unknown_unless_the_lap_says_otherwise() -> None:
    empty = TrackStatusHistory()
    assert empty.classify_window(10, 90, ABNORMAL) is WindowCondition.UNKNOWN
    assert empty.classify_window(10, 90, ABNORMAL, lap_status="UNKNOWN") is WindowCondition.UNKNOWN
    assert empty.classify_window(10, 90, ABNORMAL, lap_status="GREEN") is WindowCondition.GREEN
    # The lap's own abnormal status is never overridden by a green history.
    green = history((0, TrackStatus.GREEN))
    assert (
        green.classify_window(10, 90, ABNORMAL, lap_status="SAFETY_CAR") is WindowCondition.ABNORMAL
    )
    # Without a window start the window is unknown (unless something abnormal is known).
    assert green.classify_window(None, 90, ABNORMAL) is WindowCondition.UNKNOWN


def test_an_unknown_status_inside_the_window_is_unknown() -> None:
    h = history((0, TrackStatus.GREEN), (50, TrackStatus.UNKNOWN))

    assert h.classify_window(10, 90, ABNORMAL) is WindowCondition.UNKNOWN
