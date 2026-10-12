"""PositionTrack: interpolated positions and stepped telemetry at a race time."""

from __future__ import annotations

from uuid import UUID

import pytest

from app.domain.enums import DrsState
from app.positions.track import PositionTrack, TrackRow

A, B = UUID(int=1), UUID(int=2)


def row(
    driver: UUID, t: int, x: int | None = None, y: int | None = None, **tel: object
) -> TrackRow:
    fields = {
        "speed_kph": None,
        "rpm": None,
        "gear": None,
        "throttle_pct": None,
        "brake": None,
        "drs_code": None,
    } | tel
    return TrackRow(driver, t, x, y, False if x is not None else None, **fields)  # type: ignore[arg-type]


def track(*rows: TrackRow, max_gap_ms: int = 3_000) -> PositionTrack:
    return PositionTrack(rows, max_gap_ms=max_gap_ms, order=[A, B])


def test_positions_are_interpolated_between_samples() -> None:
    t = track(row(A, 0, 0, 100), row(A, 500, 1000, 300))
    [car] = t.frame_at(250)
    assert (car.x, car.y) == (500, 200)
    assert t.frame_at(125)[0].x == 250


def test_an_exact_sample_is_returned_as_stored() -> None:
    t = track(row(A, 0, 0, 0), row(A, 500, 1000, 300))
    assert (t.frame_at(500)[0].x, t.frame_at(500)[0].y) == (1000, 300)
    assert (t.frame_at(0)[0].x, t.frame_at(0)[0].y) == (0, 0)


def test_no_position_outside_the_data_or_across_a_gap() -> None:
    t = track(row(A, 0, 0, 0), row(A, 500, 10, 10), row(A, 10_000, 20, 20))
    assert t.frame_at(-1)[0].x is None
    assert t.frame_at(10_001)[0].x is None
    assert t.frame_at(5_000)[0].x is None  # 9.5 s between samples


def test_telemetry_steps_and_is_never_interpolated() -> None:
    t = track(
        row(A, 0, speed_kph=100, rpm=10_000, gear=3, throttle_pct=50, brake=False, drs_code=1),
        row(A, 500, speed_kph=200, rpm=12_000, gear=5, throttle_pct=100, brake=True, drs_code=12),
    )
    tel = t.frame_at(499)[0].telemetry
    assert tel is not None
    assert (tel.speed_kph, tel.gear, tel.brake, tel.drs) == (100, 3, False, DrsState.OFF)
    tel = t.frame_at(500)[0].telemetry
    assert tel is not None and (tel.speed_kph, tel.drs) == (200, DrsState.OPEN)
    assert t.frame_at(-1)[0].telemetry is None
    assert t.frame_at(3_500)[0].telemetry is not None  # exactly the max gap
    assert t.frame_at(3_501)[0].telemetry is None  # last sample older than the max gap


def test_position_and_telemetry_are_independent() -> None:
    t = track(
        row(A, 0, 0, 0), row(A, 500, 10, 0), row(B, 0, speed_kph=90), row(B, 500, speed_kph=95)
    )
    assert t.positions_available and t.telemetry_available
    a, b = t.frame_at(250)
    assert a.telemetry is None and a.x == 5
    assert b.x is None and b.telemetry is not None and b.telemetry.speed_kph == 90


def test_frames_follow_the_given_driver_order_and_skip_drivers_without_data() -> None:
    t = PositionTrack(
        [row(A, 0, 1, 1), row(B, 0, 2, 2)], max_gap_ms=3_000, order=[B, UUID(int=9), A]
    )
    assert [c.driver_id for c in t.frame_at(0)] == [B, A]


def test_payload_shape() -> None:
    t = track(row(A, 0, 1, 2, speed_kph=300, gear=8, drs_code=8))
    assert t.frame_at(0)[0].to_payload() == {
        "driver_id": str(A),
        "x": 1,
        "y": 2,
        "off_track": False,
        "telemetry": {
            "speed_kph": 300,
            "rpm": None,
            "gear": 8,
            "throttle_pct": None,
            "brake": None,
            "drs": "ELIGIBLE",
        },
    }


@pytest.mark.parametrize(
    ("code", "state"),
    [
        (None, None),
        (0, DrsState.OFF),
        (1, DrsState.OFF),
        (8, DrsState.ELIGIBLE),
        (10, DrsState.OPEN),
        (12, DrsState.OPEN),
        (14, DrsState.OPEN),
        (2, DrsState.UNKNOWN),
        (9, DrsState.UNKNOWN),
    ],
)
def test_drs_code_mapping(code: int | None, state: DrsState | None) -> None:
    assert DrsState.from_code(code) is state
