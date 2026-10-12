"""Track data normalization: race-time grid, laps, outline, warnings."""

from __future__ import annotations

from app.ingestion.records import (
    CarSeries,
    ExtractedCircuit,
    ExtractedCorner,
    ExtractedDriverTrack,
    ExtractedTrackData,
    PositionSeries,
    TrackDriverInfo,
    TrackLapWindow,
)
from app.ingestion.track_normalizer import MAX_OUTLINE_POINTS, normalize_track_data

EPOCH = 1_000_000


def pos(times: list[int], *, x0: int = 0) -> PositionSeries:
    return PositionSeries(
        session_time_ms=times,
        x=[x0 + i for i in range(len(times))],
        y=[-i for i in range(len(times))],
        off_track=[False] * len(times),
    )


def car(times: list[int], **overrides: list) -> CarSeries:
    n = len(times)
    values = {
        "speed_kph": [200] * n,
        "rpm": [11_000] * n,
        "gear": [7] * n,
        "throttle_pct": [100] * n,
        "brake": [False] * n,
        "drs_code": [8] * n,
    } | overrides
    return CarSeries(session_time_ms=times, **values)


def laps(n: int = 2, lap_ms: int = 10_000) -> list[TrackLapWindow]:
    return [
        TrackLapWindow(
            lap_number=i,
            end_ms=EPOCH + i * lap_ms,
            lap_time_ms=lap_ms,
            start_ms=EPOCH + (i - 1) * lap_ms,
            usable_for_outline=True,
        )
        for i in range(1, n + 1)
    ]


def data(
    *drivers: ExtractedDriverTrack, circuit: ExtractedCircuit | None = None
) -> ExtractedTrackData:
    return ExtractedTrackData(drivers=list(drivers), circuit=circuit)


def driver(abbr: str = "VER", **kwargs: object) -> ExtractedDriverTrack:
    values = {"driver_number": 1, "team_color": "3671c6", "positions": None, "car": None} | kwargs
    return ExtractedDriverTrack(abbreviation=abbr, **values)  # type: ignore[arg-type]


def run(extracted: ExtractedTrackData, infos: list[TrackDriverInfo] | None = None, step: int = 500):
    return normalize_track_data(
        extracted,
        drivers=infos if infos is not None else [TrackDriverInfo("VER", laps())],
        epoch_ms=EPOCH,
        sample_interval_ms=step,
    )


def test_samples_are_on_the_race_time_grid_with_nearest_values() -> None:
    times = [EPOCH - 10, EPOCH + 260, EPOCH + 480, EPOCH + 1_010]
    result = run(data(driver(positions=pos(times))))
    assert [(s.race_time_ms, s.x) for s in result.samples] == [(0, 0), (500, 2), (1_000, 3)]
    assert all(s.race_time_ms % 500 == 0 for s in result.samples)


def test_gaps_are_kept_not_filled() -> None:
    times = [EPOCH, EPOCH + 500, EPOCH + 5_000]
    result = run(data(driver(positions=pos(times))))
    assert [s.race_time_ms for s in result.samples] == [0, 500, 5_000]
    # A raw sample only fills the grid point within half an interval: 1 000-4 500 stay empty.


def test_misaligned_positions_and_telemetry_share_one_row_per_grid_point() -> None:
    result = run(
        data(
            driver(
                positions=pos([EPOCH + 30, EPOCH + 530]),
                car=car([EPOCH + 210, EPOCH + 470, EPOCH + 2_000]),
            )
        )
    )
    rows = [(s.race_time_ms, s.x is not None, s.speed_kph is not None) for s in result.samples]
    assert rows == [(0, True, True), (500, True, True), (2_000, False, True)]
    assert result.position_sample_count == 2
    assert result.telemetry_sample_count == 3


def test_lap_numbers_follow_stored_lap_ends() -> None:
    times = [EPOCH - 1_000, EPOCH + 5_000, EPOCH + 10_000, EPOCH + 15_000, EPOCH + 30_000]
    result = run(data(driver(positions=pos(times))))
    assert {s.race_time_ms: s.lap_number for s in result.samples} == {
        -1_000: None,
        5_000: 1,
        10_000: 1,
        15_000: 2,
        30_000: None,  # after the last stored lap
    }


def test_samples_outside_the_race_window_are_dropped() -> None:
    times = [EPOCH - 120_000, EPOCH, EPOCH + 20_000 + 61_000]
    result = run(data(driver(positions=pos(times))))
    assert [s.race_time_ms for s in result.samples] == [0]


def test_missing_sources_are_reported_not_fabricated() -> None:
    infos = [
        TrackDriverInfo("VER", laps()),
        TrackDriverInfo("HAM", laps()),
        TrackDriverInfo("NOR", []),
    ]
    result = run(
        data(
            driver("VER", positions=pos([EPOCH])),
            driver("HAM", car=car([EPOCH])),
            driver("NOR", positions=pos([EPOCH])),
        ),
        infos + [TrackDriverInfo("LEC", laps())],
    )
    assert "No car telemetry for VER" in result.warnings
    assert "No position data for HAM" in result.warnings
    assert "No stored laps for NOR; samples skipped" in result.warnings
    assert "No track data for LEC" in result.warnings
    by_driver = {s.driver_abbreviation: s for s in result.samples}
    assert set(by_driver) == {"VER", "HAM"}
    assert by_driver["HAM"].x is None and by_driver["HAM"].speed_kph == 200
    assert by_driver["VER"].speed_kph is None and by_driver["VER"].x == 0


def test_out_of_range_values_are_kept_and_reported() -> None:
    result = run(data(driver(car=car([EPOCH], throttle_pct=[104]))))
    assert result.samples[0].throttle_pct == 104
    assert "1 telemetry values out of expected range for VER" in result.warnings


def test_outline_uses_the_fastest_clean_lap_and_is_bounded() -> None:
    times = list(range(EPOCH, EPOCH + 20_000, 5))  # 4 000 points over two laps
    fast = [
        TrackLapWindow(1, EPOCH + 10_000, 9_000, EPOCH, True),
        TrackLapWindow(2, EPOCH + 20_000, 8_000, EPOCH + 10_000, False),  # not clean
    ]
    result = run(data(driver(positions=pos(times))), [TrackDriverInfo("VER", fast)])
    assert 0 < len(result.outline) <= MAX_OUTLINE_POINTS
    assert result.outline[0] == (0, 0)  # lap 1 (the clean one) starts at the first point
    assert max(x for x, _ in result.outline) <= 2_000


def test_no_outline_without_a_clean_lap() -> None:
    result = run(data(driver(positions=pos([EPOCH, EPOCH + 500]))))
    assert result.outline == []
    assert "No clean lap with position data; circuit outline not available" in result.warnings


def test_corners_bounds_and_team_colours() -> None:
    circuit = ExtractedCircuit(
        rotation_deg=92.0,
        corners=[
            ExtractedCorner(2, "", 10.6, 20.2, 45.0),
            ExtractedCorner(1, "a", -5.0, 3.0, None),
        ],
    )
    result = run(data(driver(positions=pos([EPOCH, EPOCH + 500], x0=-7)), circuit=circuit))
    assert result.rotation_deg == 92.0
    assert result.corners == [
        {"number": 1, "letter": "a", "x": -5, "y": 3, "angle_deg": None},
        {"number": 2, "letter": "", "x": 11, "y": 20, "angle_deg": 45.0},
    ]
    assert result.bounds == (-7, -6, -1, 0)
    assert result.team_colors == {"VER": "3671c6"}


def test_missing_circuit_info_is_a_warning() -> None:
    result = run(data(driver(positions=pos([EPOCH]))))
    assert result.corners is None
    assert "Circuit info unavailable; corners not imported" in result.warnings


def test_normalization_is_deterministic() -> None:
    extracted = data(
        driver(
            positions=pos(list(range(EPOCH, EPOCH + 20_000, 270))),
            car=car(list(range(EPOCH, EPOCH + 20_000, 230))),
        )
    )
    assert run(extracted) == run(extracted)
