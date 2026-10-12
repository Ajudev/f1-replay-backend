"""Align extracted car positions and telemetry to the race clock.

No pandas, FastF1, or ORM imports.

Every sample is moved from session time to race time (``session_time - epoch``, the
same epoch as the stored timeline, so markers line up with the replay clock) and put on
a fixed grid of ``sample_interval_ms``. Each grid point takes the nearest raw sample
within half an interval (so one raw sample is never stretched over several points), separately for positions and telemetry (their timestamps never
line up); a grid point without one stays empty. Nothing is interpolated or clamped here.
"""

from __future__ import annotations

from bisect import bisect_left

from app.ingestion.records import (
    CarSeries,
    ExtractedDriverTrack,
    ExtractedTrackData,
    NormalizedTrackData,
    NormalizedTrackSample,
    PositionSeries,
    TrackDriverInfo,
    TrackLapWindow,
)

_MAX_WARNINGS = 50
#: Samples are kept from this long before the race start (grid, formation end) ...
PRE_START_MS = 60_000
#: ... until this long after the driver's last lap (in-lap after the flag).
POST_FINISH_MS = 60_000
MAX_OUTLINE_POINTS = 800
#: An outline lap needs at least this many raw points to be drawn.
MIN_OUTLINE_POINTS = 50

#: Plausible value ranges; values outside are kept as-is and reported.
_TELEMETRY_RANGES = {
    "speed_kph": (0, 400),
    "rpm": (0, 20_000),
    "gear": (0, 8),
    "throttle_pct": (0, 100),
}


def normalize_track_data(
    extracted: ExtractedTrackData,
    *,
    drivers: list[TrackDriverInfo],
    epoch_ms: int,
    sample_interval_ms: int,
) -> NormalizedTrackData:
    """Resample every stored driver's positions and telemetry onto the race-time grid."""
    warnings: list[str] = []
    by_abbr = {
        (d.abbreviation or "").strip().upper(): d for d in extracted.drivers if d.abbreviation
    }
    samples: list[NormalizedTrackSample] = []
    team_colors: dict[str, str] = {}
    position_count = telemetry_count = 0
    xs: list[int] = []
    ys: list[int] = []

    for driver in sorted(drivers, key=lambda d: d.abbreviation):
        abbr = driver.abbreviation
        source = by_abbr.get(abbr)
        if source is None:
            warnings.append(f"No track data for {abbr}")
            continue
        if source.team_color:
            team_colors[abbr] = source.team_color
        if source.positions is None:
            warnings.append(f"No position data for {abbr}")
        if source.car is None:
            warnings.append(f"No car telemetry for {abbr}")
        if not driver.laps:
            warnings.append(f"No stored laps for {abbr}; samples skipped")
            continue

        laps = sorted(driver.laps, key=lambda lap: lap.end_ms)
        ends = [lap.end_ms for lap in laps]
        start = -PRE_START_MS
        stop = ends[-1] + POST_FINISH_MS - epoch_ms
        positions = (
            _resample(source.positions.session_time_ms, start, stop, sample_interval_ms, epoch_ms)
            if source.positions
            else {}
        )
        car = (
            _resample(source.car.session_time_ms, start, stop, sample_interval_ms, epoch_ms)
            if source.car
            else {}
        )
        out_of_range = 0
        for race_ms in sorted(positions.keys() | car.keys()):
            sample = _sample(
                abbr,
                race_ms,
                _lap_at(race_ms + epoch_ms, race_ms, ends, laps),
                source.positions,
                positions.get(race_ms),
                source.car,
                car.get(race_ms),
            )
            samples.append(sample)
            if sample.x is not None and sample.y is not None:
                position_count += 1
                xs.append(sample.x)
                ys.append(sample.y)
            if sample.speed_kph is not None or sample.gear is not None:
                telemetry_count += 1
            out_of_range += _out_of_range(sample)
        if out_of_range:
            warnings.append(f"{out_of_range} telemetry values out of expected range for {abbr}")

    outline = _outline(extracted, drivers, by_abbr, warnings)
    xs.extend(x for x, _ in outline)
    ys.extend(y for _, y in outline)
    circuit = extracted.circuit
    if circuit is None:
        warnings.append("Circuit info unavailable; corners not imported")
    corners = (
        [
            {
                "number": c.number,
                "letter": c.letter,
                "x": round(c.x),
                "y": round(c.y),
                "angle_deg": c.angle_deg,
            }
            for c in sorted(circuit.corners, key=lambda c: (c.number, c.letter))
        ]
        if circuit is not None
        else None
    )
    return NormalizedTrackData(
        outline=outline,
        corners=corners,
        rotation_deg=circuit.rotation_deg if circuit is not None else None,
        bounds=(min(xs), max(xs), min(ys), max(ys)) if xs else (0, 0, 0, 0),
        sample_interval_ms=sample_interval_ms,
        samples=samples,
        team_colors=team_colors,
        position_sample_count=position_count,
        telemetry_sample_count=telemetry_count,
        warnings=warnings[:_MAX_WARNINGS],
    )


def _resample(times: list[int], start: int, stop: int, step: int, epoch_ms: int) -> dict[int, int]:
    """Grid race time → index of the nearest raw sample within ``step / 2`` (earlier on ties).

    ``times`` are session times in ascending order; grid points are multiples of ``step``.
    """
    result: dict[int, int] = {}
    if not times:
        return result
    count = len(times)
    i = 0
    grid = -(-start // step) * step  # first multiple of step >= start
    while grid <= stop:
        target = grid + epoch_ms
        while i + 1 < count and times[i + 1] <= target:
            i += 1
        best: tuple[int, int] | None = None
        for j in (i, i + 1):
            if j < count:
                distance = abs(times[j] - target)
                if 2 * distance <= step and (best is None or distance < best[0]):
                    best = (distance, j)
        if best is not None:
            result[grid] = best[1]
        grid += step
    return result


def _lap_at(
    session_ms: int, race_ms: int, ends: list[int], laps: list[TrackLapWindow]
) -> int | None:
    """The lap being driven at ``session_ms`` (``None`` before the start or after the last
    stored lap end)."""
    if race_ms < 0:
        return None
    index = bisect_left(ends, session_ms)
    return laps[index].lap_number if index < len(laps) else None


def _sample(
    abbr: str,
    race_ms: int,
    lap_number: int | None,
    positions: PositionSeries | None,
    p: int | None,
    car: CarSeries | None,
    c: int | None,
) -> NormalizedTrackSample:
    has_p = positions is not None and p is not None
    has_c = car is not None and c is not None
    return NormalizedTrackSample(
        driver_abbreviation=abbr,
        race_time_ms=race_ms,
        lap_number=lap_number,
        x=positions.x[p] if has_p else None,  # type: ignore[union-attr, index]
        y=positions.y[p] if has_p else None,  # type: ignore[union-attr, index]
        off_track=positions.off_track[p] if has_p else None,  # type: ignore[union-attr, index]
        speed_kph=car.speed_kph[c] if has_c else None,  # type: ignore[union-attr, index]
        rpm=car.rpm[c] if has_c else None,  # type: ignore[union-attr, index]
        gear=car.gear[c] if has_c else None,  # type: ignore[union-attr, index]
        throttle_pct=car.throttle_pct[c] if has_c else None,  # type: ignore[union-attr, index]
        brake=car.brake[c] if has_c else None,  # type: ignore[union-attr, index]
        drs_code=car.drs_code[c] if has_c else None,  # type: ignore[union-attr, index]
    )


def _out_of_range(sample: NormalizedTrackSample) -> int:
    count = 0
    for name, (low, high) in _TELEMETRY_RANGES.items():
        value = getattr(sample, name)
        if value is not None and not low <= value <= high:
            count += 1
    return count


def _outline(
    extracted: ExtractedTrackData,
    drivers: list[TrackDriverInfo],
    by_abbr: dict[str, ExtractedDriverTrack],
    warnings: list[str],
) -> list[tuple[int, int]]:
    """Track outline from the raw positions of the fastest clean lap with enough points."""
    candidates = sorted(
        (lap.lap_time_ms, driver.abbreviation, lap.lap_number, lap)
        for driver in drivers
        for lap in driver.laps
        if lap.usable_for_outline and lap.lap_time_ms is not None and lap.start_ms is not None
    )
    for _, abbr, _, lap in candidates:
        source = by_abbr.get(abbr)
        positions = source.positions if source is not None else None
        if positions is None:
            continue
        lo = bisect_left(positions.session_time_ms, lap.start_ms)
        hi = bisect_left(positions.session_time_ms, lap.end_ms + 1)
        points = list(zip(positions.x[lo:hi], positions.y[lo:hi], strict=True))
        if len(points) < MIN_OUTLINE_POINTS:
            continue
        stride = -(-len(points) // MAX_OUTLINE_POINTS)
        return points[::stride]
    warnings.append("No clean lap with position data; circuit outline not available")
    return []
