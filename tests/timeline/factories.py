"""Synthetic timeline sources for tests."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

from app.domain.enums import SessionType, TrackStatus
from app.timeline.source import (
    SourceDriver,
    SourceLap,
    SourceTrackStatus,
    TimelineSource,
)

EPOCH = 3_600_000  # session time of the race start used by fixtures


def drv(abbr: str, grid: int | None = None) -> SourceDriver:
    return SourceDriver(id=uuid4(), abbreviation=abbr, grid_position=grid)


def lap(
    driver: SourceDriver | UUID,
    number: int,
    *,
    start: int | None = None,
    end: int | None = None,
    lap_time: int | None = 90_000,
    position: int | None = None,
    pit_in: int | None = None,
    pit_out: int | None = None,
    deleted: bool | None = False,
    compound: str | None = "SOFT",
    stint: int | None = 1,
    tyre_age: int | None = None,
) -> SourceLap:
    driver_id = driver.id if isinstance(driver, SourceDriver) else driver
    return SourceLap(
        driver_id=driver_id,
        lap_number=number,
        lap_time_ms=lap_time,
        position=position,
        compound=compound,
        tyre_age_laps=tyre_age if tyre_age is not None else number,
        stint_number=stint,
        is_deleted=deleted,
        is_accurate=True,
        lap_start_time_ms=start,
        lap_end_time_ms=end,
        pit_in_time_ms=pit_in,
        pit_out_time_ms=pit_out,
        is_pit_in_lap=pit_in is not None,
        is_pit_out_lap=pit_out is not None,
    )


def status(
    sequence: int, session_time_ms: int, value: TrackStatus, code: str = "1"
) -> SourceTrackStatus:
    return SourceTrackStatus(
        sequence=sequence,
        session_time_ms=session_time_ms,
        status=value,
        source_code=code,
    )


def source(
    drivers: list[SourceDriver],
    laps: list[SourceLap],
    track_statuses: list[SourceTrackStatus] | None = None,
    session_type: SessionType = SessionType.RACE,
) -> TimelineSource:
    return TimelineSource(
        session_id=uuid4(),
        race_id=uuid4(),
        session_type=session_type,
        season=2024,
        round=1,
        drivers=drivers,
        laps=laps,
        track_statuses=track_statuses or [],
    )


def one_driver_source(n_laps: int = 3, **lap_kwargs: object) -> tuple[TimelineSource, SourceDriver]:
    d = drv("VER", 1)
    laps = [
        lap(
            d,
            n,
            start=EPOCH + (n - 1) * 90_000,
            end=EPOCH + n * 90_000,
            **lap_kwargs,  # type: ignore[arg-type]
        )
        for n in range(1, n_laps + 1)
    ]
    return source([d], laps), d


def representative_race() -> TimelineSource:
    """3 drivers x 5 laps: a pit stop, a safety car period and an overtake.

    Grid: VER 1, HAM 2, NOR 3. NOR gains P2 on lap 1 and passes VER on lap 4.
    HAM pits at the end of lap 2 (pre-race garage exit on lap 1 is excluded).
    Safety car from +100s to +200s after race start.
    """
    ver, ham, nor = drv("VER", 1), drv("HAM", 2), drv("NOR", 3)
    positions = {
        "VER": [1, 1, 1, 2, 2],
        "HAM": [2, 3, 3, 3, 3],
        "NOR": [2, 2, 2, 1, 1],
    }
    offsets = {"VER": 0, "HAM": 500, "NOR": 1_000}
    laps: list[SourceLap] = []
    for d in (ver, ham, nor):
        for n in range(1, 6):
            start = EPOCH + (n - 1) * 90_000 + offsets[d.abbreviation]
            lap_time = 90_000
            if d.abbreviation == "NOR" and n == 4:
                lap_time = 88_500  # fastest of the race
            pit_in = pit_out = None
            stint = 1
            if d.abbreviation == "HAM":
                if n == 1:
                    pit_out = EPOCH - 5_000  # pre-race pit lane exit
                if n == 2:
                    pit_in = start + 80_000
                if n == 3:
                    pit_out = start + 22_000
                stint = 1 if n <= 2 else 2
            laps.append(
                lap(
                    d,
                    n,
                    start=start,
                    end=None if (d.abbreviation == "VER" and n == 5) else start + lap_time,
                    lap_time=lap_time,
                    position=positions[d.abbreviation][n - 1],
                    pit_in=pit_in,
                    pit_out=pit_out,
                    stint=stint,
                )
            )
    # VER lap 5 has no end time -> fallback start + lap_time
    track = [
        status(0, EPOCH - 600_000, TrackStatus.GREEN, "1"),
        status(1, EPOCH + 100_000, TrackStatus.SAFETY_CAR, "4"),
        status(2, EPOCH + 200_000, TrackStatus.GREEN, "1"),
    ]
    return source([ver, ham, nor], laps, track)


def with_laps(src: TimelineSource, laps: list[SourceLap]) -> TimelineSource:
    return replace(src, laps=laps)
