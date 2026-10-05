"""Normalizer unit tests with synthetic Extracted* objects."""

from __future__ import annotations

from datetime import date

import pytest

from app.domain.enums import SessionType, TrackStatus
from app.ingestion.errors import NormalizationError
from app.ingestion.normalizer import map_track_status_code, normalize
from app.ingestion.records import (
    ExtractedDriver,
    ExtractedLap,
    ExtractedSession,
    ExtractedTrackStatus,
)


def _base_session(**kwargs: object) -> ExtractedSession:
    defaults: dict[str, object] = {
        "season": 2024,
        "round": 1,
        "event_name": "Bahrain Grand Prix",
        "official_event_name": "Formula 1 Gulf Air Bahrain Grand Prix 2024",
        "country": "Bahrain",
        "location": "Sakhir",
        "event_date": date(2024, 3, 2),
        "session_type": SessionType.RACE,
        "session_name": "Race",
        "session_start": None,
        "drivers": [],
        "laps": [],
        "track_statuses": [],
    }
    defaults.update(kwargs)
    return ExtractedSession(**defaults)  # type: ignore[arg-type]


def _driver(
    abbr: str = "NOR",
    *,
    finish_position: int | None = 3,
    grid_position: int | None = 5,
) -> ExtractedDriver:
    return ExtractedDriver(
        driver_number=4,
        abbreviation=abbr,
        first_name="Lando",
        last_name="Norris",
        full_name="Lando Norris",
        team_name="McLaren",
        grid_position=grid_position,
        finish_position=finish_position,
        result_status="Finished",
    )


def _lap(
    abbr: str = "NOR",
    lap_number: int = 1,
    *,
    position: int | None = 4,
    sector1: int | None = 20_000,
    sector2: int | None = 25_000,
    sector3: int | None = 22_000,
    stint: int | None = 1,
    compound: str | None = "SOFT",
    tyre_age: int | None = 1,
    pit_in: int | None = None,
    pit_out: int | None = None,
) -> ExtractedLap:
    return ExtractedLap(
        driver_abbreviation=abbr,
        driver_number=4,
        lap_number=lap_number,
        lap_time_ms=67_000,
        position=position,
        compound=compound,
        tyre_age_laps=tyre_age,
        stint_number=stint,
        pit_in_time_ms=pit_in,
        pit_out_time_ms=pit_out,
        sector1_time_ms=sector1,
        sector2_time_ms=sector2,
        sector3_time_ms=sector3,
        lap_start_time_ms=0,
        lap_end_time_ms=67_000,
        is_deleted=False,
        is_accurate=True,
        team_name="McLaren",
    )


def test_normalize_drivers_laps_sectors_stints_and_pits() -> None:
    extracted = _base_session(
        drivers=[_driver()],
        laps=[
            _lap(lap_number=1, pit_in=100_000),
            _lap(lap_number=2, pit_out=102_500, stint=2, compound="MEDIUM", tyre_age=0),
            _lap(lap_number=10, pit_in=500_000, pit_out=502_000, stint=2, compound="MEDIUM"),
        ],
    )
    result = normalize(extracted)

    assert len(result.drivers) == 1
    assert result.drivers[0].abbreviation == "NOR"
    assert result.drivers[0].finish_position == 3
    assert result.drivers[0].grid_position == 5

    assert len(result.laps) == 3
    lap1, lap2, lap10 = result.laps
    assert lap1.is_pit_in_lap is True
    assert lap1.is_pit_out_lap is False
    assert lap1.pit_duration_ms is None  # pit-out on next lap

    assert lap2.is_pit_out_lap is True
    assert lap2.pit_duration_ms is None

    assert lap10.pit_duration_ms == 2_000
    assert lap10.is_pit_in_lap is True
    assert lap10.is_pit_out_lap is True

    assert len(lap1.sectors) == 3
    assert {s.sector_number for s in lap1.sectors} == {1, 2, 3}

    assert len(result.stints) == 2
    stint1 = next(s for s in result.stints if s.stint_number == 1)
    stint2 = next(s for s in result.stints if s.stint_number == 2)
    assert stint1.start_lap == 1 and stint1.end_lap == 1
    assert stint1.compound == "SOFT"
    assert stint2.start_lap == 2 and stint2.end_lap == 10
    assert stint2.compound == "MEDIUM"
    assert stint2.tyre_age_at_start == 0


def test_missing_sector_time_omitted() -> None:
    extracted = _base_session(
        drivers=[_driver()],
        laps=[_lap(sector2=None)],
    )
    result = normalize(extracted)
    assert [s.sector_number for s in result.laps[0].sectors] == [1, 3]


def test_bad_abbreviation_skipped_with_laps() -> None:
    extracted = _base_session(
        drivers=[_driver(abbr="NO"), _driver(abbr="PIA")],
        laps=[_lap(abbr="NO"), _lap(abbr="PIA", lap_number=1)],
    )
    # Second driver needs distinct number/name — patch manually
    drivers = list(extracted.drivers)
    drivers[1] = ExtractedDriver(
        driver_number=81,
        abbreviation="PIA",
        first_name="Oscar",
        last_name="Piastri",
        full_name="Oscar Piastri",
        team_name="McLaren",
        grid_position=2,
        finish_position=2,
        result_status="Finished",
    )
    extracted = _base_session(
        drivers=drivers,
        laps=[
            _lap(abbr="NO"),
            ExtractedLap(
                driver_abbreviation="PIA",
                driver_number=81,
                lap_number=1,
                lap_time_ms=68_000,
                position=2,
                compound="SOFT",
                tyre_age_laps=1,
                stint_number=1,
                pit_in_time_ms=None,
                pit_out_time_ms=None,
                sector1_time_ms=20_000,
                sector2_time_ms=25_000,
                sector3_time_ms=22_000,
                lap_start_time_ms=0,
                lap_end_time_ms=67_000,
                is_deleted=False,
                is_accurate=True,
                team_name="McLaren",
            ),
        ],
    )
    result = normalize(extracted)
    assert len(result.drivers) == 1
    assert result.drivers[0].abbreviation == "PIA"
    assert len(result.laps) == 1
    assert result.skipped_count >= 2
    assert any("abbreviation" in w.lower() for w in result.warnings)


def test_track_status_code_mapping() -> None:
    assert map_track_status_code("1") == TrackStatus.GREEN
    assert map_track_status_code("2") == TrackStatus.YELLOW
    assert map_track_status_code("4") == TrackStatus.SAFETY_CAR
    assert map_track_status_code("5") == TrackStatus.RED_FLAG
    assert map_track_status_code("6") == TrackStatus.VIRTUAL_SAFETY_CAR
    assert map_track_status_code("7") == TrackStatus.VIRTUAL_SAFETY_CAR_ENDING
    assert map_track_status_code("26") == TrackStatus.VIRTUAL_SAFETY_CAR  # 6 before 2
    assert map_track_status_code("26") != TrackStatus.YELLOW
    assert map_track_status_code("9") == TrackStatus.UNKNOWN

    extracted = _base_session(
        drivers=[_driver()],
        track_statuses=[
            ExtractedTrackStatus(race_time_ms=1000, source_code="26", message="VSC"),
            ExtractedTrackStatus(race_time_ms=500, source_code="1", message="Green"),
            ExtractedTrackStatus(race_time_ms=None, source_code="1", message="bad"),
            ExtractedTrackStatus(race_time_ms=2000, source_code="9", message="??"),
        ],
    )
    result = normalize(extracted)
    assert len(result.track_statuses) == 3
    assert result.track_statuses[0].sequence == 0
    assert result.track_statuses[0].status == TrackStatus.GREEN
    assert result.track_statuses[1].status == TrackStatus.VIRTUAL_SAFETY_CAR
    assert result.track_statuses[1].source_code == "26"
    assert result.track_statuses[2].status == TrackStatus.UNKNOWN
    assert any("missing time" in w.lower() for w in result.warnings)


def test_lap_position_not_copied_to_finish_position() -> None:
    extracted = _base_session(
        drivers=[_driver(finish_position=7, grid_position=12)],
        laps=[_lap(position=1)],
    )
    result = normalize(extracted)
    assert result.drivers[0].finish_position == 7
    assert result.drivers[0].grid_position == 12
    assert result.laps[0].position == 1


def _assert_no_nan(value: object) -> None:
    if isinstance(value, float):
        assert value == value  # NaN != NaN
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_no_nan(item)
    if hasattr(value, "__dataclass_fields__"):
        for field_name in value.__dataclass_fields__:
            _assert_no_nan(getattr(value, field_name))


def test_no_nan_in_normalized_fields() -> None:
    extracted = _base_session(
        drivers=[_driver()],
        laps=[_lap()],
        track_statuses=[
            ExtractedTrackStatus(race_time_ms=0, source_code="1", message=None),
        ],
    )
    result = normalize(extracted)
    _assert_no_nan(result)


def test_duplicate_abbreviation_raises_normalization_error() -> None:
    extracted = _base_session(
        drivers=[
            _driver(abbr="NOR"),
            ExtractedDriver(
                driver_number=44,
                abbreviation="NOR",
                first_name="Other",
                last_name="Norris",
                full_name="Other Norris",
                team_name="Other",
                grid_position=6,
                finish_position=6,
                result_status="Finished",
            ),
        ],
    )
    with pytest.raises(NormalizationError, match="NOR"):
        normalize(extracted)


def test_lap_end_time_is_carried_through_normalization() -> None:
    result = normalize(_base_session(drivers=[_driver()], laps=[_lap()]))
    assert result.laps[0].lap_end_time_ms == 67_000
