"""Import service tests with fake SessionLoader and SQLite."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import SessionType, TrackStatus
from app.ingestion.errors import NormalizationError, PersistenceError
from app.ingestion.records import (
    ExtractedDriver,
    ExtractedLap,
    ExtractedSession,
    ExtractedTrackStatus,
)
from app.ingestion.service import ImportService
from app.models import Driver, Lap, Race, RaceSession, Sector, TrackStatusPeriod, TyreStint


class FakeLoader:
    def __init__(self, extracted: ExtractedSession) -> None:
        self.extracted = extracted
        self.calls = 0

    def load(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> ExtractedSession:
        self.calls += 1
        return self.extracted


def _sample_extracted(
    *,
    drivers: list[ExtractedDriver] | None = None,
    laps: list[ExtractedLap] | None = None,
) -> ExtractedSession:
    if drivers is None:
        drivers = [
            ExtractedDriver(
                driver_number=4,
                abbreviation="NOR",
                first_name="Lando",
                last_name="Norris",
                full_name="Lando Norris",
                team_name="McLaren",
                grid_position=5,
                finish_position=3,
                result_status="Finished",
            ),
            ExtractedDriver(
                driver_number=81,
                abbreviation="PIA",
                first_name="Oscar",
                last_name="Piastri",
                full_name="Oscar Piastri",
                team_name="McLaren",
                grid_position=2,
                finish_position=2,
                result_status="Finished",
            ),
        ]
    if laps is None:
        laps = [
            ExtractedLap(
                driver_abbreviation="NOR",
                driver_number=4,
                lap_number=1,
                lap_time_ms=90_000,
                position=4,
                compound="SOFT",
                tyre_age_laps=1,
                stint_number=1,
                pit_in_time_ms=None,
                pit_out_time_ms=None,
                sector1_time_ms=30_000,
                sector2_time_ms=30_000,
                sector3_time_ms=30_000,
                lap_start_time_ms=0,
                lap_end_time_ms=90_000,
                is_deleted=False,
                is_accurate=True,
                team_name="McLaren",
            ),
            ExtractedLap(
                driver_abbreviation="NOR",
                driver_number=4,
                lap_number=2,
                lap_time_ms=91_000,
                position=3,
                compound="SOFT",
                tyre_age_laps=2,
                stint_number=1,
                pit_in_time_ms=None,
                pit_out_time_ms=None,
                sector1_time_ms=30_100,
                sector2_time_ms=None,
                sector3_time_ms=30_200,
                lap_start_time_ms=90_000,
                lap_end_time_ms=180_000,
                is_deleted=False,
                is_accurate=True,
                team_name="McLaren",
            ),
        ]
    return ExtractedSession(
        season=2024,
        round=1,
        event_name="Bahrain Grand Prix",
        official_event_name="Official Bahrain GP",
        country="Bahrain",
        location="Sakhir",
        event_date=date(2024, 3, 2),
        session_type=SessionType.RACE,
        session_name="Race",
        session_start=None,
        drivers=drivers,
        laps=laps,
        track_statuses=[
            ExtractedTrackStatus(race_time_ms=0, source_code="1", message="AllClear"),
            ExtractedTrackStatus(race_time_ms=50_000, source_code="4", message="SCDeployed"),
        ],
    )


async def test_persist_full_graph_with_foreign_keys(db_session: AsyncSession) -> None:
    loader = FakeLoader(_sample_extracted())
    service = ImportService(loader=loader, session=db_session)
    result = await service.import_session(2024, 1, SessionType.RACE)

    assert result.status == "imported"
    assert loader.calls == 1
    assert result.driver_count == 2
    assert result.lap_count == 2
    assert result.sector_count == 5  # 3 + 2 (missing sector2 on lap 2)
    assert result.stint_count == 1
    assert result.track_status_count == 2

    race = await db_session.scalar(select(Race).where(Race.id == result.race_id))
    assert race is not None
    assert race.official_name == "Official Bahrain GP"
    assert race.circuit_name is None
    assert race.location == "Sakhir"

    drivers = (
        await db_session.scalars(select(Driver).where(Driver.session_id == result.session_id))
    ).all()
    assert {d.abbreviation for d in drivers} == {"NOR", "PIA"}
    nor = next(d for d in drivers if d.abbreviation == "NOR")
    assert nor.grid_position == 5
    assert nor.finish_position == 3

    laps = (await db_session.scalars(select(Lap).where(Lap.session_id == result.session_id))).all()
    assert all(lap.driver_id == nor.id for lap in laps)
    assert all(lap.session_id == result.session_id for lap in laps)

    sectors = (
        await db_session.scalars(
            select(Sector).join(Lap, Sector.lap_id == Lap.id).where(Lap.session_id == result.session_id)
        )
    ).all()
    assert len(sectors) == 5

    stints = (
        await db_session.scalars(select(TyreStint).where(TyreStint.session_id == result.session_id))
    ).all()
    assert len(stints) == 1
    assert stints[0].driver_id == nor.id

    periods = (
        await db_session.scalars(
            select(TrackStatusPeriod).where(TrackStatusPeriod.session_id == result.session_id)
        )
    ).all()
    assert len(periods) == 2
    assert periods[0].status == TrackStatus.GREEN
    assert periods[1].status == TrackStatus.SAFETY_CAR


async def test_second_import_without_replace_is_idempotent(db_session: AsyncSession) -> None:
    loader = FakeLoader(_sample_extracted())
    service = ImportService(loader=loader, session=db_session)
    first = await service.import_session(2024, 1, SessionType.RACE)
    second = await service.import_session(2024, 1, SessionType.RACE, replace=False)

    assert second.status == "already_imported"
    assert second.race_id == first.race_id
    assert second.session_id == first.session_id
    assert loader.calls == 1
    assert second.driver_count == first.driver_count == 2
    assert second.lap_count == first.lap_count == 2
    assert second.sector_count == first.sector_count == 5
    assert second.stint_count == first.stint_count == 1
    assert second.track_status_count == first.track_status_count == 2
    assert second.event_name == "Bahrain Grand Prix"
    assert second.warnings == []
    assert second.skipped_count == 0

    race_count = await db_session.scalar(select(func.count()).select_from(Race))
    session_count = await db_session.scalar(select(func.count()).select_from(RaceSession))
    driver_count = await db_session.scalar(select(func.count()).select_from(Driver))
    assert race_count == 1
    assert session_count == 1
    assert driver_count == 2


async def test_replace_rebuilds_children_without_duplicating_race(
    db_session: AsyncSession,
) -> None:
    loader = FakeLoader(_sample_extracted())
    service = ImportService(loader=loader, session=db_session)
    first = await service.import_session(2024, 1, SessionType.RACE)

    # Change extract slightly for replace
    replacement = _sample_extracted(
        drivers=[
            ExtractedDriver(
                driver_number=1,
                abbreviation="VER",
                first_name="Max",
                last_name="Verstappen",
                full_name="Max Verstappen",
                team_name="Red Bull",
                grid_position=1,
                finish_position=1,
                result_status="Finished",
            )
        ],
        laps=[
            ExtractedLap(
                driver_abbreviation="VER",
                driver_number=1,
                lap_number=1,
                lap_time_ms=88_000,
                position=1,
                compound="MEDIUM",
                tyre_age_laps=1,
                stint_number=1,
                pit_in_time_ms=None,
                pit_out_time_ms=None,
                sector1_time_ms=29_000,
                sector2_time_ms=29_000,
                sector3_time_ms=30_000,
                lap_start_time_ms=0,
                lap_end_time_ms=90_000,
                is_deleted=False,
                is_accurate=True,
                team_name="Red Bull",
            )
        ],
    )
    loader.extracted = replacement
    second = await service.import_session(2024, 1, SessionType.RACE, replace=True)

    assert second.status == "replaced"
    assert second.race_id == first.race_id
    assert second.session_id != first.session_id
    assert loader.calls == 2

    race_count = await db_session.scalar(select(func.count()).select_from(Race))
    session_count = await db_session.scalar(select(func.count()).select_from(RaceSession))
    driver_count = await db_session.scalar(select(func.count()).select_from(Driver))
    assert race_count == 1
    assert session_count == 1
    assert driver_count == 1
    abbr = await db_session.scalar(select(Driver.abbreviation))
    assert abbr == "VER"


async def test_integrity_failure_rolls_back_race(db_session: AsyncSession) -> None:
    # Duplicate (driver, lap_number) passes normalization but fails uniqueness on flush.
    lap = ExtractedLap(
        driver_abbreviation="NOR",
        driver_number=4,
        lap_number=1,
        lap_time_ms=90_000,
        position=4,
        compound="SOFT",
        tyre_age_laps=1,
        stint_number=1,
        pit_in_time_ms=None,
        pit_out_time_ms=None,
        sector1_time_ms=30_000,
        sector2_time_ms=30_000,
        sector3_time_ms=30_000,
        lap_start_time_ms=0,
        lap_end_time_ms=90_000,
        is_deleted=False,
        is_accurate=True,
        team_name="McLaren",
    )
    bad = _sample_extracted(
        drivers=[
            ExtractedDriver(
                driver_number=4,
                abbreviation="NOR",
                first_name="Lando",
                last_name="Norris",
                full_name="Lando Norris",
                team_name="McLaren",
                grid_position=5,
                finish_position=3,
                result_status="Finished",
            ),
        ],
        laps=[lap, lap],
    )
    loader = FakeLoader(bad)
    service = ImportService(loader=loader, session=db_session)

    with pytest.raises(PersistenceError):
        await service.import_session(2024, 1, SessionType.RACE)

    await db_session.rollback()
    race_count = await db_session.scalar(select(func.count()).select_from(Race))
    assert race_count == 0


async def test_duplicate_abbreviation_raises_before_persist(
    db_session: AsyncSession,
) -> None:
    bad = _sample_extracted(
        drivers=[
            ExtractedDriver(
                driver_number=4,
                abbreviation="NOR",
                first_name="Lando",
                last_name="Norris",
                full_name="Lando Norris",
                team_name="McLaren",
                grid_position=5,
                finish_position=3,
                result_status="Finished",
            ),
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
        laps=[],
    )
    loader = FakeLoader(bad)
    service = ImportService(loader=loader, session=db_session)

    with pytest.raises(NormalizationError, match="NOR"):
        await service.import_session(2024, 1, SessionType.RACE)

    race_count = await db_session.scalar(select(func.count()).select_from(Race))
    assert race_count == 0


async def test_name_based_already_imported_returns_stored_counts(
    db_session: AsyncSession,
) -> None:
    loader = FakeLoader(_sample_extracted())
    service = ImportService(loader=loader, session=db_session)
    first = await service.import_session(2024, 1, SessionType.RACE)
    second = await service.import_session(
        2024,
        "Bahrain Grand Prix",
        SessionType.RACE,
        replace=False,
    )

    assert second.status == "already_imported"
    assert loader.calls == 2  # name path still loads to resolve round
    assert second.driver_count == first.driver_count
    assert second.lap_count == first.lap_count
    assert second.sector_count == first.sector_count
    assert second.warnings == []
    assert second.skipped_count == 0


async def test_import_does_not_call_fastf1(db_session: AsyncSession) -> None:
    loader = FakeLoader(_sample_extracted())
    service = ImportService(loader=loader, session=db_session)
    await service.import_session(2024, 1, SessionType.RACE)
    assert loader.calls == 1
    # Fake loader only — if FastF1 were used, this test environment would need network/cache.
