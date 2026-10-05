"""ORM model persistence and constraint tests (SQLite)."""

from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import EventType, ReplayStatus, SessionType
from app.models import (
    Driver,
    Lap,
    Race,
    RaceEvent,
    RaceSession,
    ReplaySession,
    Sector,
    TyreStint,
)


async def _seed_session(db: AsyncSession) -> tuple[Race, RaceSession, Driver]:
    race = Race(season=2024, round=8, name="Monaco Grand Prix", circuit_name="Monaco")
    db.add(race)
    await db.flush()

    session = RaceSession(
        race_id=race.id,
        session_type=SessionType.RACE,
        name="Race",
    )
    db.add(session)
    await db.flush()

    driver = Driver(
        session_id=session.id,
        driver_number=4,
        abbreviation="nor",
        full_name="Lando Norris",
    )
    db.add(driver)
    await db.flush()
    return race, session, driver


async def test_persist_race_session_driver_lap_sector(db_session: AsyncSession) -> None:
    _, session, driver = await _seed_session(db_session)

    lap = Lap(
        session_id=session.id,
        driver_id=driver.id,
        lap_number=1,
        lap_time_ms=78_123,
        position=3,
        compound="SOFT",
        tyre_age_laps=1,
    )
    db_session.add(lap)
    await db_session.flush()

    sector = Sector(lap_id=lap.id, sector_number=1, sector_time_ms=26_100)
    db_session.add(sector)
    await db_session.flush()

    assert driver.abbreviation == "NOR"
    assert lap.id is not None
    assert sector.sector_number == 1

    loaded = await db_session.scalar(select(Lap).where(Lap.id == lap.id))
    assert loaded is not None
    assert loaded.lap_time_ms == 78_123


async def test_persist_tyre_stint_replay_and_event(db_session: AsyncSession) -> None:
    _, session, driver = await _seed_session(db_session)

    stint = TyreStint(
        session_id=session.id,
        driver_id=driver.id,
        stint_number=1,
        compound="MEDIUM",
        start_lap=1,
        end_lap=20,
        tyre_age_at_start=0,
    )
    replay = ReplaySession(session_id=session.id)
    event = RaceEvent(
        session_id=session.id,
        event_type=EventType.LAP_COMPLETED,
        driver_id=driver.id,
        lap_number=1,
        race_time_ms=90_000,
        sequence=0,
        payload={"source": "test"},
    )
    db_session.add_all([stint, replay, event])
    await db_session.flush()

    assert replay.status == ReplayStatus.PENDING
    assert replay.playback_speed == Decimal("1.00")
    assert event.payload == {"source": "test"}

    # Event without driver_id is allowed (MATCH SIMPLE skips composite FK).
    track = RaceEvent(
        session_id=session.id,
        event_type=EventType.TRACK_STATUS_CHANGED,
        driver_id=None,
        sequence=1,
        payload={},
    )
    db_session.add(track)
    await db_session.flush()
    assert track.driver_id is None


async def test_duplicate_season_round_rejected(db_session: AsyncSession) -> None:
    db_session.add(Race(season=2024, round=1, name="Bahrain"))
    await db_session.flush()
    db_session.add(Race(season=2024, round=1, name="Duplicate"))
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_duplicate_race_session_type_rejected(db_session: AsyncSession) -> None:
    race = Race(season=2023, round=1, name="Bahrain")
    db_session.add(race)
    await db_session.flush()
    db_session.add(
        RaceSession(race_id=race.id, session_type=SessionType.RACE, name="Race"),
    )
    await db_session.flush()
    db_session.add(
        RaceSession(race_id=race.id, session_type=SessionType.RACE, name="Race again"),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_duplicate_session_abbreviation_rejected(db_session: AsyncSession) -> None:
    _, session, _ = await _seed_session(db_session)
    db_session.add(
        Driver(session_id=session.id, abbreviation="NOR", full_name="Other"),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_duplicate_driver_lap_number_rejected(db_session: AsyncSession) -> None:
    _, session, driver = await _seed_session(db_session)
    db_session.add(
        Lap(session_id=session.id, driver_id=driver.id, lap_number=5),
    )
    await db_session.flush()
    db_session.add(
        Lap(session_id=session.id, driver_id=driver.id, lap_number=5),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_duplicate_lap_sector_number_rejected(db_session: AsyncSession) -> None:
    _, session, driver = await _seed_session(db_session)
    lap = Lap(session_id=session.id, driver_id=driver.id, lap_number=2)
    db_session.add(lap)
    await db_session.flush()
    db_session.add(Sector(lap_id=lap.id, sector_number=2, sector_time_ms=1000))
    await db_session.flush()
    db_session.add(Sector(lap_id=lap.id, sector_number=2, sector_time_ms=2000))
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_sector_number_outside_range_rejected(db_session: AsyncSession) -> None:
    _, session, driver = await _seed_session(db_session)
    lap = Lap(session_id=session.id, driver_id=driver.id, lap_number=3)
    db_session.add(lap)
    await db_session.flush()
    db_session.add(Sector(lap_id=lap.id, sector_number=4, sector_time_ms=1000))
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_lap_cannot_reference_driver_from_different_session(
    db_session: AsyncSession,
) -> None:
    race = Race(season=2022, round=1, name="Bahrain")
    db_session.add(race)
    await db_session.flush()

    session_a = RaceSession(
        race_id=race.id,
        session_type=SessionType.PRACTICE_1,
        name="FP1",
    )
    session_b = RaceSession(
        race_id=race.id,
        session_type=SessionType.PRACTICE_2,
        name="FP2",
    )
    db_session.add_all([session_a, session_b])
    await db_session.flush()

    driver_a = Driver(
        session_id=session_a.id,
        abbreviation="VER",
        full_name="Max Verstappen",
    )
    db_session.add(driver_a)
    await db_session.flush()

    # Lap claims session_b but driver belongs to session_a.
    db_session.add(
        Lap(session_id=session_b.id, driver_id=driver_a.id, lap_number=1),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_duplicate_session_sequence_rejected(db_session: AsyncSession) -> None:
    _, session, _ = await _seed_session(db_session)
    db_session.add(
        RaceEvent(
            session_id=session.id,
            event_type=EventType.PIT_ENTRY,
            sequence=10,
            payload={},
        ),
    )
    await db_session.flush()
    db_session.add(
        RaceEvent(
            session_id=session.id,
            event_type=EventType.PIT_EXIT,
            sequence=10,
            payload={},
        ),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_invalid_event_type_rejected(db_session: AsyncSession) -> None:
    _, session, _ = await _seed_session(db_session)
    event = RaceEvent(
        session_id=session.id,
        event_type="NOT_A_REAL_EVENT",  # type: ignore[arg-type]
        sequence=0,
        payload={},
    )
    db_session.add(event)
    with pytest.raises((LookupError, StatementError, IntegrityError, ValueError)):
        await db_session.flush()


async def test_replay_session_requires_existing_session(db_session: AsyncSession) -> None:
    db_session.add(ReplaySession(session_id=uuid4()))
    with pytest.raises(IntegrityError):
        await db_session.flush()


def test_event_type_enum_contains_exact_required_members() -> None:
    expected = {
        "RACE_STARTED",
        "LAP_COMPLETED",
        "SECTOR_COMPLETED",
        "POSITION_CHANGED",
        "PIT_ENTRY",
        "PIT_EXIT",
        "FASTEST_LAP",
        "TRACK_STATUS_CHANGED",
        "BATTLE_FORMING",
        "PACE_DEGRADATION",
        "PACE_ANOMALY",
    }
    assert {member.value for member in EventType} == expected
    assert len(EventType) == len(expected)


async def test_duplicate_session_driver_number_rejected(db_session: AsyncSession) -> None:
    _, session, _ = await _seed_session(db_session)
    db_session.add(
        Driver(
            session_id=session.id,
            driver_number=4,
            abbreviation="PIA",
            full_name="Oscar Piastri",
        ),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_null_driver_numbers_allowed_in_same_session(db_session: AsyncSession) -> None:
    race = Race(season=2021, round=1, name="Bahrain")
    db_session.add(race)
    await db_session.flush()
    session = RaceSession(
        race_id=race.id,
        session_type=SessionType.RACE,
        name="Race",
    )
    db_session.add(session)
    await db_session.flush()

    db_session.add_all(
        [
            Driver(
                session_id=session.id,
                driver_number=None,
                abbreviation="AAA",
                full_name="Driver A",
            ),
            Driver(
                session_id=session.id,
                driver_number=None,
                abbreviation="BBB",
                full_name="Driver B",
            ),
        ],
    )
    await db_session.flush()


async def test_tyre_stint_end_lap_before_start_lap_rejected(db_session: AsyncSession) -> None:
    _, session, driver = await _seed_session(db_session)
    db_session.add(
        TyreStint(
            session_id=session.id,
            driver_id=driver.id,
            stint_number=1,
            compound="HARD",
            start_lap=10,
            end_lap=5,
        ),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_replay_playback_speed_not_positive_rejected(db_session: AsyncSession) -> None:
    _, session, _ = await _seed_session(db_session)
    db_session.add(
        ReplaySession(session_id=session.id, playback_speed=Decimal("0")),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_race_event_driver_from_different_session_rejected(
    db_session: AsyncSession,
) -> None:
    race = Race(season=2020, round=1, name="Austria")
    db_session.add(race)
    await db_session.flush()

    session_a = RaceSession(
        race_id=race.id,
        session_type=SessionType.PRACTICE_1,
        name="FP1",
    )
    session_b = RaceSession(
        race_id=race.id,
        session_type=SessionType.PRACTICE_2,
        name="FP2",
    )
    db_session.add_all([session_a, session_b])
    await db_session.flush()

    driver_a = Driver(
        session_id=session_a.id,
        abbreviation="HAM",
        full_name="Lewis Hamilton",
    )
    db_session.add(driver_a)
    await db_session.flush()

    db_session.add(
        RaceEvent(
            session_id=session_b.id,
            event_type=EventType.FASTEST_LAP,
            driver_id=driver_a.id,
            sequence=0,
            payload={},
        ),
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()
