"""PostgreSQL query helpers for imported races and sessions.

Must not import FastF1 or pandas.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.domain.enums import SessionType
from app.models import (
    Driver,
    Lap,
    Race,
    RaceSession,
    Sector,
    TrackStatusPeriod,
    TyreStint,
)
from app.schemas.races import (
    DriverSummary,
    LapOut,
    LapPage,
    RaceDetail,
    RaceSummary,
    SeasonSummary,
    SectorOut,
    SessionCounts,
    SessionDetail,
    SessionSummary,
    StintOut,
    TrackStatusOut,
)


class RaceNotFoundError(Exception):
    def __init__(self, race_id: UUID) -> None:
        self.message = f"Race not found: {race_id}"
        super().__init__(self.message)


class SessionNotFoundError(Exception):
    def __init__(self, session_id: UUID) -> None:
        self.message = f"Session not found: {session_id}"
        super().__init__(self.message)


class RaceSessionTypeNotFoundError(Exception):
    def __init__(self, race_id: UUID, session_type: SessionType) -> None:
        self.message = f"Race {race_id} has no imported {session_type.value} session"
        super().__init__(self.message)


class DriverNotFoundError(Exception):
    def __init__(self, session_id: UUID, driver: str) -> None:
        self.message = f"Driver {driver!r} is not part of session {session_id}"
        super().__init__(self.message)


def driver_condition(driver: str) -> ColumnElement[bool]:
    """Match a driver by UUID or abbreviation (case-insensitive)."""
    wanted = driver.strip()
    try:
        return Driver.id == UUID(wanted)
    except ValueError:
        return Driver.abbreviation == wanted.upper()


class RaceQueryService:
    """Read imported race data from PostgreSQL."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_seasons(self) -> list[SeasonSummary]:
        rows = await self._session.execute(
            select(Race.season, func.count(Race.id))
            .group_by(Race.season)
            .order_by(Race.season.desc())
        )
        return [SeasonSummary(season=season, race_count=count) for season, count in rows.all()]

    async def list_races(
        self,
        *,
        season: int | None = None,
        round_number: int | None = None,
        event: str | None = None,
        session_type: SessionType | None = None,
    ) -> list[RaceSummary]:
        """Imported races, newest season first; filters are combined with AND."""
        conditions: list[ColumnElement[bool]] = []
        if season is not None:
            conditions.append(Race.season == season)
        if round_number is not None:
            conditions.append(Race.round == round_number)
        if event is not None and event.strip():
            pattern = f"%{event.strip().lower()}%"
            conditions.append(
                or_(
                    func.lower(Race.name).like(pattern),
                    func.lower(Race.official_name).like(pattern),
                    func.lower(Race.country).like(pattern),
                    func.lower(Race.location).like(pattern),
                )
            )
        if session_type is not None:
            conditions.append(Race.sessions.any(RaceSession.session_type == session_type))
        result = await self._session.scalars(
            select(Race)
            .where(*conditions)
            .options(selectinload(Race.sessions))
            .order_by(Race.season.desc(), Race.round.asc())
        )
        return [RaceSummary(**_race_fields(race)) for race in result.all()]

    async def get_race(self, race_id: UUID) -> RaceDetail:
        race = await self._session.scalar(
            select(Race).where(Race.id == race_id).options(selectinload(Race.sessions))
        )
        if race is None:
            raise RaceNotFoundError(race_id)
        return RaceDetail(**_race_fields(race))

    async def race_session_id(self, race_id: UUID, session_type: SessionType) -> UUID:
        """Id of the race's session of ``session_type`` (404-style errors otherwise)."""
        race_exists = await self._session.scalar(select(Race.id).where(Race.id == race_id))
        if race_exists is None:
            raise RaceNotFoundError(race_id)
        session_id = await self._session.scalar(
            select(RaceSession.id).where(
                RaceSession.race_id == race_id, RaceSession.session_type == session_type
            )
        )
        if session_id is None:
            raise RaceSessionTypeNotFoundError(race_id, session_type)
        return session_id

    async def list_drivers(self, session_id: UUID) -> list[DriverSummary]:
        await self._require_session(session_id)
        rows = await self._session.scalars(
            select(Driver).where(Driver.session_id == session_id).order_by(Driver.abbreviation)
        )
        return [_driver_summary(driver) for driver in rows.all()]

    async def get_session(self, session_id: UUID) -> SessionDetail:
        race_session = await self._session.scalar(
            select(RaceSession)
            .where(RaceSession.id == session_id)
            .options(selectinload(RaceSession.drivers))
        )
        if race_session is None:
            raise SessionNotFoundError(session_id)

        lap_count = await self._session.scalar(
            select(func.count()).select_from(Lap).where(Lap.session_id == session_id)
        )
        sector_count = await self._session.scalar(
            select(func.count())
            .select_from(Sector)
            .join(Lap, Sector.lap_id == Lap.id)
            .where(Lap.session_id == session_id)
        )
        stint_count = await self._session.scalar(
            select(func.count()).select_from(TyreStint).where(TyreStint.session_id == session_id)
        )
        track_count = await self._session.scalar(
            select(func.count())
            .select_from(TrackStatusPeriod)
            .where(TrackStatusPeriod.session_id == session_id)
        )

        drivers = sorted(race_session.drivers, key=lambda d: d.abbreviation)
        return SessionDetail(
            id=race_session.id,
            race_id=race_session.race_id,
            session_type=race_session.session_type,
            name=race_session.name,
            start_time=race_session.start_time,
            end_time=race_session.end_time,
            drivers=[_driver_summary(driver) for driver in drivers],
            counts=SessionCounts(
                laps=int(lap_count or 0),
                sectors=int(sector_count or 0),
                stints=int(stint_count or 0),
                track_status_periods=int(track_count or 0),
            ),
        )

    async def list_laps(
        self,
        session_id: UUID,
        *,
        limit: int = 100,
        offset: int = 0,
        driver: str | None = None,
        lap_from: int | None = None,
        lap_to: int | None = None,
    ) -> LapPage:
        """Laps ordered by lap number then driver; ``driver`` is an abbreviation or UUID
        and must belong to the session."""
        await self._require_session(session_id)

        filters: list[ColumnElement[bool]] = [Lap.session_id == session_id]
        if driver is not None:
            filters.append(Driver.id == await self.resolve_driver_id(session_id, driver))
        if lap_from is not None:
            filters.append(Lap.lap_number >= lap_from)
        if lap_to is not None:
            filters.append(Lap.lap_number <= lap_to)

        base = (
            select(Lap)
            .join(Driver, Lap.driver_id == Driver.id)
            .where(*filters)
            .options(selectinload(Lap.sectors), selectinload(Lap.driver))
            .order_by(Lap.lap_number.asc(), Driver.abbreviation.asc())
        )

        total = await self._session.scalar(
            select(func.count())
            .select_from(Lap)
            .join(Driver, Lap.driver_id == Driver.id)
            .where(*filters)
        )

        rows = (await self._session.scalars(base.limit(limit).offset(offset))).all()

        items = [
            LapOut(
                id=lap.id,
                driver_abbreviation=lap.driver.abbreviation,
                lap_number=lap.lap_number,
                lap_time_ms=lap.lap_time_ms,
                position=lap.position,
                compound=lap.compound,
                tyre_age_laps=lap.tyre_age_laps,
                stint_number=lap.stint_number,
                is_deleted=lap.is_deleted,
                is_accurate=lap.is_accurate,
                lap_start_time_ms=lap.lap_start_time_ms,
                lap_end_time_ms=lap.lap_end_time_ms,
                pit_in_time_ms=lap.pit_in_time_ms,
                pit_out_time_ms=lap.pit_out_time_ms,
                is_pit_in_lap=lap.is_pit_in_lap,
                is_pit_out_lap=lap.is_pit_out_lap,
                pit_duration_ms=lap.pit_duration_ms,
                sectors=[
                    SectorOut(
                        sector_number=sector.sector_number,
                        sector_time_ms=sector.sector_time_ms,
                    )
                    for sector in sorted(lap.sectors, key=lambda s: s.sector_number)
                ],
            )
            for lap in rows
        ]
        return LapPage(items=items, total=int(total or 0), limit=limit, offset=offset)

    async def list_stints(self, session_id: UUID, *, driver: str | None = None) -> list[StintOut]:
        await self._require_session(session_id)
        filters: list[ColumnElement[bool]] = [TyreStint.session_id == session_id]
        if driver is not None:
            filters.append(TyreStint.driver_id == await self.resolve_driver_id(session_id, driver))
        rows = (
            await self._session.scalars(
                select(TyreStint)
                .join(Driver, TyreStint.driver_id == Driver.id)
                .where(*filters)
                .options(selectinload(TyreStint.driver))
                .order_by(Driver.abbreviation.asc(), TyreStint.stint_number.asc())
            )
        ).all()
        return [
            StintOut(
                id=stint.id,
                driver_abbreviation=stint.driver.abbreviation,
                stint_number=stint.stint_number,
                compound=stint.compound,
                start_lap=stint.start_lap,
                end_lap=stint.end_lap,
                tyre_age_at_start=stint.tyre_age_at_start,
            )
            for stint in rows
        ]

    async def list_track_status(self, session_id: UUID) -> list[TrackStatusOut]:
        await self._require_session(session_id)
        rows = (
            await self._session.scalars(
                select(TrackStatusPeriod)
                .where(TrackStatusPeriod.session_id == session_id)
                .order_by(TrackStatusPeriod.sequence.asc())
            )
        ).all()
        return [
            TrackStatusOut(
                id=row.id,
                race_time_ms=row.race_time_ms,
                status=row.status,
                source_code=row.source_code,
                message=row.message,
                sequence=row.sequence,
            )
            for row in rows
        ]

    async def resolve_driver_id(self, session_id: UUID, driver: str) -> UUID:
        """Driver UUID for an abbreviation or UUID within the session."""
        driver_id = await self._session.scalar(
            select(Driver.id).where(Driver.session_id == session_id, driver_condition(driver))
        )
        if driver_id is None:
            raise DriverNotFoundError(session_id, driver)
        return driver_id

    async def _require_session(self, session_id: UUID) -> RaceSession:
        race_session = await self._session.scalar(
            select(RaceSession).where(RaceSession.id == session_id)
        )
        if race_session is None:
            raise SessionNotFoundError(session_id)
        return race_session


def _race_fields(race: Race) -> dict[str, object]:
    sessions = sorted(race.sessions, key=lambda s: s.session_type.value)
    return {
        "id": race.id,
        "season": race.season,
        "round": race.round,
        "name": race.name,
        "official_name": race.official_name,
        "country": race.country,
        "location": race.location,
        "event_date": race.event_date,
        "sessions": [
            SessionSummary(
                id=item.id,
                session_type=item.session_type,
                name=item.name,
                start_time=item.start_time,
                end_time=item.end_time,
            )
            for item in sessions
        ],
    }


def _driver_summary(driver: Driver) -> DriverSummary:
    return DriverSummary(
        id=driver.id,
        driver_number=driver.driver_number,
        abbreviation=driver.abbreviation,
        full_name=driver.full_name,
        first_name=driver.first_name,
        last_name=driver.last_name,
        team_name=driver.team_name,
        grid_position=driver.grid_position,
        finish_position=driver.finish_position,
        result_status=driver.result_status,
    )
