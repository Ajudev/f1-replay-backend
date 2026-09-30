"""PostgreSQL query helpers for imported races and sessions.

Must not import FastF1 or pandas.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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


class RaceQueryService:
    """Read imported race data from PostgreSQL."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_races(self) -> list[RaceSummary]:
        result = await self._session.scalars(
            select(Race).order_by(Race.season.desc(), Race.round.asc())
        )
        races = result.all()
        return [
            RaceSummary(
                id=race.id,
                season=race.season,
                round=race.round,
                name=race.name,
                official_name=race.official_name,
                country=race.country,
                location=race.location,
                event_date=race.event_date,
            )
            for race in races
        ]

    async def get_race(self, race_id: UUID) -> RaceDetail:
        race = await self._session.scalar(
            select(Race)
            .where(Race.id == race_id)
            .options(selectinload(Race.sessions))
        )
        if race is None:
            raise RaceNotFoundError(race_id)
        sessions = sorted(race.sessions, key=lambda s: s.session_type.value)
        return RaceDetail(
            id=race.id,
            season=race.season,
            round=race.round,
            name=race.name,
            official_name=race.official_name,
            country=race.country,
            location=race.location,
            event_date=race.event_date,
            sessions=[
                SessionSummary(
                    id=item.id,
                    session_type=item.session_type,
                    name=item.name,
                    start_time=item.start_time,
                    end_time=item.end_time,
                )
                for item in sessions
            ],
        )

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
            select(func.count())
            .select_from(TyreStint)
            .where(TyreStint.session_id == session_id)
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
            drivers=[
                DriverSummary(
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
                for driver in drivers
            ],
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
    ) -> LapPage:
        await self._require_session(session_id)

        filters = [Lap.session_id == session_id]
        if driver is not None:
            abbr = driver.strip().upper()
            filters.append(Driver.abbreviation == abbr)

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

        rows = (
            await self._session.scalars(base.limit(limit).offset(offset))
        ).all()

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

    async def list_stints(self, session_id: UUID) -> list[StintOut]:
        await self._require_session(session_id)
        rows = (
            await self._session.scalars(
                select(TyreStint)
                .join(Driver, TyreStint.driver_id == Driver.id)
                .where(TyreStint.session_id == session_id)
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

    async def _require_session(self, session_id: UUID) -> RaceSession:
        race_session = await self._session.scalar(
            select(RaceSession).where(RaceSession.id == session_id)
        )
        if race_session is None:
            raise SessionNotFoundError(session_id)
        return race_session
