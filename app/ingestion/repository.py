"""ORM persistence for normalized session imports."""

from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import SessionType
from app.ingestion.errors import PersistenceError
from app.ingestion.records import ExistingSessionInfo, NormalizedSession, PersistedImport
from app.models import (
    Driver,
    Lap,
    Race,
    RaceSession,
    Sector,
    TrackStatusPeriod,
    TyreStint,
)

logger = logging.getLogger(__name__)


class ImportRepository:
    """Persist a normalized session graph into PostgreSQL."""

    async def find_existing_by_round(
        self,
        session: AsyncSession,
        season: int,
        round_number: int,
        session_type: SessionType,
    ) -> ExistingSessionInfo | None:
        """Return an already-imported session for season/round/type, if any."""
        row = await session.execute(
            select(Race, RaceSession)
            .join(RaceSession, RaceSession.race_id == Race.id)
            .where(
                Race.season == season,
                Race.round == round_number,
                RaceSession.session_type == session_type,
            )
        )
        matched = row.first()
        if matched is None:
            return None
        race, race_session = matched
        counts = await self._session_counts(session, race_session.id)
        return ExistingSessionInfo(
            race_id=race.id,
            session_id=race_session.id,
            season=race.season,
            round=race.round,
            event_name=race.name,
            session_type=race_session.session_type,
            **counts,
        )

    async def persist(
        self,
        session: AsyncSession,
        normalized: NormalizedSession,
        *,
        replace: bool,
    ) -> PersistedImport:
        try:
            return await self._persist(session, normalized, replace=replace)
        except IntegrityError as exc:
            raise PersistenceError(
                "Failed to persist imported session due to a data integrity conflict"
            ) from exc

    async def _persist(
        self,
        session: AsyncSession,
        normalized: NormalizedSession,
        *,
        replace: bool,
    ) -> PersistedImport:
        race = await session.scalar(
            select(Race).where(
                Race.season == normalized.season,
                Race.round == normalized.round,
            )
        )
        if race is None:
            race = Race(
                season=normalized.season,
                round=normalized.round,
                name=normalized.event_name,
                official_name=normalized.official_event_name,
                circuit_name=None,
                country=normalized.country,
                location=normalized.location,
                event_date=normalized.event_date,
            )
            session.add(race)
            await session.flush()

        existing_session = await session.scalar(
            select(RaceSession).where(
                RaceSession.race_id == race.id,
                RaceSession.session_type == normalized.session_type,
            )
        )

        if existing_session is not None and not replace:
            counts = await self._session_counts(session, existing_session.id)
            return PersistedImport(
                already_present=True,
                replaced=False,
                race_id=race.id,
                session_id=existing_session.id,
                season=race.season,
                round=race.round,
                event_name=race.name,
                session_type=existing_session.session_type,
                **counts,
            )

        replaced = False
        if existing_session is not None and replace:
            await session.delete(existing_session)
            await session.flush()
            replaced = True

        race_session = RaceSession(
            race_id=race.id,
            session_type=normalized.session_type,
            name=normalized.session_name,
            start_time=normalized.session_start,
            end_time=None,
        )
        session.add(race_session)
        await session.flush()

        drivers = [
            Driver(
                session_id=race_session.id,
                driver_number=item.driver_number,
                abbreviation=item.abbreviation,
                full_name=item.full_name,
                first_name=item.first_name,
                last_name=item.last_name,
                team_name=item.team_name,
                grid_position=item.grid_position,
                finish_position=item.finish_position,
                result_status=item.result_status,
            )
            for item in normalized.drivers
        ]
        session.add_all(drivers)
        await session.flush()

        driver_by_abbr = {driver.abbreviation: driver for driver in drivers}

        laps: list[Lap] = []
        lap_sector_pairs: list[tuple[Lap, list]] = []
        for item in normalized.laps:
            driver = driver_by_abbr[item.driver_abbreviation]
            lap = Lap(
                session_id=race_session.id,
                driver_id=driver.id,
                lap_number=item.lap_number,
                lap_time_ms=item.lap_time_ms,
                position=item.position,
                compound=item.compound,
                tyre_age_laps=item.tyre_age_laps,
                stint_number=item.stint_number,
                is_deleted=item.is_deleted,
                is_accurate=item.is_accurate,
                lap_start_time_ms=item.lap_start_time_ms,
                lap_end_time_ms=item.lap_end_time_ms,
                pit_in_time_ms=item.pit_in_time_ms,
                pit_out_time_ms=item.pit_out_time_ms,
                is_pit_in_lap=item.is_pit_in_lap,
                is_pit_out_lap=item.is_pit_out_lap,
                pit_duration_ms=item.pit_duration_ms,
            )
            laps.append(lap)
            lap_sector_pairs.append((lap, item.sectors))

        session.add_all(laps)
        await session.flush()

        sectors: list[Sector] = []
        for lap, sector_items in lap_sector_pairs:
            for sector_item in sector_items:
                sectors.append(
                    Sector(
                        lap_id=lap.id,
                        sector_number=sector_item.sector_number,
                        sector_time_ms=sector_item.sector_time_ms,
                    )
                )
        session.add_all(sectors)
        await session.flush()

        stints = [
            TyreStint(
                session_id=race_session.id,
                driver_id=driver_by_abbr[item.driver_abbreviation].id,
                stint_number=item.stint_number,
                compound=item.compound,
                start_lap=item.start_lap,
                end_lap=item.end_lap,
                tyre_age_at_start=item.tyre_age_at_start,
            )
            for item in normalized.stints
        ]
        session.add_all(stints)
        await session.flush()

        track_periods = [
            TrackStatusPeriod(
                session_id=race_session.id,
                race_time_ms=item.race_time_ms,
                status=item.status,
                source_code=item.source_code,
                message=item.message,
                sequence=item.sequence,
            )
            for item in normalized.track_statuses
        ]
        session.add_all(track_periods)
        await session.flush()

        return PersistedImport(
            already_present=False,
            replaced=replaced,
            race_id=race.id,
            session_id=race_session.id,
            season=race.season,
            round=race.round,
            event_name=race.name,
            session_type=race_session.session_type,
            driver_count=len(drivers),
            lap_count=len(laps),
            sector_count=len(sectors),
            stint_count=len(stints),
            track_status_count=len(track_periods),
        )

    async def _session_counts(
        self,
        session: AsyncSession,
        session_id: UUID,
    ) -> dict[str, int]:
        driver_count = await session.scalar(
            select(func.count()).select_from(Driver).where(Driver.session_id == session_id)
        )
        lap_count = await session.scalar(
            select(func.count()).select_from(Lap).where(Lap.session_id == session_id)
        )
        sector_count = await session.scalar(
            select(func.count())
            .select_from(Sector)
            .join(Lap, Sector.lap_id == Lap.id)
            .where(Lap.session_id == session_id)
        )
        stint_count = await session.scalar(
            select(func.count()).select_from(TyreStint).where(TyreStint.session_id == session_id)
        )
        track_status_count = await session.scalar(
            select(func.count())
            .select_from(TrackStatusPeriod)
            .where(TrackStatusPeriod.session_id == session_id)
        )
        return {
            "driver_count": int(driver_count or 0),
            "lap_count": int(lap_count or 0),
            "sector_count": int(sector_count or 0),
            "stint_count": int(stint_count or 0),
            "track_status_count": int(track_status_count or 0),
        }
