"""Persist a synthetic TimelineSource into the test database."""

from __future__ import annotations

from datetime import date
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Driver, Lap, Race, RaceSession, TrackStatusPeriod
from app.timeline.source import TimelineSource


async def seed_source(factory: async_sessionmaker[AsyncSession], src: TimelineSource) -> UUID:
    async with factory() as db:
        db.add(
            Race(
                id=src.race_id,
                season=src.season,
                round=src.round,
                name="Test Grand Prix",
                country="Testland",
                location="Testville",
                event_date=date(2024, 3, 2),
            )
        )
        await db.flush()
        db.add(
            RaceSession(
                id=src.session_id,
                race_id=src.race_id,
                session_type=src.session_type,
                name="Race",
            )
        )
        await db.flush()
        db.add_all(
            Driver(
                id=d.id,
                session_id=src.session_id,
                abbreviation=d.abbreviation,
                full_name=d.abbreviation,
                grid_position=d.grid_position,
            )
            for d in src.drivers
        )
        await db.flush()
        db.add_all(
            Lap(
                session_id=src.session_id,
                driver_id=lap.driver_id,
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
            )
            for lap in src.laps
        )
        db.add_all(
            TrackStatusPeriod(
                session_id=src.session_id,
                race_time_ms=t.session_time_ms,
                status=t.status,
                source_code=t.source_code,
                sequence=t.sequence,
            )
            for t in src.track_statuses
        )
        await db.commit()
    return src.session_id
