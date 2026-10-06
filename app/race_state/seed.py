"""Loads what a run's state is initialized from, and the timeline for rebuilds.

Reads PostgreSQL only (never FastF1). Called once per run when its state is created
or rebuilt, never per event.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import EventType
from app.models import Driver, Race, RaceEvent, RaceSession
from app.race_state.errors import RaceSeedError
from app.race_state.models import RaceSeed, SeedDriver
from app.timeline.repository import StoredEvent, TimelineRepository


class RaceSeedLoader:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def load_seed(self, session_id: UUID) -> RaceSeed:
        """Drivers with grid positions (never finishing results, which would leak the
        future), race identity, ``total_laps`` and ``total_events`` of the timeline."""
        async with self._session_factory() as db:
            head = (
                await db.execute(
                    select(RaceSession.session_type, Race.id, Race.season, Race.round)
                    .join(Race, Race.id == RaceSession.race_id)
                    .where(RaceSession.id == session_id)
                )
            ).first()
            if head is None:
                raise RaceSeedError(f"Cannot initialize race state: session {session_id} not found")
            drivers = (
                await db.execute(
                    select(
                        Driver.id,
                        Driver.abbreviation,
                        Driver.driver_number,
                        Driver.full_name,
                        Driver.team_name,
                        Driver.grid_position,
                    )
                    .where(Driver.session_id == session_id)
                    .order_by(Driver.abbreviation)
                )
            ).all()
            total_events = int(
                await db.scalar(
                    select(func.count())
                    .select_from(RaceEvent)
                    .where(RaceEvent.session_id == session_id)
                )
                or 0
            )
            total_laps = await db.scalar(
                select(func.max(RaceEvent.lap_number)).where(
                    RaceEvent.session_id == session_id,
                    RaceEvent.event_type == EventType.LAP_COMPLETED,
                )
            )
        if total_events == 0:
            raise RaceSeedError(
                f"Cannot initialize race state: session {session_id} has no timeline events"
            )
        session_type, race_id, season, round_number = head
        return RaceSeed(
            session_id=session_id,
            race_id=race_id,
            season=season,
            round=round_number,
            session_type=session_type.value,
            total_laps=total_laps,
            total_events=total_events,
            drivers=tuple(SeedDriver(*row) for row in drivers),
        )

    async def load_events_between(
        self, session_id: UUID, from_sequence: int, before_sequence: int
    ) -> list[StoredEvent]:
        """Stored timeline events with ``from_sequence <= sequence < before_sequence``,
        in order (filtered in SQL)."""
        async with self._session_factory() as db:
            return await TimelineRepository(db).load_events(
                session_id, from_sequence=from_sequence, before_sequence=before_sequence
            )
