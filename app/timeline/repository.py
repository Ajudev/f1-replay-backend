"""Timeline persistence: load builder input, write and read events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import EventType
from app.models import (
    Driver,
    Lap,
    Race,
    RaceEvent,
    RaceSession,
    SessionTimeline,
    TrackStatusPeriod,
)
from app.timeline.builder import BuiltTimeline
from app.timeline.source import (
    SourceDriver,
    SourceLap,
    SourceTrackStatus,
    TimelineSource,
)


@dataclass(frozen=True, slots=True)
class StoredEvent:
    id: UUID
    sequence: int
    event_type: EventType
    race_time_ms: int | None
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class StoredTimeline:
    session_id: UUID
    schema_version: int
    generated_at: datetime
    race_start_session_time_ms: int
    event_count: int
    warnings: list[str]


def _stored(row: SessionTimeline) -> StoredTimeline:
    return StoredTimeline(
        session_id=row.session_id,
        schema_version=row.schema_version,
        generated_at=row.generated_at,
        race_start_session_time_ms=row.race_start_session_time_ms,
        event_count=row.event_count,
        warnings=list(row.warnings or []),
    )


class TimelineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def load_source(self, session_id: UUID) -> TimelineSource | None:
        """Load builder input with four queries, regardless of race size."""
        head = (
            await self._session.execute(
                select(RaceSession.session_type, Race.id, Race.season, Race.round)
                .join(Race, Race.id == RaceSession.race_id)
                .where(RaceSession.id == session_id)
            )
        ).first()
        if head is None:
            return None
        session_type, race_id, season, round_number = head

        driver_rows = (
            await self._session.execute(
                select(Driver.id, Driver.abbreviation, Driver.grid_position).where(
                    Driver.session_id == session_id
                )
            )
        ).all()
        lap_rows = (
            await self._session.execute(
                select(
                    Lap.driver_id,
                    Lap.lap_number,
                    Lap.lap_time_ms,
                    Lap.position,
                    Lap.compound,
                    Lap.tyre_age_laps,
                    Lap.stint_number,
                    Lap.is_deleted,
                    Lap.is_accurate,
                    Lap.lap_start_time_ms,
                    Lap.lap_end_time_ms,
                    Lap.pit_in_time_ms,
                    Lap.pit_out_time_ms,
                    Lap.is_pit_in_lap,
                    Lap.is_pit_out_lap,
                ).where(Lap.session_id == session_id)
            )
        ).all()
        status_rows = (
            await self._session.execute(
                select(
                    TrackStatusPeriod.sequence,
                    TrackStatusPeriod.race_time_ms,
                    TrackStatusPeriod.status,
                    TrackStatusPeriod.source_code,
                ).where(TrackStatusPeriod.session_id == session_id)
            )
        ).all()

        return TimelineSource(
            session_id=session_id,
            race_id=race_id,
            session_type=session_type,
            season=season,
            round=round_number,
            drivers=[SourceDriver(*row) for row in driver_rows],
            laps=[SourceLap(*row) for row in lap_rows],
            track_statuses=[SourceTrackStatus(*row) for row in status_rows],
        )

    async def lock_session(self, session_id: UUID) -> bool:
        """Lock the session row until the transaction ends; False if it is missing."""
        found = await self._session.scalar(
            select(RaceSession.id).where(RaceSession.id == session_id).with_for_update()
        )
        return found is not None

    async def session_exists(self, session_id: UUID) -> bool:
        found = await self._session.scalar(
            select(RaceSession.id).where(RaceSession.id == session_id)
        )
        return found is not None

    async def get_timeline(self, session_id: UUID) -> StoredTimeline | None:
        row = await self._session.scalar(
            select(SessionTimeline).where(SessionTimeline.session_id == session_id)
        )
        return _stored(row) if row is not None else None

    async def counts_by_type(self, session_id: UUID) -> dict[str, int]:
        rows = (
            await self._session.execute(
                select(RaceEvent.event_type, func.count())
                .where(RaceEvent.session_id == session_id)
                .group_by(RaceEvent.event_type)
            )
        ).all()
        return {event_type.value: int(count) for event_type, count in sorted(rows)}

    async def replace_timeline(
        self,
        session_id: UUID,
        built: BuiltTimeline,
        *,
        schema_version: int,
        generated_at: datetime,
    ) -> StoredTimeline:
        """Delete any prior timeline and bulk-insert the new one (no commit)."""
        await self._session.execute(delete(RaceEvent).where(RaceEvent.session_id == session_id))
        await self._session.execute(
            delete(SessionTimeline).where(SessionTimeline.session_id == session_id)
        )
        await self._session.flush()

        row = SessionTimeline(
            session_id=session_id,
            schema_version=schema_version,
            generated_at=generated_at,
            race_start_session_time_ms=built.race_start_session_time_ms,
            event_count=len(built.events),
            warnings=list(built.warnings),
        )
        self._session.add(row)
        await self._session.flush()

        if built.events:
            await self._session.execute(
                insert(RaceEvent),
                [
                    {
                        "session_id": session_id,
                        "event_type": event.event_type,
                        "driver_id": event.driver_id,
                        "lap_number": event.lap_number,
                        "race_time_ms": event.race_time_ms,
                        "sequence": event.sequence,
                        "payload": event.payload,
                    }
                    for event in built.events
                ],
            )
        await self._session.flush()
        return _stored(row)

    async def load_events(
        self,
        session_id: UUID,
        *,
        from_sequence: int | None = None,
        before_sequence: int | None = None,
    ) -> list[StoredEvent]:
        """Stored events in ``sequence`` order (one query): all of them unless
        ``from_sequence`` (inclusive) / ``before_sequence`` (exclusive) bound it."""
        filters: list[Any] = [RaceEvent.session_id == session_id]
        if from_sequence is not None:
            filters.append(RaceEvent.sequence >= from_sequence)
        if before_sequence is not None:
            filters.append(RaceEvent.sequence < before_sequence)
        joined = RaceEvent.__table__.outerjoin(  # type: ignore[attr-defined]
            Driver.__table__,  # type: ignore[attr-defined]
            (RaceEvent.driver_id == Driver.id) & (RaceEvent.session_id == Driver.session_id),
        )
        rows = (
            await self._session.execute(
                select(
                    RaceEvent.id,
                    RaceEvent.sequence,
                    RaceEvent.event_type,
                    RaceEvent.race_time_ms,
                    RaceEvent.lap_number,
                    RaceEvent.driver_id,
                    Driver.abbreviation,
                    RaceEvent.payload,
                )
                .select_from(joined)
                .where(*filters)
                .order_by(RaceEvent.sequence.asc())
            )
        ).all()
        return [StoredEvent(*row) for row in rows]

    async def list_events(
        self,
        session_id: UUID,
        *,
        driver: str | None,
        event_types: list[EventType] | None,
        lap_from: int | None,
        lap_to: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[StoredEvent], int]:
        filters: list[Any] = [RaceEvent.session_id == session_id]
        if driver is not None:
            filters.append(Driver.abbreviation == driver.strip().upper())
        if event_types:
            filters.append(RaceEvent.event_type.in_(event_types))
        if lap_from is not None:
            filters.append(RaceEvent.lap_number >= lap_from)
        if lap_to is not None:
            filters.append(RaceEvent.lap_number <= lap_to)

        joined = RaceEvent.__table__.outerjoin(  # type: ignore[attr-defined]
            Driver.__table__,  # type: ignore[attr-defined]
            (RaceEvent.driver_id == Driver.id) & (RaceEvent.session_id == Driver.session_id),
        )
        total = await self._session.scalar(select(func.count()).select_from(joined).where(*filters))
        rows = (
            await self._session.execute(
                select(
                    RaceEvent.id,
                    RaceEvent.sequence,
                    RaceEvent.event_type,
                    RaceEvent.race_time_ms,
                    RaceEvent.lap_number,
                    RaceEvent.driver_id,
                    Driver.abbreviation,
                    RaceEvent.payload,
                )
                .select_from(joined)
                .where(*filters)
                .order_by(RaceEvent.sequence.asc())
                .limit(limit)
                .offset(offset)
            )
        ).all()
        return [StoredEvent(*row) for row in rows], int(total or 0)
