"""PostgreSQL persistence of detected events."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.detection.errors import ReplayGoneError
from app.detection.models import DetectedEvent
from app.domain.enums import DetectedEventType
from app.models import DetectedEvent as DetectedEventRow
from app.models import ReplaySession

logger = logging.getLogger(__name__)


class DetectedEventSink(Protocol):
    """Where the processor persists detections (replaceable in tests)."""

    async def save(self, events: Sequence[DetectedEvent]) -> None: ...


@dataclass(frozen=True, slots=True)
class DetectedEventQuery:
    replay_id: UUID
    run_id: UUID | None = None
    event_types: tuple[DetectedEventType, ...] = ()
    driver: str | None = None
    lap_from: int | None = None
    lap_to: int | None = None
    limit: int = 100
    offset: int = 0


class DetectedEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_many(
        self, events: Sequence[DetectedEvent], *, created_at: datetime | None = None
    ) -> int:
        """Insert detections (no commit). The deterministic id is the primary key, so an
        event that already exists is silently ignored; returns the rows inserted."""
        if not events:
            return 0
        now = created_at or datetime.now(UTC)
        rows = [
            {
                "id": e.detected_event_id,
                "replay_id": e.replay_id,
                "run_id": e.run_id,
                "session_id": e.session_id,
                "race_id": e.race_id,
                "event_type": e.event_type.value,
                "schema_version": e.schema_version,
                "lap_number": e.lap_number,
                "race_time_ms": e.race_time_ms,
                "source_sequence": e.source_sequence,
                "primary_driver_id": e.primary_driver_id,
                "primary_driver_abbreviation": e.primary_driver_abbreviation,
                "secondary_driver_id": e.secondary_driver_id,
                "secondary_driver_abbreviation": e.secondary_driver_abbreviation,
                "severity": e.severity.value if e.severity else None,
                "confidence": e.confidence,
                "evidence": e.evidence,
                "source_event_ids": [str(i) for i in e.source_event_ids],
                "detector_name": e.detector_name,
                "detector_version": e.detector_version,
                "detected_at": e.detected_at,
                "created_at": now,
            }
            for e in events
        ]
        dialect = self._session.get_bind().dialect.name
        insert = pg_insert if dialect == "postgresql" else sqlite_insert
        result = await self._session.execute(
            insert(DetectedEventRow).values(rows).on_conflict_do_nothing(index_elements=["id"])
        )
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    async def replay_exists(self, replay_id: UUID) -> bool:
        return (
            await self._session.scalar(
                select(ReplaySession.id).where(ReplaySession.id == replay_id)
            )
        ) is not None

    async def latest_run_id(self, replay_id: UUID) -> UUID | None:
        """Run of the most recently written detection of the replay."""
        return await self._session.scalar(
            select(DetectedEventRow.run_id)
            .where(DetectedEventRow.replay_id == replay_id)
            .order_by(DetectedEventRow.created_at.desc(), DetectedEventRow.source_sequence.desc())
            .limit(1)
        )

    async def list(self, query: DetectedEventQuery) -> tuple[list[DetectedEventRow], int]:
        conditions = self._conditions(query)
        total = await self._session.scalar(
            select(func.count()).select_from(DetectedEventRow).where(*conditions)
        )
        rows = await self._session.scalars(
            select(DetectedEventRow)
            .where(*conditions)
            .order_by(
                DetectedEventRow.source_sequence, DetectedEventRow.event_type, DetectedEventRow.id
            )
            .limit(query.limit)
            .offset(query.offset)
        )
        return list(rows), int(total or 0)

    @staticmethod
    def _conditions(query: DetectedEventQuery) -> list[ColumnElement[bool]]:
        row = DetectedEventRow
        conditions: list[ColumnElement[bool]] = [row.replay_id == query.replay_id]
        if query.run_id is not None:
            conditions.append(row.run_id == query.run_id)
        if query.event_types:
            conditions.append(row.event_type.in_([t.value for t in query.event_types]))
        if query.lap_from is not None:
            conditions.append(row.lap_number >= query.lap_from)
        if query.lap_to is not None:
            conditions.append(row.lap_number <= query.lap_to)
        if query.driver is not None:
            wanted = query.driver.strip()
            try:
                driver_id = UUID(wanted)
            except ValueError:
                conditions.append(
                    or_(
                        func.upper(row.primary_driver_abbreviation) == wanted.upper(),
                        func.upper(row.secondary_driver_abbreviation) == wanted.upper(),
                    )
                )
            else:
                conditions.append(
                    or_(row.primary_driver_id == driver_id, row.secondary_driver_id == driver_id)
                )
        return conditions


class DatabaseDetectedEventSink:
    """Writes each batch in its own short transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(self, events: Sequence[DetectedEvent]) -> None:
        if not events:
            return
        async with self._session_factory() as db:
            repo = DetectedEventRepository(db)
            try:
                inserted = await repo.insert_many(events)
                await db.commit()
            except IntegrityError as exc:
                await db.rollback()
                # Only a vanished replay is final; any other integrity error is retried.
                if not await repo.replay_exists(events[0].replay_id):
                    raise ReplayGoneError(f"Replay {events[0].replay_id} no longer exists") from exc
                raise
        logger.debug(
            "Detected events persisted replay_id=%s received=%d inserted=%d",
            events[0].replay_id,
            len(events),
            inserted,
        )
