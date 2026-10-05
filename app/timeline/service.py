"""Timeline application service: generate, read and summarize timelines."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import EventType
from app.services.race_queries import SessionNotFoundError
from app.timeline.builder import build_timeline
from app.timeline.errors import (
    TimelineConflictError,
    TimelineError,
    TimelineNotGeneratedError,
)
from app.timeline.events import TIMELINE_SCHEMA_VERSION
from app.timeline.repository import StoredEvent, StoredTimeline, TimelineRepository

logger = logging.getLogger(__name__)

_CONFLICT_CONSTRAINTS = frozenset(
    {"uq_session_timelines_session_id", "uq_race_events_session_id_sequence"}
)
_SQLITE_CONFLICT_MARKERS = (
    "unique constraint failed: session_timelines.session_id",
    "unique constraint failed: race_events.session_id, race_events.sequence",
)


def _is_timeline_unique_violation(exc: IntegrityError) -> bool:
    """True only for unique violations that mean "another generation won".

    Postgres (asyncpg) exposes the constraint name on the driver exception; SQLite
    only provides message text.
    """
    orig = exc.orig
    for candidate in (orig, getattr(orig, "__cause__", None)):
        name = getattr(candidate, "constraint_name", None)
        if name is not None:
            return name in _CONFLICT_CONSTRAINTS
    message = str(orig).lower()
    return any(marker in message for marker in _SQLITE_CONFLICT_MARKERS)


TimelineStatus = Literal["generated", "regenerated", "already_generated", "available"]


@dataclass(frozen=True, slots=True)
class TimelineSummary:
    status: TimelineStatus
    session_id: UUID
    schema_version: int
    is_current_schema_version: bool
    generated_at: datetime
    race_start_session_time_ms: int
    event_count: int
    counts_by_type: dict[str, int]
    warnings: list[str]


@dataclass(frozen=True, slots=True)
class TimelineEventPage:
    items: list[StoredEvent]
    total: int
    limit: int
    offset: int


class TimelineService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repo = TimelineRepository(session)

    async def generate(self, session_id: UUID, *, regenerate: bool = False) -> TimelineSummary:
        logger.info(
            "Timeline generation started session_id=%s regenerate=%s", session_id, regenerate
        )
        # Serialize generation per session: hold a row lock on the parent session
        # for the rest of this transaction (ignored by SQLite).
        if not await self._repo.lock_session(session_id):
            raise SessionNotFoundError(session_id)
        source = await self._repo.load_source(session_id)
        if source is None:
            await self._session.rollback()
            raise SessionNotFoundError(session_id)

        existing = await self._repo.get_timeline(session_id)
        if existing is not None and not regenerate:
            if existing.schema_version != TIMELINE_SCHEMA_VERSION:
                logger.warning(
                    "Timeline already generated with outdated schema session_id=%s "
                    "stored_version=%d current_version=%d; regenerate to upgrade",
                    session_id,
                    existing.schema_version,
                    TIMELINE_SCHEMA_VERSION,
                )
            logger.info(
                "Timeline already generated session_id=%s events=%d",
                session_id,
                existing.event_count,
            )
            summary = await self._summary(existing, "already_generated")
            await self._session.rollback()  # release the session lock
            return summary

        try:
            built = build_timeline(source)
        except TimelineError as exc:
            await self._session.rollback()
            logger.warning(
                "Timeline generation failed session_id=%s race_id=%s reason=%s",
                session_id,
                source.race_id,
                exc.message,
            )
            raise

        type_counts = Counter(event.event_type.value for event in built.events)
        logger.info(
            "Timeline built session_id=%s race_id=%s events=%d counts=%s warnings=%d "
            "validation=passed",
            session_id,
            source.race_id,
            len(built.events),
            dict(sorted(type_counts.items())),
            len(built.warnings),
        )

        try:
            stored = await self._repo.replace_timeline(
                session_id,
                built,
                schema_version=TIMELINE_SCHEMA_VERSION,
                generated_at=datetime.now(UTC),
            )
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if not _is_timeline_unique_violation(exc):
                logger.exception("Timeline persist integrity error session_id=%s", session_id)
                raise
            logger.warning("Timeline persist conflict session_id=%s", session_id)
            raise TimelineConflictError(
                "A timeline for this session is being generated concurrently; retry shortly"
            ) from exc
        except Exception:
            await self._session.rollback()
            logger.exception("Timeline persist failed session_id=%s", session_id)
            raise

        logger.info(
            "Timeline persisted session_id=%s events=%d regenerated=%s",
            session_id,
            stored.event_count,
            existing is not None,
        )
        return TimelineSummary(
            status="regenerated" if existing is not None else "generated",
            session_id=session_id,
            schema_version=stored.schema_version,
            is_current_schema_version=stored.schema_version == TIMELINE_SCHEMA_VERSION,
            generated_at=stored.generated_at,
            race_start_session_time_ms=stored.race_start_session_time_ms,
            event_count=stored.event_count,
            counts_by_type=dict(sorted(type_counts.items())),
            warnings=stored.warnings,
        )

    async def get_summary(self, session_id: UUID) -> TimelineSummary:
        stored = await self._require_timeline(session_id)
        return await self._summary(stored, "available")

    async def get_events(
        self,
        session_id: UUID,
        *,
        driver: str | None = None,
        event_types: list[EventType] | None = None,
        lap_from: int | None = None,
        lap_to: int | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> TimelineEventPage:
        await self._require_timeline(session_id)
        items, total = await self._repo.list_events(
            session_id,
            driver=driver,
            event_types=event_types,
            lap_from=lap_from,
            lap_to=lap_to,
            limit=limit,
            offset=offset,
        )
        return TimelineEventPage(items=items, total=total, limit=limit, offset=offset)

    async def _require_timeline(self, session_id: UUID) -> StoredTimeline:
        if not await self._repo.session_exists(session_id):
            raise SessionNotFoundError(session_id)
        stored = await self._repo.get_timeline(session_id)
        if stored is None:
            raise TimelineNotGeneratedError(
                f"No timeline has been generated for session {session_id}; "
                f"POST /sessions/{session_id}/timeline to generate it"
            )
        return stored

    async def _summary(self, stored: StoredTimeline, status: TimelineStatus) -> TimelineSummary:
        counts = await self._repo.counts_by_type(stored.session_id)
        return TimelineSummary(
            status=status,
            session_id=stored.session_id,
            schema_version=stored.schema_version,
            is_current_schema_version=stored.schema_version == TIMELINE_SCHEMA_VERSION,
            generated_at=stored.generated_at,
            race_start_session_time_ms=stored.race_start_session_time_ms,
            event_count=stored.event_count,
            counts_by_type=counts,
            warnings=stored.warnings,
        )
