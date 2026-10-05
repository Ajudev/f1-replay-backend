"""Replay persistence: replay_sessions rows and the timeline they replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import ReplayStatus
from app.models import RaceSession, ReplaySession
from app.replay.errors import ReplayTimelineUnavailableError
from app.replay.runner import TimelineEntry
from app.replay.state import ACTIVE_STATUSES, ReplayState
from app.timeline.repository import TimelineRepository


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    state: ReplayState
    race_id: UUID
    created_at: datetime
    updated_at: datetime


def _state(row: ReplaySession) -> ReplayState:
    return ReplayState(
        replay_id=row.id,
        session_id=row.session_id,
        status=row.status,
        playback_speed=Decimal(row.playback_speed),
        current_race_time_ms=row.current_race_time_ms,
        current_sequence=row.current_sequence,
        current_lap=row.current_lap,
        total_events=row.total_events,
        total_laps=row.total_laps,
        started_at=row.started_at,
        paused_at=row.paused_at,
        ended_at=row.ended_at,
        status_reason=row.status_reason,
    )


class ReplayRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def session_race_id(self, session_id: UUID) -> UUID | None:
        return await self._session.scalar(
            select(RaceSession.race_id).where(RaceSession.id == session_id)
        )

    async def create(self, session_id: UUID, playback_speed: Decimal) -> UUID:
        row = ReplaySession(session_id=session_id, playback_speed=playback_speed)
        self._session.add(row)
        await self._session.flush()
        return row.id

    async def get(self, replay_id: UUID) -> ReplayRecord | None:
        found = (
            await self._session.execute(
                select(ReplaySession, RaceSession.race_id)
                .join(RaceSession, RaceSession.id == ReplaySession.session_id)
                .where(ReplaySession.id == replay_id)
                .execution_options(populate_existing=True)
            )
        ).first()
        if found is None:
            return None
        row, race_id = found
        return ReplayRecord(
            state=_state(row),
            race_id=race_id,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    async def save(self, state: ReplayState) -> None:
        """Write the replay's lifecycle/progress fields (no commit)."""
        await self._session.execute(
            update(ReplaySession)
            .where(ReplaySession.id == state.replay_id)
            .values(
                status=state.status,
                playback_speed=state.playback_speed,
                current_race_time_ms=state.current_race_time_ms,
                current_sequence=state.current_sequence,
                current_lap=state.current_lap,
                total_events=state.total_events,
                total_laps=state.total_laps,
                started_at=state.started_at,
                paused_at=state.paused_at,
                ended_at=state.ended_at,
                status_reason=state.status_reason,
            )
        )

    async def stop_active(self, *, reason: str, ended_at: datetime) -> list[UUID]:
        """Mark every RUNNING/PAUSED replay STOPPED (no commit); returns their ids."""
        ids = list(
            await self._session.scalars(
                select(ReplaySession.id).where(ReplaySession.status.in_(ACTIVE_STATUSES))
            )
        )
        if ids:
            await self._session.execute(
                update(ReplaySession)
                .where(ReplaySession.id.in_(ids), ReplaySession.status.in_(ACTIVE_STATUSES))
                .values(status=ReplayStatus.STOPPED, ended_at=ended_at, status_reason=reason)
            )
        return ids

    async def load_timeline(self, session_id: UUID) -> list[TimelineEntry]:
        """The session's stored timeline in sequence order; never builds one."""
        timelines = TimelineRepository(self._session)
        if await timelines.get_timeline(session_id) is None:
            raise ReplayTimelineUnavailableError(
                f"No timeline has been generated for session {session_id}; "
                f"POST /sessions/{session_id}/timeline to generate it"
            )
        entries: list[TimelineEntry] = []
        for event in await timelines.load_events(session_id):
            if event.race_time_ms is None:
                raise ReplayTimelineUnavailableError(
                    f"Timeline event {event.sequence} has no race time; regenerate the timeline"
                )
            entries.append(
                TimelineEntry(
                    sequence=event.sequence,
                    event_type=event.event_type,
                    race_time_ms=event.race_time_ms,
                    lap_number=event.lap_number,
                    driver_id=event.driver_id,
                    driver_abbreviation=event.driver_abbreviation,
                    payload=event.payload,
                )
            )
        return entries

    async def timeline_exists(self, session_id: UUID) -> bool:
        return await TimelineRepository(self._session).get_timeline(session_id) is not None
