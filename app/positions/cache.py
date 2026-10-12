"""Loads a session's track samples once and keeps a few sessions in memory.

The WebSocket gateway asks for a frame several times a second per running replay;
only the first request of a session touches PostgreSQL. Sessions without track data
are cached as ``None`` too, so they cost nothing per tick. A failed load is not
retried for ``retry_after_seconds`` (a database outage must not be hit and logged on
every tick). Re-importing a session's track data must call ``invalidate``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Driver, DriverTrackSample
from app.positions.track import PositionTrack, TrackRow

logger = logging.getLogger(__name__)


class PositionTrackCache:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        max_gap_ms: int,
        max_sessions: int,
        retry_after_seconds: float = 30.0,
    ) -> None:
        self._session_factory = session_factory
        self._max_gap_ms = max_gap_ms
        self._max_sessions = max_sessions
        self._tracks: OrderedDict[UUID, PositionTrack | None] = OrderedDict()
        # One load per session even when several replays ask at once.
        self._loading: dict[UUID, asyncio.Task[PositionTrack | None]] = {}
        # Bumped by ``invalidate`` so a load that started before it is not cached.
        self._generation = 0
        self._retry_after = retry_after_seconds
        self._failed_until: dict[UUID, float] = {}

    async def get(self, session_id: UUID) -> PositionTrack | None:
        """The session's track, or ``None`` when it has no track data."""
        if session_id in self._tracks:
            self._tracks.move_to_end(session_id)
            return self._tracks[session_id]
        loop = asyncio.get_running_loop()
        if self._failed_until.get(session_id, 0.0) > loop.time():
            return None
        generation = self._generation
        task = self._loading.get(session_id)
        if task is None:
            task = asyncio.create_task(self._load(session_id), name=f"track-load-{session_id}")
            self._loading[session_id] = task
            task.add_done_callback(lambda done: self._forget(session_id, done))
        try:
            track = await asyncio.shield(task)
        except Exception:
            if self._failed_until.get(session_id, 0.0) <= loop.time():
                logger.exception("Track data load failed session_id=%s", session_id)
                self._failed_until[session_id] = loop.time() + self._retry_after
            return None
        self._failed_until.pop(session_id, None)
        if generation == self._generation:
            self._store(session_id, track)
        return track

    def invalidate(self, session_id: UUID) -> None:
        self._generation += 1
        self._tracks.pop(session_id, None)
        self._loading.pop(session_id, None)
        self._failed_until.pop(session_id, None)

    def _forget(self, session_id: UUID, task: asyncio.Task[PositionTrack | None]) -> None:
        if self._loading.get(session_id) is task:
            del self._loading[session_id]

    def _store(self, session_id: UUID, track: PositionTrack | None) -> None:
        self._tracks[session_id] = track
        self._tracks.move_to_end(session_id)
        while len(self._tracks) > self._max_sessions:
            self._tracks.popitem(last=False)

    async def _load(self, session_id: UUID) -> PositionTrack | None:
        async with self._session_factory() as db:
            order = list(
                await db.scalars(
                    select(Driver.id)
                    .where(Driver.session_id == session_id)
                    .order_by(Driver.abbreviation)
                )
            )
            result = await db.execute(
                select(
                    DriverTrackSample.driver_id,
                    DriverTrackSample.race_time_ms,
                    DriverTrackSample.x,
                    DriverTrackSample.y,
                    DriverTrackSample.off_track,
                    DriverTrackSample.speed_kph,
                    DriverTrackSample.rpm,
                    DriverTrackSample.gear,
                    DriverTrackSample.throttle_pct,
                    DriverTrackSample.brake,
                    DriverTrackSample.drs_code,
                )
                .where(DriverTrackSample.session_id == session_id)
                .order_by(DriverTrackSample.driver_id, DriverTrackSample.race_time_ms)
            )
            rows = result.all()
        if not rows:
            logger.info("No track data for session_id=%s", session_id)
            return None
        # Building the arrays is CPU work over ~250k rows: keep it off the event loop.
        track = await asyncio.to_thread(
            PositionTrack,
            (TrackRow(*row) for row in rows),
            max_gap_ms=self._max_gap_ms,
            order=order,
        )
        logger.info("Track data loaded session_id=%s samples=%d", session_id, len(rows))
        return track
