"""Read side of the race state: live Redis state, with PostgreSQL snapshot fallback."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID

from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import ReplayStatus
from app.race_state.errors import (
    DriverNotInStateError,
    RaceStateStoreUnavailableError,
    RaceStateUnavailableError,
    ReplayNotStartedError,
)
from app.race_state.models import DriverState, RaceState, StateSource
from app.race_state.repository import RaceStateStore
from app.race_state.snapshots import RaceStateSnapshotRepository
from app.replay.errors import ReplayPersistenceError
from app.replay.service import ReplayService

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RaceStateView:
    state: RaceState
    source: StateSource
    replay_status: ReplayStatus


class RaceStateService:
    def __init__(
        self,
        store: RaceStateStore,
        session_factory: async_sessionmaker[AsyncSession],
        replay_service: ReplayService,
    ) -> None:
        self._store = store
        self._session_factory = session_factory
        self._replays = replay_service

    async def get_state(self, replay_id: UUID) -> RaceStateView:
        """Current state of a replay.

        ``ReplayNotFoundError`` (unknown replay), ``ReplayNotStartedError`` (still
        ``CREATED``), ``RaceStateUnavailableError`` (no live state and no snapshot) and
        ``RaceStateStoreUnavailableError`` (Redis failure; deliberately no silent
        fallback to an older snapshot, which would present stale data as current).
        PostgreSQL failures surface as ``ReplayPersistenceError`` (503).
        """
        replay = await self._replays.get(replay_id)
        status = replay.state.status
        if status is ReplayStatus.CREATED:
            raise ReplayNotStartedError(replay_id)

        try:
            live = await self._store.get(replay_id)
        except (RedisError, OSError, RuntimeError) as exc:
            logger.error("Race state store unavailable replay_id=%s", replay_id, exc_info=exc)
            raise RaceStateStoreUnavailableError from exc
        if live is not None:
            return RaceStateView(live, StateSource.LIVE, status)

        try:
            async with self._session_factory() as db:
                snapshot = await RaceStateSnapshotRepository(db).latest(replay_id)
        except (SQLAlchemyError, OSError) as exc:
            logger.exception("Race state snapshot read failed replay_id=%s", replay_id)
            raise ReplayPersistenceError("Replay storage unavailable") from exc
        if snapshot is None:
            raise RaceStateUnavailableError(replay_id)
        return RaceStateView(snapshot.state, StateSource.SNAPSHOT, status)

    async def get_driver(self, replay_id: UUID, driver: str) -> tuple[RaceStateView, DriverState]:
        """One driver by abbreviation (case-insensitive) or UUID."""
        view = await self.get_state(replay_id)
        wanted = driver.strip()
        match: DriverState | None = None
        try:
            match = view.state.drivers.get(UUID(wanted))
        except ValueError:
            for candidate in view.state.drivers.values():
                if candidate.abbreviation.upper() == wanted.upper():
                    match = candidate
                    break
        if match is None:
            raise DriverNotInStateError(replay_id, driver)
        return view, match
