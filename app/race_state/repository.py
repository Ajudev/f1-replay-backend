"""Redis hot store for the current race state.

One JSON document per replay under ``{prefix}:{replay_id}:state``. A state update and
its state event are written in one ``WATCH`` / ``MULTI`` / ``EXEC`` transaction (SET with
TTL + XADD), so a transition is never stored without being published or published
twice, and concurrent workers cannot both apply the same sequence: the loser gets
``StateConflictError`` and its message is retried. No Redis modules or Lua are needed.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from pydantic import ValidationError
from redis.asyncio.client import Pipeline
from redis.exceptions import WatchError

from app.infrastructure.redis import RedisClient
from app.race_state.config import RaceStateConfig
from app.race_state.errors import StateConflictError
from app.race_state.models import RACE_STATE_SCHEMA_VERSION, RaceState
from app.streaming.config import StreamConfig
from app.streaming.envelope import StreamEvent

logger = logging.getLogger(__name__)


def _decode(raw: str | bytes | None, replay_id: UUID) -> RaceState | None:
    """Parse a stored document; an unreadable or other-version one counts as missing
    (it is rebuilt from the timeline)."""
    if raw is None:
        return None
    try:
        state = RaceState.model_validate_json(raw)
    except ValidationError:
        logger.error("Stored race state is unreadable; treating as missing replay_id=%s", replay_id)
        return None
    if state.schema_version != RACE_STATE_SCHEMA_VERSION:
        logger.warning(
            "Stored race state has schema_version=%d (expected %d); treating as missing "
            "replay_id=%s",
            state.schema_version,
            RACE_STATE_SCHEMA_VERSION,
            replay_id,
        )
        return None
    return state


class StateTransaction:
    """A watched read-modify-write of one replay's state (single GET, single EXEC)."""

    def __init__(self, store: RaceStateStore, pipe: Pipeline, replay_id: UUID) -> None:
        self._store = store
        self._pipe = pipe
        self._replay_id = replay_id

    async def load(self) -> RaceState | None:
        """Read the current state; Redis aborts ``commit`` if it changes afterwards."""
        raw = await self._pipe.get(self._store.key(self._replay_id))
        return _decode(raw, self._replay_id)

    async def commit(self, state: RaceState, event: StreamEvent | None) -> None:
        """Store ``state`` (and publish ``event``) atomically, refreshing the TTL."""
        key = self._store.key(self._replay_id)
        document = state.model_dump_json()
        self._pipe.multi()
        self._pipe.set(key, document, ex=self._store.config.ttl_seconds)
        if event is not None:
            maxlen = self._store.stream_config.maxlen
            self._pipe.xadd(
                self._store.stream_config.state_stream,
                event.to_fields(),
                maxlen=maxlen or None,
                approximate=True,
            )
        try:
            await self._pipe.execute()
        except WatchError as exc:
            raise StateConflictError(
                f"Race state of replay {self._replay_id} was changed by another worker"
            ) from exc


class RaceStateStore:
    def __init__(
        self, redis: RedisClient, *, stream_config: StreamConfig, config: RaceStateConfig
    ) -> None:
        self._redis = redis
        self.stream_config = stream_config
        self.config = config

    def key(self, replay_id: UUID) -> str:
        return f"{self.config.key_prefix}:{replay_id}:state"

    async def get(self, replay_id: UUID) -> RaceState | None:
        """Plain read (API side)."""
        return _decode(await self._redis.client.get(self.key(replay_id)), replay_id)

    @asynccontextmanager
    async def transaction(self, replay_id: UUID) -> AsyncIterator[StateTransaction]:
        """``WATCH`` the replay's key; leaving the block (even on error) releases it."""
        async with self._redis.client.pipeline(transaction=True) as pipe:
            await pipe.watch(self.key(replay_id))
            yield StateTransaction(self, pipe, replay_id)
