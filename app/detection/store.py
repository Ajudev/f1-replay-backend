"""Redis store for the per-replay detection context.

One JSON document per replay under ``{prefix}:{replay_id}:detection`` (TTL refreshed on
every write). The context update and the XADDs of the events detected in the same step
are committed in one ``WATCH`` / ``MULTI`` / ``EXEC`` transaction, the same pattern as
the race state store: detector memory is never advanced without the events it produced
being published, and never published twice. A concurrent writer makes ``commit`` raise
``DetectionConflictError``; the caller re-reads and retries.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from uuid import UUID

from pydantic import ValidationError
from redis.asyncio.client import Pipeline
from redis.exceptions import WatchError

from app.detection.config import DetectionConfig
from app.detection.errors import DetectionConflictError
from app.detection.models import CONTEXT_SCHEMA_VERSION, DetectionContext
from app.infrastructure.redis import RedisClient
from app.streaming.config import StreamConfig
from app.streaming.envelope import StreamEvent

logger = logging.getLogger(__name__)


def _decode(raw: str | bytes | None, replay_id: UUID) -> DetectionContext | None:
    """An unreadable or other-version document counts as missing (windows refill)."""
    if raw is None:
        return None
    try:
        context = DetectionContext.model_validate_json(raw)
    except ValidationError:
        logger.error("Stored detection context is unreadable; ignoring replay_id=%s", replay_id)
        return None
    if context.schema_version != CONTEXT_SCHEMA_VERSION:
        logger.warning(
            "Stored detection context has schema_version=%d (expected %d); ignoring replay_id=%s",
            context.schema_version,
            CONTEXT_SCHEMA_VERSION,
            replay_id,
        )
        return None
    return context


class DetectionTransaction:
    def __init__(self, store: DetectionContextStore, pipe: Pipeline, replay_id: UUID) -> None:
        self._store = store
        self._pipe = pipe
        self._replay_id = replay_id

    async def load(self) -> DetectionContext | None:
        raw = await self._pipe.get(self._store.key(self._replay_id))
        return _decode(raw, self._replay_id)

    async def commit(self, context: DetectionContext, events: Sequence[StreamEvent]) -> None:
        """Store ``context`` and publish ``events`` atomically, refreshing the TTL."""
        stream_config = self._store.stream_config
        self._pipe.multi()
        self._pipe.set(
            self._store.key(self._replay_id),
            context.model_dump_json(),
            ex=self._store.config.ttl_seconds,
        )
        for event in events:
            self._pipe.xadd(
                stream_config.detected_stream,
                event.to_fields(),
                maxlen=stream_config.maxlen or None,
                approximate=True,
            )
        try:
            await self._pipe.execute()
        except WatchError as exc:
            raise DetectionConflictError(
                f"Detection context of replay {self._replay_id} was changed by another worker"
            ) from exc


class DetectionContextStore:
    def __init__(
        self, redis: RedisClient, *, stream_config: StreamConfig, config: DetectionConfig
    ) -> None:
        self._redis = redis
        self.stream_config = stream_config
        self.config = config

    def key(self, replay_id: UUID) -> str:
        return f"{self.config.key_prefix}:{replay_id}:detection"

    async def get(self, replay_id: UUID) -> DetectionContext | None:
        return _decode(await self._redis.client.get(self.key(replay_id)), replay_id)

    @asynccontextmanager
    async def transaction(self, replay_id: UUID) -> AsyncIterator[DetectionTransaction]:
        async with self._redis.client.pipeline(transaction=True) as pipe:
            await pipe.watch(self.key(replay_id))
            yield DetectionTransaction(self, pipe, replay_id)
