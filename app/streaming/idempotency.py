"""Processed-event tracking so redelivered messages are not handled twice.

This gives at-least-once delivery plus best-effort deduplication, never
exactly-once: if a consumer crashes after the handler finished but before the event
is marked processed, the message is redelivered and the handler runs again.
Handlers must therefore still be idempotent.
"""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from app.infrastructure.redis import RedisClient


class IdempotencyStore(Protocol):
    """Remembers which events a consumer group has already handled."""

    async def is_processed(self, group: str, event_id: UUID) -> bool: ...

    async def mark_processed(self, group: str, event_id: UUID) -> None: ...


class RedisIdempotencyStore:
    """Redis-backed store: ``SET NX EX`` on ``stream:processed:{group}:{event_id}``."""

    def __init__(self, redis: RedisClient, ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds

    @staticmethod
    def key(group: str, event_id: UUID) -> str:
        return f"stream:processed:{group}:{event_id}"

    async def is_processed(self, group: str, event_id: UUID) -> bool:
        return bool(await self._redis.client.exists(self.key(group, event_id)))

    async def mark_processed(self, group: str, event_id: UUID) -> None:
        await self._redis.client.set(self.key(group, event_id), "1", nx=True, ex=self._ttl)


class InMemoryIdempotencyStore:
    """Process-local store for tests."""

    def __init__(self) -> None:
        self._seen: set[tuple[str, UUID]] = set()

    async def is_processed(self, group: str, event_id: UUID) -> bool:
        return (group, event_id) in self._seen

    async def mark_processed(self, group: str, event_id: UUID) -> None:
        self._seen.add((group, event_id))
