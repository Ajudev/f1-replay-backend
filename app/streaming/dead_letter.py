"""Dead-letter stream for messages that cannot be processed.

A message is dead-lettered when it is malformed, declares an unsupported schema
version, or exceeds the maximum number of deliveries. The dead-letter entry and the
XACK of the original are written in one MULTI/EXEC transaction, so the message is
neither lost nor left pending after being parked.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from app.infrastructure.redis import RedisClient

MAX_REASON_LENGTH = 500


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A dead-letter stream entry."""

    dead_letter_id: str
    original_stream: str
    original_message_id: str
    group: str
    consumer: str
    reason: str
    delivery_count: int
    failed_at: str
    data: str
    event_id: str | None


class DeadLetterQueue:
    def __init__(self, redis: RedisClient, stream: str, maxlen: int) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen

    async def move(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        message_id: str,
        fields: Mapping[str, str] | None,
        reason: str,
        delivery_count: int,
    ) -> str:
        """Park a message and acknowledge the original atomically; returns the entry id."""
        original = fields or {}
        entry = {
            # Raw payload as received, even if it is not a valid envelope.
            "data": original.get("data", ""),
            "original_stream": stream,
            "original_message_id": message_id,
            "group": group,
            "consumer": consumer,
            "reason": reason[:MAX_REASON_LENGTH],
            "delivery_count": str(delivery_count),
            "failed_at": datetime.now(UTC).isoformat(),
        }
        if "event_id" in original:
            entry["event_id"] = original["event_id"]
        trim = {"maxlen": self._maxlen, "approximate": True} if self._maxlen > 0 else {}
        async with self._redis.client.pipeline(transaction=True) as pipe:
            pipe.xadd(self._stream, entry, **trim)
            pipe.xack(stream, group, message_id)
            results = await pipe.execute()
        return str(results[0])

    async def recent(self, count: int = 50) -> list[DeadLetter]:
        """Most recent dead letters, newest first."""
        rows = await self._redis.client.xrevrange(self._stream, count=count)
        return [
            DeadLetter(
                dead_letter_id=entry_id,
                original_stream=f.get("original_stream", ""),
                original_message_id=f.get("original_message_id", ""),
                group=f.get("group", ""),
                consumer=f.get("consumer", ""),
                reason=f.get("reason", ""),
                delivery_count=int(f.get("delivery_count", 0)),
                failed_at=f.get("failed_at", ""),
                data=f.get("data", ""),
                event_id=f.get("event_id"),
            )
            for entry_id, f in rows
        ]
