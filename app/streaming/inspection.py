"""Read-only stream inspection for debugging (used by the CLI, not exposed over HTTP)."""

from __future__ import annotations

from dataclasses import dataclass

from app.infrastructure.redis import RedisClient
from app.streaming.dead_letter import DeadLetter, DeadLetterQueue
from app.streaming.envelope import StreamEvent
from app.streaming.errors import StreamError


@dataclass(frozen=True, slots=True)
class StreamInfo:
    stream: str
    length: int
    first_id: str | None
    last_id: str | None


@dataclass(frozen=True, slots=True)
class GroupInfo:
    name: str
    consumers: int
    pending: int
    last_delivered_id: str
    lag: int | None  # entries not yet delivered to the group; None when Redis cannot tell


@dataclass(frozen=True, slots=True)
class PendingSummary:
    group: str
    pending: int
    min_id: str | None
    max_id: str | None
    per_consumer: dict[str, int]


@dataclass(frozen=True, slots=True)
class PendingEntry:
    message_id: str
    consumer: str
    idle_ms: int
    delivery_count: int


@dataclass(frozen=True, slots=True)
class TailEntry:
    message_id: str
    event: StreamEvent | None
    error: str | None  # set when the entry could not be decoded


class StreamInspector:
    def __init__(self, redis: RedisClient, dead_letter_stream: str, dead_letter_maxlen: int = 0):
        self._redis = redis
        self._dead_letters = DeadLetterQueue(redis, dead_letter_stream, dead_letter_maxlen)

    async def stream_info(self, stream: str) -> StreamInfo:
        client = self._redis.client
        length = await client.xlen(stream)
        first = await client.xrange(stream, count=1)
        last = await client.xrevrange(stream, count=1)
        return StreamInfo(
            stream=stream,
            length=length,
            first_id=first[0][0] if first else None,
            last_id=last[0][0] if last else None,
        )

    async def groups(self, stream: str) -> list[GroupInfo]:
        if not await self._redis.client.exists(stream):
            return []
        rows = await self._redis.client.xinfo_groups(stream)
        return [
            GroupInfo(
                name=row["name"],
                consumers=int(row["consumers"]),
                pending=int(row["pending"]),
                last_delivered_id=row["last-delivered-id"],
                lag=None if row.get("lag") is None else int(row["lag"]),
            )
            for row in rows
        ]

    async def pending_summary(self, stream: str, group: str) -> PendingSummary:
        summary = await self._redis.client.xpending(stream, group)
        return PendingSummary(
            group=group,
            pending=int(summary["pending"]),
            min_id=summary.get("min"),
            max_id=summary.get("max"),
            per_consumer={c["name"]: int(c["pending"]) for c in summary.get("consumers") or []},
        )

    async def pending_entries(self, stream: str, group: str, count: int = 20) -> list[PendingEntry]:
        rows = await self._redis.client.xpending_range(stream, group, min="-", max="+", count=count)
        return [
            PendingEntry(
                message_id=row["message_id"],
                consumer=row["consumer"],
                idle_ms=int(row["time_since_delivered"]),
                delivery_count=int(row["times_delivered"]),
            )
            for row in rows
        ]

    async def tail(
        self,
        stream: str,
        count: int = 10,
        supported_versions: frozenset[int] | None = None,
    ) -> list[TailEntry]:
        """The most recent ``count`` entries, oldest first, decoded where possible."""
        rows = await self._redis.client.xrevrange(stream, count=count)
        entries: list[TailEntry] = []
        for message_id, fields in reversed(rows):
            try:
                event = (
                    StreamEvent.from_fields(fields, supported_versions=supported_versions)
                    if supported_versions
                    else StreamEvent.from_fields(fields)
                )
                entries.append(TailEntry(message_id, event, None))
            except StreamError as exc:
                entries.append(TailEntry(message_id, None, f"{type(exc).__name__}: {exc}"))
        return entries

    async def dead_letters(self, count: int = 20) -> list[DeadLetter]:
        return await self._dead_letters.recent(count)
