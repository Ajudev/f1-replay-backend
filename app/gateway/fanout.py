"""Tail of ``race.state.events`` and ``race.detected.events`` for WebSocket delivery.

Deliberately **not** a consumer group: a group splits messages among its consumers,
while every gateway process must see every message to serve its own clients. Each
process runs one plain ``XREAD`` loop from the streams' tail at startup (no acks, no
pending entries, nothing persisted). A message published while a process is down or
before it started is not replayed to browsers; clients recover through the snapshot.

Both streams are read in one ``XREAD`` and each batch is merged by Redis entry id
(``<ms>-<seq>``, comparable across streams on one server), so messages are handed on
in publish order. One task per process means delivery order is preserved per replay.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from app.gateway.config import GatewayConfig
from app.infrastructure.redis import RedisClient
from app.streaming.config import StreamConfig
from app.streaming.envelope import StreamEvent
from app.streaming.errors import NonRetryableEventError

logger = logging.getLogger(__name__)

#: ``(stream name, event)`` handler; called in publish order, one at a time.
EventHandler = Callable[[str, StreamEvent], Awaitable[None]]


def _entry_key(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)


class StreamTail:
    def __init__(
        self,
        redis: RedisClient,
        stream_config: StreamConfig,
        config: GatewayConfig,
        handler: EventHandler,
    ) -> None:
        self._redis = redis
        self._streams = (stream_config.state_stream, stream_config.detected_stream)
        self._supported = stream_config.supported_versions
        self._config = config
        self._handler = handler
        self._last_ids: dict[str, str] | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="ws-stream-tail")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        failing = False
        while not self._stopping:
            try:
                if self._last_ids is None:
                    self._last_ids = await self._tail_ids()
                await self.read_once()
                if failing:
                    logger.info("WebSocket stream tail recovered")
                    failing = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # delivery must outlive any single failure
                if not failing:
                    logger.warning(
                        "WebSocket stream tail failing (%s): %s", type(exc).__name__, exc
                    )
                    failing = True
                await asyncio.sleep(self._config.retry_backoff_seconds)

    async def _tail_ids(self) -> dict[str, str]:
        """Last existing entry id of each stream (``0-0`` if empty): start after it."""
        ids: dict[str, str] = {}
        for stream in self._streams:
            last = await self._redis.client.xrevrange(stream, count=1)
            ids[stream] = last[0][0] if last else "0-0"
        return ids

    async def read_once(self) -> int:
        """One ``XREAD`` round; returns the number of entries handed on.

        When a stream returned a full page, entries newer than that page's last id are held
        back (cursor not advanced) and read again, so cross-stream publish order holds.
        """
        if self._last_ids is None:
            self._last_ids = await self._tail_ids()
        response: Any = await self._redis.client.xread(
            dict(self._last_ids),
            count=self._config.stream_read_count,
            block=self._config.stream_block_ms,
        )
        if not response:
            return 0
        entries: list[tuple[str, str, dict[str, str]]] = []
        cutoff: tuple[int, int] | None = None
        for stream, items in response:
            for entry_id, fields in items:
                entries.append((stream, entry_id, fields))
            if items and len(items) >= self._config.stream_read_count:
                # COUNT applies per stream: a full page may hide older entries behind
                # its last id, so nothing newer than that may be handed on yet.
                last = _entry_key(items[-1][0])
                cutoff = last if cutoff is None else min(cutoff, last)
        entries.sort(key=lambda e: _entry_key(e[1]))
        if cutoff is not None:
            entries = [e for e in entries if _entry_key(e[1]) <= cutoff]
        for stream, entry_id, _ in entries:
            self._last_ids[stream] = entry_id  # held-back entries are re-read next round
        for stream, entry_id, fields in entries:
            try:
                event = StreamEvent.from_fields(fields, supported_versions=self._supported)
            except NonRetryableEventError as exc:
                logger.warning(
                    "Skipping undecodable entry stream=%s id=%s: %s", stream, entry_id, exc
                )
                continue
            try:
                await self._handler(stream, event)
            except Exception:
                logger.exception(
                    "WebSocket delivery failed stream=%s id=%s replay_id=%s seq=%d",
                    stream,
                    entry_id,
                    event.replay_id,
                    event.sequence,
                )
        return len(entries)
