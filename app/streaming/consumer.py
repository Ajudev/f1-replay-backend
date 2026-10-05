"""Reusable consumer-group infrastructure for Redis Streams.

Semantics (at-least-once):

- A message is acknowledged (XACK) only after the handler succeeded. If the handler
  raises, the message stays pending and is retried after it has been idle for
  ``reclaim_idle_ms`` (XAUTOCLAIM), by this or any other consumer of the group.
- Messages that are malformed, have an unsupported schema version, or exceed
  ``max_deliveries`` are moved to the dead-letter stream and acknowledged. For the
  delivery limit, the handler ran ``max_deliveries`` times and the dead-letter
  ``delivery_count`` is ``max_deliveries + 1``: the delivery that triggered it.
- An ``IdempotencyStore`` skips events the group already handled. A crash between
  the handler finishing and the event being marked processed causes a redelivery,
  so handlers must still be safe to run twice.
- One failing message never blocks later messages, and a consumer's failures never
  reach the publisher or other groups: each group has its own pending list.

The client must be created with ``decode_responses=True`` (as ``RedisClient`` does).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from redis.exceptions import RedisError, ResponseError

from app.infrastructure.redis import RedisClient
from app.streaming.config import StreamConfig
from app.streaming.dead_letter import DeadLetterQueue
from app.streaming.envelope import StreamEvent
from app.streaming.errors import NonRetryableEventError
from app.streaming.idempotency import IdempotencyStore

logger = logging.getLogger(__name__)

BACKLOG_LOG_INTERVAL_SECONDS = 60.0
ERROR_BACKOFF_SECONDS = 1.0
MAX_RECLAIM_PAGES = 100
MAX_TRACKED_ERRORS = 1000  # bound on remembered last-error texts per consumer

Entry = tuple[str, Mapping[str, str] | None]


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    """A decoded envelope together with its transport coordinates."""

    stream: str
    message_id: str  # Redis stream entry id, distinct from ``event.event_id``
    event: StreamEvent
    delivery_count: int


class EventHandler(Protocol):
    """Application logic for one consumer group. Must be idempotent."""

    async def handle(self, message: ReceivedMessage) -> None: ...


@dataclass(slots=True)
class ConsumerStats:
    processed: int = 0
    failed: int = 0
    retried: int = 0  # handled with delivery_count > 1
    reclaimed: int = 0  # claimed from the pending list via XAUTOCLAIM
    dead_lettered: int = 0
    duplicates_skipped: int = 0


def make_consumer_name(prefix: str) -> str:
    """Unique consumer name: ``{prefix}-{hostname}-{pid}-{short uuid}``."""
    return f"{prefix}-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


class StreamConsumer:
    """One consumer of a consumer group."""

    def __init__(
        self,
        redis: RedisClient,
        *,
        stream: str,
        group: str,
        consumer_name: str,
        handler: EventHandler,
        config: StreamConfig,
        idempotency_store: IdempotencyStore | None = None,
        dead_letter: DeadLetterQueue | None = None,
    ) -> None:
        self._redis = redis
        self.stream = stream
        self.group = group
        self.consumer_name = consumer_name
        self._handler = handler
        self._config = config
        self._idempotency = idempotency_store
        self._dead_letter = dead_letter or DeadLetterQueue(
            redis, config.dead_letter_stream, config.dead_letter_maxlen
        )
        self.stats = ConsumerStats()
        self._stopping = asyncio.Event()
        self._ready = False
        self._last_errors: OrderedDict[str, str] = OrderedDict()

    # -- setup / lifecycle ----------------------------------------------------------

    async def ensure_group(self) -> None:
        """Create the group (and the stream) if needed. Safe to call repeatedly."""
        try:
            await self._redis.client.xgroup_create(
                self.stream, self.group, id=self._config.group_start_id, mkstream=True
            )
            logger.info(
                "Consumer group created stream=%s group=%s start_id=%s",
                self.stream,
                self.group,
                self._config.group_start_id,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def stop(self) -> None:
        """Ask ``run`` to exit; it does so after the current read or batch."""
        self._stopping.set()

    async def run(self) -> None:
        """Consume until ``stop`` is called. Redis outages are retried with backoff."""
        logger.info(
            "Consumer starting stream=%s group=%s consumer=%s",
            self.stream,
            self.group,
            self.consumer_name,
        )
        last_reclaim = float("-inf")
        last_backlog_log = time.monotonic()
        reclaim_every = min(self._config.reclaim_idle_ms, self._config.block_ms) / 1000
        while not self._stopping.is_set():
            await asyncio.sleep(0)  # always yield, even if a read returns immediately
            try:
                if not self._ready:
                    await self.ensure_group()
                    await self.process_own_pending()
                    self._ready = True
                now = time.monotonic()
                do_reclaim = now - last_reclaim >= reclaim_every
                if do_reclaim:
                    last_reclaim = now
                await self.process_batch(reclaim=do_reclaim)
                if now - last_backlog_log >= BACKLOG_LOG_INTERVAL_SECONDS:
                    last_backlog_log = now
                    await self._log_backlog()
            except RedisError as exc:
                if isinstance(exc, ResponseError) and "NOGROUP" in str(exc):
                    self._ready = False  # group or stream was deleted; recreate
                logger.warning(
                    "Consumer Redis error group=%s consumer=%s: %s; retrying",
                    self.group,
                    self.consumer_name,
                    exc,
                )
                await self._sleep_unless_stopping(ERROR_BACKOFF_SECONDS)
        logger.info(
            "Consumer stopped group=%s consumer=%s processed=%d failed=%d dead_lettered=%d",
            self.group,
            self.consumer_name,
            self.stats.processed,
            self.stats.failed,
            self.stats.dead_lettered,
        )

    # -- single iteration (also used directly by tests) --------------------------------

    async def process_batch(self, *, reclaim: bool = True) -> int:
        """Reclaim stale pending messages, then read and handle new ones.

        Returns the number of messages looked at. Blocks up to ``block_ms`` waiting
        for new messages (no busy polling).
        """
        handled = 0
        if reclaim:
            handled += await self.reclaim_stale()
        block = self._config.block_ms or None
        response = await self._redis.client.xreadgroup(
            self.group,
            self.consumer_name,
            {self.stream: ">"},
            count=self._config.read_count,
            block=block,
        )
        for message_id, fields in _entries(response):
            await self._handle_entry(message_id, fields, delivery_count=1)
            handled += 1
        return handled

    async def process_own_pending(self) -> int:
        """One pass over this consumer's own pending list (safe restart, stable name).

        Messages that fail again stay pending; the reclaim path retries them later.
        """
        handled = 0
        cursor = "0"
        while True:
            response = await self._redis.client.xreadgroup(
                self.group,
                self.consumer_name,
                {self.stream: cursor},
                count=self._config.read_count,
            )
            entries = _entries(response)
            if not entries:
                return handled
            for message_id, fields in entries:
                count = await self._delivery_count(message_id)
                if count is not None:
                    await self._handle_entry(message_id, fields, delivery_count=count)
                    handled += 1
            cursor = entries[-1][0]

    async def reclaim_stale(self) -> int:
        """Claim messages idle for ``reclaim_idle_ms`` (XAUTOCLAIM) and handle them."""
        handled = 0
        cursor = "0-0"
        for _ in range(MAX_RECLAIM_PAGES):
            result = await self._redis.client.xautoclaim(
                self.stream,
                self.group,
                self.consumer_name,
                min_idle_time=self._config.reclaim_idle_ms,
                start_id=cursor,
                count=self._config.read_count,
            )
            cursor = str(result[0])
            claimed = result[1] if len(result) > 1 else []
            if len(result) > 2 and result[2]:
                logger.warning(
                    "Pending entries no longer in stream stream=%s group=%s ids=%s",
                    self.stream,
                    self.group,
                    result[2],
                )
            for message_id, fields in claimed:
                self.stats.reclaimed += 1
                count = await self._delivery_count(message_id)
                if count is None:
                    continue  # acknowledged by someone else in the meantime
                await self._handle_entry(message_id, fields, delivery_count=count)
                handled += 1
            if cursor == "0-0":
                break
        return handled

    # -- per-message processing ----------------------------------------------------------

    async def _handle_entry(
        self, message_id: str, fields: Mapping[str, str] | None, *, delivery_count: int
    ) -> None:
        if fields is None:
            # The entry was trimmed from the stream while still pending.
            logger.warning(
                "Pending message no longer exists, acknowledging group=%s message_id=%s",
                self.group,
                message_id,
            )
            await self._redis.client.xack(self.stream, self.group, message_id)
            return

        if delivery_count > self._config.max_deliveries:
            reason = self._last_errors.pop(message_id, None) or "Handler did not succeed"
            await self._dead_letter_message(
                message_id,
                fields,
                f"Delivery limit exceeded ({delivery_count} deliveries): {reason}",
                delivery_count,
            )
            return

        try:
            event = StreamEvent.from_fields(
                fields, supported_versions=self._config.supported_versions
            )
        except NonRetryableEventError as exc:
            await self._dead_letter_message(
                message_id, fields, f"{type(exc).__name__}: {exc}", delivery_count
            )
            return

        if self._idempotency is not None and await self._idempotency.is_processed(
            self.group, event.event_id
        ):
            await self._redis.client.xack(self.stream, self.group, message_id)
            self.stats.duplicates_skipped += 1
            logger.debug(
                "Duplicate event skipped group=%s event_id=%s message_id=%s",
                self.group,
                event.event_id,
                message_id,
            )
            return

        message = ReceivedMessage(self.stream, message_id, event, delivery_count)
        try:
            await self._handler.handle(message)
        except Exception as exc:
            self.stats.failed += 1
            self._last_errors[message_id] = f"{type(exc).__name__}: {exc}"[:300]
            self._last_errors.move_to_end(message_id)
            while len(self._last_errors) > MAX_TRACKED_ERRORS:
                self._last_errors.popitem(last=False)  # evict the oldest
            logger.warning(
                "Event handler failed group=%s consumer=%s event_id=%s message_id=%s deliveries=%d",
                self.group,
                self.consumer_name,
                event.event_id,
                message_id,
                delivery_count,
                exc_info=exc,
            )
            return  # left pending: retried after the idle timeout

        if self._idempotency is not None:
            await self._idempotency.mark_processed(self.group, event.event_id)
        await self._redis.client.xack(self.stream, self.group, message_id)
        self._last_errors.pop(message_id, None)
        self.stats.processed += 1
        if delivery_count > 1:
            self.stats.retried += 1

    async def _dead_letter_message(
        self,
        message_id: str,
        fields: Mapping[str, str],
        reason: str,
        delivery_count: int,
    ) -> None:
        await self._dead_letter.move(
            stream=self.stream,
            group=self.group,
            consumer=self.consumer_name,
            message_id=message_id,
            fields=fields,
            reason=reason,
            delivery_count=delivery_count,
        )
        self.stats.dead_lettered += 1
        logger.error(
            "Message dead-lettered group=%s consumer=%s event_id=%s message_id=%s "
            "deliveries=%d reason=%s",
            self.group,
            self.consumer_name,
            fields.get("event_id"),
            message_id,
            delivery_count,
            reason[:200],
        )

    async def _delivery_count(self, message_id: str) -> int | None:
        rows = await self._redis.client.xpending_range(
            self.stream, self.group, min=message_id, max=message_id, count=1
        )
        if not rows:
            return None
        return int(rows[0]["times_delivered"])

    async def _log_backlog(self) -> None:
        groups = await self._redis.client.xinfo_groups(self.stream)
        for info in groups:
            if info.get("name") == self.group and (info.get("pending") or info.get("lag")):
                logger.info(
                    "Consumer backlog stream=%s group=%s pending=%s lag=%s",
                    self.stream,
                    self.group,
                    info.get("pending"),
                    info.get("lag"),
                )

    async def _sleep_unless_stopping(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)


def _entries(response: Any) -> list[Entry]:
    """Flatten an XREADGROUP reply for a single stream into ``(id, fields)`` pairs."""
    if not response:
        return []
    entries: list[Entry] = []
    for _stream, messages in response:
        entries.extend((message_id, fields) for message_id, fields in messages)
    return entries
