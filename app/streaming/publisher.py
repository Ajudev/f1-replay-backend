"""Replay event sink that publishes into a Redis Stream.

Producers and consumers are decoupled: the publisher only appends to the stream
and never waits for, or learns about, consumers. A single shared raw stream carries
all replays (events are separated by ``replay_id``/``run_id`` metadata). Per-replay
streams would need dynamic group management and cleanup, and consumers would have
to discover them; one stream keeps consumer groups static.

Delivery is at-least-once. If an XADD succeeds but its reply is lost, the retry may
append the same event twice. Both entries carry the same deterministic ``event_id``,
so consumers deduplicate on it (see ``idempotency``).

Failure behaviour: only connection/timeout errors are retried (bounded, short
backoff). When attempts are exhausted a ``StreamPublishError`` is raised, which
fails the replay; the runner does not advance past an event that was not published,
so nothing is silently dropped.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.infrastructure.redis import RedisClient
from app.replay.events import ReplayEvent
from app.streaming.config import StreamConfig
from app.streaming.envelope import StreamEvent
from app.streaming.errors import StreamPublishError

logger = logging.getLogger(__name__)

_RETRYABLE = (RedisConnectionError, RedisTimeoutError)


@dataclass(slots=True)
class PublisherStats:
    """Simple counters, a seed for future metrics."""

    published: int = 0
    publish_failures: int = 0
    publish_retries: int = 0


class RedisStreamPublisher:
    """``ReplayEventSink`` backed by a Redis Stream (XADD)."""

    def __init__(self, redis: RedisClient, config: StreamConfig) -> None:
        self._redis = redis
        self._config = config
        self.stats = PublisherStats()

    async def publish(self, event: ReplayEvent) -> None:
        await self.publish_event(event)

    async def publish_event(self, event: ReplayEvent) -> str:
        """Append the event to the raw stream and return its Redis message id."""
        config = self._config
        envelope = StreamEvent.from_replay_event(event, published_at=datetime.now(UTC))
        # Encoded once: every retry sends identical bytes (same event_id/published_at).
        fields = envelope.to_fields()
        trim = {"maxlen": config.maxlen, "approximate": True} if config.maxlen > 0 else {}

        attempt = 0
        while True:
            attempt += 1
            try:
                message_id: str = await self._redis.client.xadd(config.raw_stream, fields, **trim)
            except _RETRYABLE as exc:
                if attempt >= config.publish_attempts:
                    self.stats.publish_failures += 1
                    logger.error(
                        "Event publish failed replay_id=%s event_id=%s sequence=%d "
                        "stream=%s attempts=%d",
                        event.replay_id,
                        envelope.event_id,
                        event.sequence,
                        config.raw_stream,
                        attempt,
                        exc_info=exc,
                    )
                    raise self._error("Redis unavailable while publishing", event) from exc
                self.stats.publish_retries += 1
                logger.warning(
                    "Event publish retrying replay_id=%s event_id=%s sequence=%d attempt=%d",
                    event.replay_id,
                    envelope.event_id,
                    event.sequence,
                    attempt,
                )
                await asyncio.sleep(config.publish_backoff_seconds * attempt)
            except (RedisError, RuntimeError) as exc:
                self.stats.publish_failures += 1
                logger.error(
                    "Event publish rejected replay_id=%s event_id=%s sequence=%d stream=%s",
                    event.replay_id,
                    envelope.event_id,
                    event.sequence,
                    config.raw_stream,
                    exc_info=exc,
                )
                raise self._error(f"Publish failed: {type(exc).__name__}", event) from exc
            else:
                self.stats.published += 1
                logger.debug(
                    "Event published replay_id=%s event_id=%s type=%s sequence=%d "
                    "stream=%s message_id=%s",
                    event.replay_id,
                    envelope.event_id,
                    envelope.event_type,
                    event.sequence,
                    config.raw_stream,
                    message_id,
                )
                return message_id

    def _error(self, message: str, event: ReplayEvent) -> StreamPublishError:
        return StreamPublishError(
            message,
            stream=self._config.raw_stream,
            replay_id=event.replay_id,
            sequence=event.sequence,
        )
