"""Fakeredis-backed fixtures with isolated stream and group names."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import fakeredis
import fakeredis.aioredis
import pytest
from redis.asyncio import Redis

from app.domain.enums import EventType
from app.infrastructure.redis import RedisClient
from app.replay.events import ReplayEvent
from app.streaming.config import StreamConfig
from app.streaming.consumer import ReceivedMessage, StreamConsumer
from app.streaming.idempotency import IdempotencyStore
from tests.replay.conftest import replay_env  # noqa: F401  (fixture for integration tests)


@pytest.fixture
async def redis_client() -> AsyncIterator[RedisClient]:
    """Fakeredis by default; a real Redis only when REDIS_TEST_URL is set.

    The real Redis is never the application's configured one. Every test uses
    uuid-suffixed stream names and deletes them afterwards.
    """
    url = os.environ.get("REDIS_TEST_URL")
    if url:
        client = RedisClient(url)
    else:
        server = fakeredis.FakeServer()
        client = RedisClient.from_client(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
        )
    yield client
    await client.aclose()


@pytest.fixture
def config() -> StreamConfig:
    suffix = uuid4().hex[:8]
    return StreamConfig(
        raw_stream=f"test.raw.{suffix}",
        state_stream=f"test.state.{suffix}",
        detected_stream=f"test.detected.{suffix}",
        dead_letter_stream=f"test.dead.{suffix}",
        maxlen=0,
        block_ms=10,
        reclaim_idle_ms=0,
        max_deliveries=3,
        publish_attempts=3,
        publish_backoff_seconds=0.0,
        group_start_id="0",
    )


@pytest.fixture(autouse=True)
async def cleanup_streams(redis_client: RedisClient, config: StreamConfig) -> AsyncIterator[None]:
    yield
    if redis_client.is_closed:
        return
    client = redis_client.client
    await client.delete(
        config.raw_stream, config.state_stream, config.detected_stream, config.dead_letter_stream
    )
    # Idempotency markers are keyed by group + event id, not by stream. The test
    # Redis is disposable by contract (REDIS_TEST_URL), so remove them all.
    keys = [key async for key in client.scan_iter(match="stream:processed:*", count=500)]
    if keys:
        await client.delete(*keys)


def make_event(
    sequence: int,
    *,
    replay_id: UUID | None = None,
    run_id: UUID | None = None,
    payload: dict[str, Any] | None = None,
    event_type: EventType = EventType.LAP_COMPLETED,
) -> ReplayEvent:
    return ReplayEvent(
        replay_id=replay_id or UUID(int=1),
        run_id=run_id or UUID(int=2),
        session_id=UUID(int=3),
        sequence=sequence,
        event_type=event_type,
        race_time_ms=sequence * 1000,
        lap_number=sequence // 10 + 1,
        driver_id=UUID(int=4),
        driver_abbreviation="NOR",
        payload=payload if payload is not None else {"lap_time_ms": 90_000 + sequence},
    )


@dataclass
class RecordingHandler:
    """Records handled messages; raises for sequences in ``fail_for`` (``None`` = always)."""

    handled: list[ReceivedMessage] = field(default_factory=list)
    fail_for: set[int] | None = field(default_factory=set)
    fail_times: dict[int, int] = field(default_factory=dict)  # sequence -> remaining failures

    async def handle(self, message: ReceivedMessage) -> None:
        sequence = message.event.sequence
        if self.fail_times.get(sequence, 0) > 0:
            self.fail_times[sequence] -= 1
            raise RuntimeError(f"transient failure at {sequence}")
        if self.fail_for is None or sequence in self.fail_for:
            raise RuntimeError(f"permanent failure at {sequence}")
        self.handled.append(message)

    @property
    def sequences(self) -> list[int]:
        return [m.event.sequence for m in self.handled]


GROUP = "test-group"


def make_consumer(
    redis: RedisClient,
    config: StreamConfig,
    handler: RecordingHandler,
    *,
    group: str = GROUP,
    name: str = "consumer-1",
    store: IdempotencyStore | None = None,
) -> StreamConsumer:
    return StreamConsumer(
        redis,
        stream=config.raw_stream,
        group=group,
        consumer_name=name,
        handler=handler,
        config=config,
        idempotency_store=store,
    )


def raw(client: RedisClient) -> Redis:
    return client.client
