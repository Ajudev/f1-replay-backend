"""RedisStreamPublisher behaviour against fakeredis."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

import fakeredis
import fakeredis.aioredis
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from app.infrastructure.redis import RedisClient
from app.streaming.config import StreamConfig
from app.streaming.envelope import STREAM_EVENT_SCHEMA_VERSION, StreamEvent, derive_event_id
from app.streaming.errors import StreamPublishError, StreamSerializationError
from app.streaming.publisher import RedisStreamPublisher
from tests.streaming.conftest import make_event, raw


class FlakyRedis(fakeredis.aioredis.FakeRedis):
    """XADD fails with ``error`` for the first ``failures`` calls (``None`` = always)."""

    def __init__(self, *, failures: int | None, error: Exception, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.failures = failures
        self.error = error
        self.calls = 0

    async def xadd(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if self.failures is None or self.calls <= self.failures:
            raise self.error
        return await super().xadd(*args, **kwargs)


def flaky(failures: int | None, error: Exception | None = None) -> tuple[RedisClient, FlakyRedis]:
    redis = FlakyRedis(
        failures=failures,
        error=error or RedisConnectionError("connection refused"),
        server=fakeredis.FakeServer(),
        decode_responses=True,
    )
    return RedisClient.from_client(redis), redis


async def test_publish_writes_envelope_to_raw_stream(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    event = make_event(4)
    message_id = await publisher.publish_event(event)

    rows = await raw(redis_client).xrange(config.raw_stream)
    assert len(rows) == 1
    stored_id, fields = rows[0]
    assert stored_id == message_id
    assert fields["event_id"] != message_id  # Redis id is not the event id
    assert fields["sequence"] == "4"
    assert fields["run_id"] == str(event.run_id)
    assert fields["replay_id"] == str(event.replay_id)
    assert int(fields["schema_version"]) == STREAM_EVENT_SCHEMA_VERSION
    envelope = StreamEvent.from_fields(fields)
    assert envelope.event_id == derive_event_id(event.run_id, 4)
    assert envelope.payload == event.payload
    assert envelope.driver_abbreviation == "NOR"
    assert json.loads(fields["data"])["published_at"].endswith("+00:00")
    assert publisher.stats.published == 1


async def test_publish_satisfies_sink_protocol(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    await RedisStreamPublisher(redis_client, config).publish(make_event(0))
    assert await raw(redis_client).xlen(config.raw_stream) == 1


async def test_maxlen_trims_stream(redis_client: RedisClient, config: StreamConfig) -> None:
    maxlen, total = 100, 1000
    publisher = RedisStreamPublisher(redis_client, replace(config, maxlen=maxlen))
    for i in range(total):
        await publisher.publish_event(make_event(i))
    # `MAXLEN ~` trims in whole radix-tree nodes on real Redis (it may keep more than
    # maxlen) but never below it; fakeredis trims exactly.
    length = await raw(redis_client).xlen(config.raw_stream)
    assert maxlen <= length < total
    rows = await raw(redis_client).xrevrange(config.raw_stream, count=1)
    assert rows[0][1]["sequence"] == str(total - 1)  # newest entry is always kept


async def test_unserializable_payload_raises_and_publishes_nothing(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    with pytest.raises(StreamSerializationError):
        await publisher.publish_event(make_event(0, payload={"x": object()}))
    assert await raw(redis_client).xlen(config.raw_stream) == 0


async def test_transient_connection_error_is_retried(config: StreamConfig) -> None:
    client, redis = flaky(failures=2)
    publisher = RedisStreamPublisher(client, config)
    message_id = await publisher.publish_event(make_event(0))
    assert redis.calls == 3 and message_id
    assert publisher.stats.published == 1 and publisher.stats.publish_retries == 2
    assert await redis.xlen(config.raw_stream) == 1


async def test_exhausted_retries_raise_publish_error(config: StreamConfig) -> None:
    client, redis = flaky(failures=None)
    publisher = RedisStreamPublisher(client, config)
    event = make_event(9)
    with pytest.raises(StreamPublishError) as info:
        await publisher.publish(event)
    assert redis.calls == config.publish_attempts
    assert info.value.stream == config.raw_stream
    assert info.value.replay_id == event.replay_id
    assert info.value.sequence == 9
    assert publisher.stats.publish_failures == 1 and publisher.stats.published == 0


async def test_non_connection_errors_are_not_retried(config: StreamConfig) -> None:
    client, redis = flaky(failures=None, error=ResponseError("OOM command not allowed"))
    with pytest.raises(StreamPublishError):
        await RedisStreamPublisher(client, config).publish(make_event(0))
    assert redis.calls == 1


async def test_closed_client_raises_publish_error(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    await redis_client.aclose()
    with pytest.raises(StreamPublishError):
        await publisher.publish(make_event(0))


async def test_retry_publishes_same_event_id(config: StreamConfig) -> None:
    """A lost reply followed by a retry may duplicate the entry, never change its id."""
    client, redis = flaky(failures=0)
    publisher = RedisStreamPublisher(client, config)
    event = make_event(1)
    await publisher.publish(event)
    await publisher.publish(event)  # models the duplicate append
    ids = {f["event_id"] for _, f in await redis.xrange(config.raw_stream)}
    assert len(ids) == 1


async def test_many_events_keep_order_and_sequences(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    for i in range(1000):
        await publisher.publish(make_event(i))
    rows = await raw(redis_client).xrange(config.raw_stream)
    assert [int(f["sequence"]) for _, f in rows] == list(range(1000))
    message_ids = [i for i, _ in rows]
    assert message_ids == sorted(message_ids, key=lambda m: tuple(map(int, m.split("-"))))


async def test_interleaved_replays_stay_separated(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    a, b = UUID(int=10), UUID(int=20)
    run_a, run_b = uuid4(), uuid4()
    for i in range(5):
        await publisher.publish(make_event(i, replay_id=a, run_id=run_a))
        await publisher.publish(make_event(i, replay_id=b, run_id=run_b))
    events = [
        StreamEvent.from_fields(f) for _, f in await raw(redis_client).xrange(config.raw_stream)
    ]
    for replay, run in ((a, run_a), (b, run_b)):
        own = [e for e in events if e.replay_id == replay]
        assert [e.sequence for e in own] == list(range(5))
        assert {e.run_id for e in own} == {run}
    assert len({e.event_id for e in events}) == 10


async def test_restart_uses_new_run_id_and_new_event_ids(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    replay = UUID(int=5)
    first_run, second_run = uuid4(), uuid4()
    for run in (first_run, second_run):
        for i in range(3):
            await publisher.publish(make_event(i, replay_id=replay, run_id=run))
    events = [
        StreamEvent.from_fields(f) for _, f in await raw(redis_client).xrange(config.raw_stream)
    ]
    by_run = {
        run: [e.event_id for e in events if e.run_id == run] for run in (first_run, second_run)
    }
    assert not set(by_run[first_run]) & set(by_run[second_run])
    assert [e.sequence for e in events] == [0, 1, 2, 0, 1, 2]
