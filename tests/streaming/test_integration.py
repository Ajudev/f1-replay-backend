"""Replay service -> RedisStreamPublisher -> raw stream -> consumer group."""

from __future__ import annotations

import io
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from typing import Any

import fakeredis
import fakeredis.aioredis
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import ReplayStatus
from app.infrastructure.redis import RedisClient
from app.replay.events import ReplayEvent
from app.replay.service import ReplayService
from app.streaming.audit import AuditHandler
from app.streaming.cli import build_parser, run_command
from app.streaming.config import GROUP_RAW_EVENT_AUDITORS, StreamConfig
from app.streaming.consumer import StreamConsumer
from app.streaming.envelope import StreamEvent
from app.streaming.idempotency import InMemoryIdempotencyStore
from app.streaming.inspection import StreamInspector
from app.streaming.publisher import RedisStreamPublisher
from tests.replay.conftest import ReplayEnv
from tests.replay.fakes import ManualTimer, wait_until_idle
from tests.replay.test_service import persisted
from tests.streaming.conftest import raw


@dataclass
class StreamEnv:
    service: ReplayService
    timer: ManualTimer
    publisher: RedisStreamPublisher
    event_count: int


@pytest.fixture
async def stream_env(
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,
    config: StreamConfig,
) -> AsyncIterator[StreamEnv]:
    timer = ManualTimer()
    publisher = RedisStreamPublisher(redis_client, config)
    service = ReplayService(session_factory, sink=publisher, timer=timer, stop_timeout=0.5)
    yield StreamEnv(service, timer, publisher, replay_env.event_count)
    await service.shutdown()


async def test_replay_events_flow_through_stream_to_consumer_group(
    stream_env: StreamEnv,
    replay_env: ReplayEnv,
    redis_client: RedisClient,
    config: StreamConfig,
) -> None:
    audit = AuditHandler()
    consumer = StreamConsumer(
        redis_client,
        stream=config.raw_stream,
        group=GROUP_RAW_EVENT_AUDITORS,
        consumer_name="auditor-1",
        handler=audit,
        config=config,
        idempotency_store=InMemoryIdempotencyStore(),
    )
    await consumer.ensure_group()

    replay_id = (await stream_env.service.create(replay_env.race.session_id, 20)).state.replay_id
    await stream_env.service.start(replay_id)
    await stream_env.timer.advance(10_000)
    await wait_until_idle(stream_env.service)

    while audit.total < stream_env.event_count:
        await consumer.process_batch()

    assert audit.total == stream_env.event_count
    assert [r.sequence for r in audit.records] == list(range(stream_env.event_count))
    assert {r.replay_id for r in audit.records} == {replay_id}
    assert len({r.run_id for r in audit.records}) == 1
    summary = await StreamInspector(redis_client, config.dead_letter_stream).pending_summary(
        config.raw_stream, GROUP_RAW_EVENT_AUDITORS
    )
    assert summary.pending == 0
    assert stream_env.publisher.stats.published == stream_env.event_count


async def test_restart_publishes_under_new_run_id(
    stream_env: StreamEnv, replay_env: ReplayEnv, redis_client: RedisClient, config: StreamConfig
) -> None:
    replay_id = (await stream_env.service.create(replay_env.race.session_id, 20)).state.replay_id
    await stream_env.service.start(replay_id)
    await stream_env.timer.advance(10_000)
    await wait_until_idle(stream_env.service)
    await stream_env.service.restart(replay_id)
    await stream_env.timer.advance(10_000)
    await wait_until_idle(stream_env.service)

    events = [
        StreamEvent.from_fields(f) for _, f in await raw(redis_client).xrange(config.raw_stream)
    ]
    assert len(events) == 2 * stream_env.event_count
    first, second = events[: stream_env.event_count], events[stream_env.event_count :]
    assert len({e.run_id for e in first}) == len({e.run_id for e in second}) == 1
    assert first[0].run_id != second[0].run_id
    assert [e.sequence for e in first] == [e.sequence for e in second]
    assert not {e.event_id for e in first} & {e.event_id for e in second}


class OutageRedis(fakeredis.aioredis.FakeRedis):
    """XADD works until sequence ``fail_at`` is reached, then the connection drops."""

    def __init__(self, fail_at: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fail_at = fail_at

    async def xadd(self, name: str, fields: dict[str, str], *args: Any, **kwargs: Any) -> Any:
        if int(fields["sequence"]) >= self.fail_at:
            raise RedisConnectionError("connection lost")
        return await super().xadd(name, fields, *args, **kwargs)


async def test_redis_outage_fails_replay_and_event_is_not_counted(
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
    config: StreamConfig,
) -> None:
    client = RedisClient.from_client(
        OutageRedis(3, server=fakeredis.FakeServer(), decode_responses=True)
    )
    timer = ManualTimer()
    publisher = RedisStreamPublisher(client, config)
    service = ReplayService(session_factory, sink=publisher, timer=timer, stop_timeout=0.5)
    try:
        replay_id = (await service.create(replay_env.race.session_id, 20)).state.replay_id
        await service.start(replay_id)
        await timer.advance(10_000)
        await wait_until_idle(service)

        row = await persisted(session_factory, replay_id)
        assert row.status is ReplayStatus.FAILED
        assert row.current_sequence == 2  # event 3 was never published nor counted
        assert row.status_reason and "StreamPublishError" in row.status_reason
        assert publisher.stats.published == 3
        assert publisher.stats.publish_failures == 1
        sequences = [int(f["sequence"]) for _, f in await client.client.xrange(config.raw_stream)]
        assert sequences == [0, 1, 2]
    finally:
        await service.shutdown()
        await client.aclose()


async def test_cli_commands_report_stream_state(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    publisher = RedisStreamPublisher(redis_client, config)
    from tests.streaming.conftest import make_event

    for i in range(3):
        await publisher.publish(make_event(i))
    consumer = StreamConsumer(
        redis_client,
        stream=config.raw_stream,
        group=GROUP_RAW_EVENT_AUDITORS,
        consumer_name="c",
        handler=AuditHandler(),
        config=config,
    )
    await consumer.ensure_group()

    out = io.StringIO()
    await run_command(build_parser().parse_args(["info"]), redis_client, config, out)
    text = out.getvalue()
    assert config.raw_stream in text and GROUP_RAW_EVENT_AUDITORS in text

    out = io.StringIO()
    args = build_parser().parse_args(["tail", "--count", "2"])
    await run_command(args, redis_client, config, out)
    assert out.getvalue().count('"sequence"') == 2

    out = io.StringIO()
    args = build_parser().parse_args(["pending", "--group", GROUP_RAW_EVENT_AUDITORS])
    await run_command(args, redis_client, config, out)
    assert '"pending": 0' in out.getvalue()

    out = io.StringIO()
    await run_command(build_parser().parse_args(["dead-letters"]), redis_client, config, out)
    assert out.getvalue().strip() == "[]"


async def test_inspector_groups_report_lag_and_pending(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    from tests.streaming.conftest import RecordingHandler, make_consumer, make_event

    consumer = make_consumer(redis_client, config, RecordingHandler(fail_for=None))
    await consumer.ensure_group()
    publisher = RedisStreamPublisher(redis_client, config)
    for i in range(4):
        await publisher.publish(make_event(i))
    inspector = StreamInspector(redis_client, config.dead_letter_stream)
    (group,) = await inspector.groups(config.raw_stream)
    assert (
        group.pending == 0 and group.lag is not None and group.lag >= 3
    )  # fakeredis may undercount by one

    await consumer.process_batch(reclaim=False)
    (group,) = await inspector.groups(config.raw_stream)
    assert group.pending == 4
    info = await inspector.stream_info(config.raw_stream)
    assert info.length == 4 and info.first_id and info.last_id
    entries = await inspector.pending_entries(config.raw_stream, "test-group")
    assert [e.delivery_count for e in entries] == [1, 1, 1, 1]
    assert await inspector.groups("does.not.exist") == []


def test_stream_config_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.core.config import get_settings

    monkeypatch.setenv("STREAM_RAW_EVENTS", "custom.raw")
    monkeypatch.setenv("STREAM_MAXLEN", "0")
    monkeypatch.setenv("STREAM_MAX_DELIVERIES", "9")
    get_settings.cache_clear()
    cfg = StreamConfig.from_settings(get_settings())
    assert cfg.raw_stream == "custom.raw" and cfg.maxlen == 0 and cfg.max_deliveries == 9
    assert cfg.state_stream == "race.state.events"
    assert cfg.dead_letter_stream == "race.dead_letter.events"


class CorruptingSink:
    """Replaces the payload of one sequence with an unserializable value."""

    def __init__(self, inner: RedisStreamPublisher, bad_sequence: int) -> None:
        self._inner = inner
        self._bad = bad_sequence

    async def publish(self, event: ReplayEvent) -> None:
        if event.sequence == self._bad:
            event = replace(event, payload={"bad": object()})
        await self._inner.publish(event)


async def test_unserializable_payload_fails_replay_and_event_is_not_counted(
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,
    config: StreamConfig,
) -> None:
    timer = ManualTimer()
    service = ReplayService(
        session_factory,
        sink=CorruptingSink(RedisStreamPublisher(redis_client, config), 3),
        timer=timer,
        stop_timeout=0.5,
    )
    try:
        replay_id = (await service.create(replay_env.race.session_id, 20)).state.replay_id
        await service.start(replay_id)
        await timer.advance(10_000)
        await wait_until_idle(service)

        row = await persisted(session_factory, replay_id)
        assert row.status is ReplayStatus.FAILED
        assert row.current_sequence == 2
        assert row.status_reason and "StreamSerializationError" in row.status_reason
        sequences = [
            int(f["sequence"]) for _, f in await raw(redis_client).xrange(config.raw_stream)
        ]
        assert sequences == [0, 1, 2]
    finally:
        await service.shutdown()
