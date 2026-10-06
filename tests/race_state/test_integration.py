"""Replay engine -> raw stream -> StreamConsumer + RaceStateProcessor -> Redis state."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.redis import RedisClient
from app.models import RaceStateSnapshot
from app.race_state.config import RaceStateConfig
from app.race_state.models import DriverRaceStatus, RacePhase
from app.race_state.processor import RaceStateProcessor
from app.race_state.repository import RaceStateStore
from app.race_state.worker import build_consumer
from app.replay.service import ReplayService
from app.streaming.config import GROUP_STATE_PROCESSORS, StreamConfig
from app.streaming.consumer import ReceivedMessage, StreamConsumer
from app.streaming.envelope import StreamEvent
from app.streaming.inspection import StreamInspector
from app.streaming.publisher import RedisStreamPublisher
from tests.race_state.conftest import StateEnv, make_replay
from tests.race_state.test_processor import reduce_independently
from tests.replay.conftest import ReplayEnv
from tests.replay.fakes import ManualTimer, wait_until_idle


@pytest.fixture
async def live(
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,
    config: StreamConfig,
) -> AsyncIterator[tuple[ReplayService, ManualTimer, StreamConsumer, RaceStateStore]]:
    timer = ManualTimer()
    service = ReplayService(
        session_factory,
        sink=RedisStreamPublisher(redis_client, config),
        timer=timer,
        stop_timeout=0.5,
    )
    state_config = RaceStateConfig(snapshot_every_laps=2)
    consumer = build_consumer(redis_client, session_factory, config, state_config)
    await consumer.ensure_group()
    store = RaceStateStore(redis_client, stream_config=config, config=state_config)
    yield service, timer, consumer, store
    await service.shutdown()
    keys = [k async for k in redis_client.client.scan_iter(match="race:*:state")]
    if keys:
        await redis_client.client.delete(*keys)


async def drain(consumer: StreamConsumer, redis: RedisClient, config: StreamConfig) -> None:
    for _ in range(50):
        await consumer.process_batch()
        summary = await StreamInspector(redis, config.dead_letter_stream).pending_summary(
            config.raw_stream, GROUP_STATE_PROCESSORS
        )
        groups = await redis.client.xinfo_groups(config.raw_stream)
        lag = next(g["lag"] for g in groups if g["name"] == GROUP_STATE_PROCESSORS)
        if not lag and not summary.pending:
            return


async def test_replayed_race_produces_the_final_state_and_state_events(
    live: tuple[ReplayService, ManualTimer, StreamConsumer, RaceStateStore],
    replay_env: ReplayEnv,
    redis_client: RedisClient,
    config: StreamConfig,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, timer, consumer, store = live
    replay_id = (await service.create(replay_env.race.session_id, 20)).state.replay_id
    await service.start(replay_id)
    await timer.advance(10_000)
    await wait_until_idle(service)

    await drain(consumer, redis_client, config)

    state = await store.get(replay_id)
    assert state is not None
    assert state.phase is RacePhase.COMPLETED
    assert state.last_sequence == replay_env.event_count - 1
    assert state.leader_laps_completed == state.total_laps == 5
    assert state.fastest_lap is not None and state.fastest_lap.abbreviation == "NOR"
    assert [d.abbreviation for d in state.drivers_by_position()] == ["NOR", "VER", "HAM"]
    assert all(d.race_status is DriverRaceStatus.FINISHED for d in state.drivers.values())
    ham = next(d for d in state.drivers.values() if d.abbreviation == "HAM")
    assert ham.pit_stop_count == 1 and ham.stint_number == 2

    entries = await redis_client.client.xrange(config.state_stream)
    published = [StreamEvent.from_fields(fields) for _, fields in entries]
    assert published[0].event_type == "STATE_INITIALIZED"
    assert published[-1].event_type == "STATE_COMPLETED"
    assert {e.replay_id for e in published} == {replay_id}
    assert len({e.run_id for e in published}) == 1
    assert [e.sequence for e in published] == sorted(e.sequence for e in published)

    async with session_factory() as db:
        count = await db.scalar(select(func.count()).select_from(RaceStateSnapshot))
    assert count == 4  # INITIAL, two PERIODIC, FINAL: never per event


async def test_restart_resets_the_state_to_the_new_run(
    live: tuple[ReplayService, ManualTimer, StreamConsumer, RaceStateStore],
    replay_env: ReplayEnv,
    redis_client: RedisClient,
    config: StreamConfig,
) -> None:
    service, timer, consumer, store = live
    replay_id = (await service.create(replay_env.race.session_id, 20)).state.replay_id
    await service.start(replay_id)
    await timer.advance(10_000)
    await wait_until_idle(service)
    await drain(consumer, redis_client, config)
    first = await store.get(replay_id)
    assert first is not None

    await service.restart(replay_id)
    await timer.advance(10_000)
    await wait_until_idle(service)
    await drain(consumer, redis_client, config)

    second = await store.get(replay_id)
    assert second is not None
    assert second.run_id != first.run_id
    assert second.phase is RacePhase.COMPLETED
    first_dump, second_dump = first.logical_dump(), second.logical_dump()
    for dump in (first_dump, second_dump):
        for key in ("run_id", "last_event_id"):
            dump.pop(key)
    assert first_dump == second_dump  # same timeline, same logical state


async def _consume_stream(
    env: StateEnv,
    redis_client: RedisClient,
    config: StreamConfig,
    replay_id: UUID,
    entries: list[StreamEvent],
    processor: RaceStateProcessor,
) -> StreamConsumer:
    from dataclasses import replace

    # Reclaim effectively never fires: only in-process resolution can make progress.
    slow = replace(config, reclaim_idle_ms=30_000)
    consumer = StreamConsumer(
        redis_client,
        stream=config.raw_stream,
        group=GROUP_STATE_PROCESSORS,
        consumer_name="state-1",
        handler=processor,
        config=slow,
    )
    await consumer.ensure_group()
    for event in entries:
        await redis_client.client.xadd(config.raw_stream, event.to_fields())
    for _ in range(len(entries) + 5):
        await consumer.process_batch()
    return consumer


class FlakyProcessor:
    """Wraps the processor; the chosen sequence fails once before touching state."""

    def __init__(self, inner: RaceStateProcessor, fail_at: int) -> None:
        self._inner = inner
        self.fail_at = fail_at
        self.failed = False

    async def handle(self, message: ReceivedMessage) -> None:
        if message.event.sequence == self.fail_at and not self.failed:
            self.failed = True
            raise ConnectionError("transient database failure")
        await self._inner.handle(message)


async def test_a_transient_failure_does_not_delay_later_events_until_reclaim(
    state_env: StateEnv, redis_client: RedisClient, config: StreamConfig
) -> None:
    env = state_env
    replay_id = await make_replay(env.factory, env.race.session_id)
    events = env.events(replay_id, uuid4())
    flaky = FlakyProcessor(env.processor, fail_at=1)

    consumer = await _consume_stream(env, redis_client, config, replay_id, events, flaky)  # type: ignore[arg-type]

    state = await env.state(replay_id)
    expected = await reduce_independently(env, events)
    assert state is not None and state.last_sequence == len(events) - 1
    assert state.phase is RacePhase.COMPLETED
    assert state.logical_dump() == expected.logical_dump()
    assert consumer.stats.failed == 1 and consumer.stats.dead_lettered == 0
    assert consumer.stats.reclaimed == 0  # no waiting for reclaim

    # The failed message is still pending; when it is finally retried it is a duplicate.
    published = len(await env.state_events())
    later = StreamConsumer(  # reclaim idle time 0: the pending message is retried now
        redis_client,
        stream=config.raw_stream,
        group=GROUP_STATE_PROCESSORS,
        consumer_name="state-2",
        handler=env.processor,
        config=config,
    )
    await later.reclaim_stale()
    assert later.stats.reclaimed == 1 and later.stats.failed == 0  # a duplicate, ACKed
    assert len(await env.state_events()) == published
    pending = await StreamInspector(redis_client, config.dead_letter_stream).pending_summary(
        config.raw_stream, GROUP_STATE_PROCESSORS
    )
    assert pending.pending == 0


async def test_an_event_that_is_never_published_does_not_stall_the_state(
    state_env: StateEnv, redis_client: RedisClient, config: StreamConfig
) -> None:
    env = state_env
    replay_id = await make_replay(env.factory, env.race.session_id)
    events = env.events(replay_id, uuid4())
    entries = [e for e in events if e.sequence != 5]  # sequence 5 is lost

    consumer = await _consume_stream(env, redis_client, config, replay_id, entries, env.processor)

    state = await env.state(replay_id)
    expected = await reduce_independently(env, events)
    assert state is not None and state.phase is RacePhase.COMPLETED
    assert state.last_sequence == len(events) - 1
    assert state.logical_dump() == expected.logical_dump()
    assert consumer.stats.dead_lettered == 0 and consumer.stats.failed == 0
    types = [e.event_type for e in await env.state_events()]
    assert "STATE_REBUILT" in types and types[-1] == "STATE_COMPLETED"


async def test_out_of_order_stream_entries_converge_without_failures(
    state_env: StateEnv, redis_client: RedisClient, config: StreamConfig
) -> None:
    env = state_env
    replay_id = await make_replay(env.factory, env.race.session_id)
    events = env.events(replay_id, uuid4())

    consumer = await _consume_stream(
        env,
        redis_client,
        config,
        replay_id,
        [events[0], events[2], events[1], events[3]],
        env.processor,
    )

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 3
    assert consumer.stats.failed == 0 and consumer.stats.dead_lettered == 0
