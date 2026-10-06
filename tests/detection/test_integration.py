"""Replay engine -> raw stream -> race state processor -> state stream -> detection engine."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.detection.config import DetectionConfig
from app.detection.store import DetectionContextStore
from app.detection.worker import build_consumer as build_detection_consumer
from app.infrastructure.redis import RedisClient
from app.models import DetectedEvent as DetectedEventRow
from app.race_state.config import RaceStateConfig
from app.race_state.worker import build_consumer as build_state_consumer
from app.replay.service import ReplayService
from app.streaming.config import GROUP_EVENT_DETECTORS, GROUP_STATE_PROCESSORS, StreamConfig
from app.streaming.consumer import StreamConsumer
from app.streaming.envelope import StreamEvent, derive_event_id
from app.streaming.inspection import StreamInspector
from app.streaming.publisher import RedisStreamPublisher
from tests.replay.conftest import ReplayEnv
from tests.replay.fakes import ManualTimer, wait_until_idle


@dataclass
class Pipeline:
    service: ReplayService
    timer: ManualTimer
    state_consumer: StreamConsumer
    detection_consumer: StreamConsumer
    redis: RedisClient
    config: StreamConfig


@pytest.fixture
async def pipeline(
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,
    config: StreamConfig,
) -> AsyncIterator[Pipeline]:
    timer = ManualTimer()
    service = ReplayService(
        session_factory,
        sink=RedisStreamPublisher(redis_client, config),
        timer=timer,
        stop_timeout=0.5,
    )
    state_consumer = build_state_consumer(
        redis_client, session_factory, config, RaceStateConfig(snapshot_every_laps=2)
    )
    detection_consumer = build_detection_consumer(
        redis_client, session_factory, config, DetectionConfig(), RaceStateConfig()
    )
    await state_consumer.ensure_group()
    await detection_consumer.ensure_group()
    yield Pipeline(service, timer, state_consumer, detection_consumer, redis_client, config)
    await service.shutdown()
    keys = [k async for k in redis_client.client.scan_iter(match="race:*")]
    if keys:
        await redis_client.client.delete(*keys)


async def drain(consumer: StreamConsumer, p: Pipeline, stream: str, group: str) -> None:
    for _ in range(80):
        await consumer.process_batch()
        summary = await StreamInspector(p.redis, p.config.dead_letter_stream).pending_summary(
            stream, group
        )
        groups = await p.redis.client.xinfo_groups(stream)
        lag = next(g["lag"] for g in groups if g["name"] == group)
        if not lag and not summary.pending:
            return


async def drain_all(p: Pipeline) -> None:
    await drain(p.state_consumer, p, p.config.raw_stream, GROUP_STATE_PROCESSORS)
    await drain(p.detection_consumer, p, p.config.state_stream, GROUP_EVENT_DETECTORS)


async def run_replay(p: Pipeline, replay_env: ReplayEnv) -> UUID:
    replay_id = (await p.service.create(replay_env.race.session_id, 20)).state.replay_id
    await p.service.start(replay_id)
    await p.timer.advance(10_000)
    await wait_until_idle(p.service)
    await drain_all(p)
    return replay_id


async def published(p: Pipeline) -> list[StreamEvent]:
    entries = await p.redis.client.xrange(p.config.detected_stream)
    return [StreamEvent.from_fields(fields) for _, fields in entries]


async def test_a_replayed_race_produces_detected_events(
    pipeline: Pipeline,
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    replay_id = await run_replay(pipeline, replay_env)

    events = await published(pipeline)
    by_type: dict[str, list[StreamEvent]] = {}
    for event in events:
        by_type.setdefault(event.event_type, []).append(event)

    assert events, "a replayed race must produce detections"
    assert {e.replay_id for e in events} == {replay_id}
    assert len({e.run_id for e in events}) == 1

    # HAM's pit stop starts a new stint.
    [stint] = by_type["NEW_STINT"]
    assert stint.payload["primary_driver_abbreviation"] == "HAM"
    assert stint.payload["evidence"]["stint_number"] == 2

    # Laps overlapping the safety car period are not used for pace or battles.
    assert not {"PACE_ANOMALY", "PACE_DEGRADATION", "BATTLE_FORMING"} & set(by_type)

    # Source references point back at the raw timeline event of the same run.
    for event in events:
        payload = event.payload
        assert payload["replay_id"] == str(replay_id)
        assert payload["source_event_ids"] == [
            str(derive_event_id(event.run_id, payload["source_sequence"]))
        ]
        assert payload["schema_version"] == 1 and payload["detector_version"] == 1
    assert [e.sequence for e in events] == sorted(e.sequence for e in events)

    async with session_factory() as db:
        rows = list(await db.scalars(select(DetectedEventRow)))
    assert {r.id for r in rows} == {e.event_id for e in events}

    context = await DetectionContextStore(
        pipeline.redis, stream_config=pipeline.config, config=DetectionConfig()
    ).get(replay_id)
    assert context is not None and context.state.phase.value == "COMPLETED"


async def test_a_restarted_replay_is_detected_again_under_its_new_run(
    pipeline: Pipeline, replay_env: ReplayEnv
) -> None:
    replay_id = await run_replay(pipeline, replay_env)
    first = await published(pipeline)

    await pipeline.service.restart(replay_id)
    await pipeline.timer.advance(10_000)
    await wait_until_idle(pipeline.service)
    await drain_all(pipeline)

    everything = await published(pipeline)
    second = everything[len(first) :]
    assert len({e.run_id for e in everything}) == 2
    assert [(e.event_type, e.sequence) for e in second] == [
        (e.event_type, e.sequence) for e in first
    ]
    assert {e.event_id for e in first}.isdisjoint({e.event_id for e in second})
