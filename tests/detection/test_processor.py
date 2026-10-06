"""The stream handler: Redis context, publishing, PostgreSQL, retries and isolation."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import OperationalError

from app.detection.errors import DetectionConflictError
from app.detection.processor import DetectionProcessor
from app.detection.store import DetectionTransaction
from app.domain.enums import DetectedEventType
from app.streaming.consumer import StreamConsumer
from app.streaming.envelope import StreamEvent
from app.streaming.errors import MalformedEventError
from tests.detection.builders import BASE_TIME, Scenario
from tests.detection.conftest import DetectEnv
from tests.detection.test_battle import chase

GAPS = [3200, 2400, 1500, 800]
REPLAY_2 = UUID(int=1001)
RUN_2 = UUID(int=2002)
RUN_3 = UUID(int=2003)


def battle_events(replay_id: UUID | None = None, run_id: UUID | None = None) -> list[StreamEvent]:
    kwargs = {}
    if replay_id:
        kwargs["replay_id"] = replay_id
    if run_id:
        kwargs["run_id"] = run_id
    scenario = Scenario(detectors=["battle"], **kwargs)  # type: ignore[arg-type]
    scenario.deliver = False
    chase(GAPS, scenario=scenario)
    return scenario.state_events


async def run_all(env: DetectEnv, events: list[StreamEvent]) -> None:
    for event in events:
        await env.handle(event)


async def test_detections_are_published_persisted_and_the_context_is_stored(
    detect_env: DetectEnv,
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)

    await run_all(env, events)

    published = await env.published()
    assert [e.event_type for e in published] == ["BATTLE_FORMING"]
    envelope = published[0]
    assert envelope.replay_id == events[0].replay_id and envelope.run_id == events[0].run_id
    assert envelope.sequence == events[-1].sequence
    assert envelope.payload["primary_driver_abbreviation"] == "BBB"
    assert envelope.payload["evidence"]["gap_ms"] == 800

    [row] = await env.rows()
    assert row.id == envelope.event_id
    assert (row.event_type, row.primary_driver_abbreviation) == ("BATTLE_FORMING", "BBB")
    assert row.evidence["gap_history"][0] == {"lap": 2, "gap_ms": 3200}

    context = await env.store.get(events[0].replay_id)
    assert context is not None and context.last_sequence == events[-1].sequence
    assert "battle" in context.detectors
    ttl = await env.redis.client.ttl(env.store.key(events[0].replay_id))
    assert 0 < ttl <= env.detection_config.ttl_seconds


async def test_redelivered_state_events_publish_and_persist_nothing_twice(
    detect_env: DetectEnv,
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)
    await run_all(env, events)
    context_before = (await env.store.get(events[0].replay_id)).model_dump_json()  # type: ignore[union-attr]

    await run_all(env, events)  # the whole stream again, e.g. after a consumer restart

    assert len(await env.published()) == 1
    assert len(await env.rows()) == 1
    assert (await env.store.get(events[0].replay_id)).model_dump_json() == context_before  # type: ignore[union-attr]


async def test_a_redis_failure_after_persisting_does_not_duplicate_on_retry(
    detect_env: DetectEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)
    await run_all(env, events[:-1])

    original = DetectionTransaction.commit
    calls = {"n": 0}

    async def flaky_commit(self: DetectionTransaction, context, published) -> None:  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] == 1:
            raise RedisConnectionError("redis down")
        await original(self, context, published)

    monkeypatch.setattr(DetectionTransaction, "commit", flaky_commit)

    with pytest.raises(RedisConnectionError):
        await env.handle(events[-1])
    assert len(await env.rows()) == 1  # persisted before the failing commit
    assert await env.published() == []  # ... but not published, and the context not advanced

    await env.handle(events[-1])  # the consumer redelivers

    assert len(await env.published()) == 1
    assert len(await env.rows()) == 1  # insert-ignore: still one row
    context = await env.store.get(events[0].replay_id)
    assert context is not None and context.last_sequence == events[-1].sequence


async def test_a_database_failure_leaves_the_context_untouched_for_the_retry(
    detect_env: DetectEnv,
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)
    await run_all(env, events[:-1])

    class DownSink:
        async def save(self, batch: object) -> None:
            raise OperationalError("INSERT", {}, Exception("database down"))

    with pytest.raises(OperationalError):
        await env.handle(events[-1], env.make_processor(DownSink()))

    context = await env.store.get(events[0].replay_id)
    assert context is not None and context.last_sequence == events[-2].sequence
    assert await env.published() == []

    await env.handle(events[-1])
    assert len(await env.published()) == 1


async def test_a_lost_compare_and_set_is_retried_in_process(
    detect_env: DetectEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)
    original = DetectionTransaction.commit
    calls = {"n": 0}

    async def conflicting_commit(self: DetectionTransaction, context, published) -> None:  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] == 1:
            raise DetectionConflictError("another worker won")
        await original(self, context, published)

    monkeypatch.setattr(DetectionTransaction, "commit", conflicting_commit)

    await run_all(env, events)

    assert calls["n"] == len(events) + 1
    assert len(await env.published()) == 1


async def test_persistent_conflicts_are_raised_after_the_attempt_limit(
    detect_env: DetectEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)

    async def always_conflict(self: DetectionTransaction, context, published) -> None:  # noqa: ANN001
        raise DetectionConflictError("busy")

    monkeypatch.setattr(DetectionTransaction, "commit", always_conflict)

    with pytest.raises(DetectionConflictError):
        await env.handle(events[0])


async def test_replays_are_isolated_from_each_other(detect_env: DetectEnv) -> None:
    env = detect_env
    first = battle_events()
    second = battle_events(REPLAY_2, RUN_2)  # identical race, other replay
    await env.add_replay(first[0].replay_id)
    await env.add_replay(REPLAY_2)

    for a, b in zip(first, second, strict=True):  # interleaved
        await env.handle(a)
        await env.handle(b)

    published = await env.published()
    assert {(e.replay_id, e.event_type) for e in published} == {
        (first[0].replay_id, "BATTLE_FORMING"),
        (REPLAY_2, "BATTLE_FORMING"),
    }
    assert len(published) == 2
    assert {r.replay_id for r in await env.rows()} == {first[0].replay_id, REPLAY_2}
    one = await env.store.get(first[0].replay_id)
    two = await env.store.get(REPLAY_2)
    assert one is not None and two is not None and one.run_id != two.run_id
    assert one.state.replay_id != two.state.replay_id


async def test_a_new_run_of_a_replay_starts_with_fresh_detector_memory(
    detect_env: DetectEnv,
) -> None:
    env = detect_env
    run_a = battle_events()
    run_b = battle_events(run_id=RUN_2)
    replay_id = run_a[0].replay_id
    await env.add_replay(replay_id)

    await run_all(env, run_a)
    await run_all(env, run_b)

    published = await env.published()
    assert [e.run_id for e in published] == [run_a[0].run_id, RUN_2]
    context = await env.store.get(replay_id)
    assert context is not None and context.run_id == RUN_2
    assert len({r.id for r in await env.rows()}) == 2


async def test_stragglers_of_an_older_run_are_ignored(detect_env: DetectEnv) -> None:
    env = detect_env
    run_a = battle_events()
    run_b = battle_events(run_id=RUN_2)
    await env.add_replay(run_a[0].replay_id)
    await run_all(env, run_a[:3])
    await run_all(env, run_b[:3])
    straggler = replace(run_a[3], published_at=BASE_TIME - timedelta(seconds=10))

    await env.handle(straggler)

    context = await env.store.get(run_a[0].replay_id)
    assert context is not None and context.run_id == RUN_2
    assert await env.published() == []


async def test_an_update_without_a_context_bootstraps_from_the_race_state(
    detect_env: DetectEnv,
) -> None:
    env = detect_env
    events = battle_events()
    replay_id = events[0].replay_id
    scenario = Scenario(detectors=["battle"])
    scenario.deliver = False
    chase(GAPS, scenario=scenario)
    async with env.state_store.transaction(replay_id) as txn:
        await txn.load()
        await txn.commit(scenario.state, None)  # the race state processor is ahead of us
    await env.add_replay(replay_id)

    await env.handle(events[-1])  # first update this worker sees

    context = await env.store.get(replay_id)
    assert context is not None and context.last_sequence == events[-1].sequence
    assert context.state.drivers  # mirror taken from the race state
    assert await env.published() == []  # windows start empty: no premature battle


async def test_an_update_without_context_or_race_state_is_skipped(detect_env: DetectEnv) -> None:
    env = detect_env
    events = battle_events()

    await env.handle(events[-1])

    assert await env.store.get(events[-1].replay_id) is None
    assert await env.published() == []


async def test_malformed_and_foreign_events(detect_env: DetectEnv) -> None:
    env = detect_env
    good = battle_events()[1]

    with pytest.raises(MalformedEventError):
        await env.handle(replace(good, payload={"unexpected": True}))

    await env.handle(replace(good, event_type="LAP_COMPLETED"))  # not a state event: ignored
    assert await env.store.get(good.replay_id) is None


async def test_the_consumer_acknowledges_after_handling(
    detect_env: DetectEnv,
    config,  # noqa: ANN001
) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)
    consumer = StreamConsumer(
        env.redis,
        stream=config.state_stream,
        group="race-event-detectors",
        consumer_name="detect-1",
        handler=env.processor,
        config=config,
    )
    await consumer.ensure_group()
    for event in events:
        await env.redis.client.xadd(config.state_stream, event.to_fields())

    for _ in range(len(events) + 2):
        await consumer.process_batch()

    assert consumer.stats.processed == len(events)
    assert consumer.stats.failed == 0 and consumer.stats.dead_lettered == 0
    pending = await env.redis.client.xpending(config.state_stream, "race-event-detectors")
    assert pending["pending"] == 0
    assert [e.event_type for e in await env.published()] == ["BATTLE_FORMING"]


def test_the_worker_wires_the_state_stream_and_group(detect_env: DetectEnv) -> None:
    from app.detection.worker import build_consumer
    from app.race_state.config import RaceStateConfig
    from app.streaming.config import GROUP_EVENT_DETECTORS

    consumer = build_consumer(
        detect_env.redis,
        detect_env.factory,
        detect_env.stream_config,
        detect_env.detection_config,
        RaceStateConfig(),
    )

    assert consumer.stream == detect_env.stream_config.state_stream
    assert consumer.group == GROUP_EVENT_DETECTORS == "race-event-detectors"
    assert DetectedEventType.OVERTAKE.value == "OVERTAKE"
    assert isinstance(detect_env.processor, DetectionProcessor)


async def test_a_deleted_replay_is_skipped_without_retry_or_publishing(
    detect_env: DetectEnv,
) -> None:
    env = detect_env
    events = battle_events()  # the replay row was never created: FK violation

    await run_all(env, events)  # must not raise

    assert await env.published() == []
    assert await env.rows() == []
    context = await env.store.get(events[0].replay_id)
    assert context is not None and context.last_sequence == events[-2].sequence  # skipped one


async def test_other_database_errors_stay_retryable(detect_env: DetectEnv) -> None:
    env = detect_env
    events = battle_events()
    await env.add_replay(events[0].replay_id)
    await run_all(env, events[:-1])

    class DownSink:
        async def save(self, batch: object) -> None:
            raise OperationalError("INSERT", {}, Exception("database down"))

    with pytest.raises(OperationalError):
        await env.handle(events[-1], env.make_processor(DownSink()))
