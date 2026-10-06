"""Redis hot store: documents, key isolation and atomic state + event commits."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis

from app.infrastructure.redis import RedisClient
from app.race_state.config import RaceStateConfig
from app.race_state.errors import StateConflictError
from app.race_state.events import build_state_event
from app.race_state.models import StateDelta, StateEventType
from app.race_state.reducer import initial_state
from app.race_state.repository import RaceStateStore
from app.streaming.config import StreamConfig
from app.streaming.envelope import StreamEvent, derive_event_id
from tests.race_state.builders import HAM, REPLAY, RUN, SESSION, make_seed
from tests.race_state.conftest import BASE_TIME


def make_store(redis: RedisClient, config: StreamConfig, **overrides: object) -> RaceStateStore:
    return RaceStateStore(
        redis,
        stream_config=config,
        config=RaceStateConfig(**overrides),  # type: ignore[arg-type]
    )


def state_for(replay_id: UUID, run_id: UUID = RUN):
    return initial_state(make_seed(), replay_id=replay_id, run_id=run_id)


def source_event(replay_id: UUID, sequence: int = 0, run_id: UUID = RUN) -> StreamEvent:
    return StreamEvent(
        event_id=derive_event_id(run_id, sequence),
        schema_version=1,
        event_type="LAP_COMPLETED",
        replay_id=replay_id,
        run_id=run_id,
        session_id=SESSION,
        sequence=sequence,
        race_time_ms=1000,
        lap_number=1,
        driver_id=HAM,
        driver_abbreviation="HAM",
        published_at=BASE_TIME,
        payload={},
    )


def state_event(replay_id: UUID, state, sequence: int = 0) -> StreamEvent:
    return build_state_event(
        event_type=StateEventType.STATE_UPDATED,
        state=state,
        delta=StateDelta(),
        source=source_event(replay_id, sequence),
        rebuilt=False,
        published_at=BASE_TIME,
    )


async def test_missing_state_reads_as_none(redis_client: RedisClient, config: StreamConfig) -> None:
    store = make_store(redis_client, config)
    assert await store.get(uuid4()) is None


async def test_write_and_read_back_the_full_state(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = make_store(redis_client, config)
    state = state_for(REPLAY)
    async with store.transaction(REPLAY) as txn:
        assert await txn.load() is None
        await txn.commit(state, None)

    loaded = await store.get(REPLAY)
    assert loaded == state
    assert store.key(REPLAY) == f"race:{REPLAY}:state"
    ttl = await redis_client.client.ttl(store.key(REPLAY))
    assert 0 < ttl <= 604_800
    await redis_client.client.delete(store.key(REPLAY))


async def test_key_prefix_and_ttl_are_configurable(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = make_store(redis_client, config, key_prefix="custom", ttl_seconds=60)
    async with store.transaction(REPLAY) as txn:
        await txn.commit(state_for(REPLAY), None)
    assert store.key(REPLAY).startswith("custom:")
    assert 0 < await redis_client.client.ttl(store.key(REPLAY)) <= 60
    await redis_client.client.delete(store.key(REPLAY))


async def test_states_of_different_replays_are_isolated(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = make_store(redis_client, config)
    a, b = uuid4(), uuid4()
    for replay_id in (a, b):
        async with store.transaction(replay_id) as txn:
            await txn.commit(state_for(replay_id, uuid4()), None)
    state_a, state_b = await store.get(a), await store.get(b)
    assert state_a is not None and state_b is not None
    assert state_a.replay_id == a and state_b.replay_id == b
    assert state_a.run_id != state_b.run_id
    await redis_client.client.delete(store.key(a), store.key(b))


async def test_state_and_state_event_are_committed_together(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = make_store(redis_client, config)
    state = state_for(REPLAY)
    async with store.transaction(REPLAY) as txn:
        await txn.commit(state, state_event(REPLAY, state))

    entries = await redis_client.client.xrange(config.state_stream)
    assert len(entries) == 1
    decoded = StreamEvent.from_fields(entries[0][1])
    assert decoded.event_type == "STATE_UPDATED"
    assert (decoded.replay_id, decoded.run_id, decoded.sequence) == (REPLAY, RUN, 0)
    assert await store.get(REPLAY) == state
    await redis_client.client.delete(store.key(REPLAY))


async def test_concurrent_change_aborts_the_commit_and_publishes_nothing(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = make_store(redis_client, config)
    state = state_for(REPLAY)
    async with store.transaction(REPLAY) as txn:
        await txn.commit(state, None)

    other = state.model_copy(update={"last_sequence": 5})
    async with store.transaction(REPLAY) as txn:
        assert await txn.load() == state
        # Another worker writes between our read and our commit.
        raw: Redis = redis_client.client
        await raw.set(store.key(REPLAY), other.model_dump_json())
        with pytest.raises(StateConflictError):
            await txn.commit(
                state.model_copy(update={"last_sequence": 1}), state_event(REPLAY, state, 1)
            )

    stored = await store.get(REPLAY)
    assert stored is not None and stored.last_sequence == 5
    assert await redis_client.client.xlen(config.state_stream) == 0
    await redis_client.client.delete(store.key(REPLAY))


async def test_unreadable_or_other_version_documents_count_as_missing(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = make_store(redis_client, config)
    await redis_client.client.set(store.key(REPLAY), "{not json")
    assert await store.get(REPLAY) is None

    future = state_for(REPLAY).model_copy(update={"schema_version": 99})
    await redis_client.client.set(store.key(REPLAY), future.model_dump_json())
    assert await store.get(REPLAY) is None
    await redis_client.client.delete(store.key(REPLAY))


async def test_state_stream_is_trimmed_approximately(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    from dataclasses import replace

    store = make_store(redis_client, replace(config, maxlen=10))
    state = state_for(REPLAY)
    for sequence in range(300):
        async with store.transaction(REPLAY) as txn:
            await txn.commit(state, state_event(REPLAY, state, sequence))
    assert await redis_client.client.xlen(config.state_stream) < 300
    await redis_client.client.delete(store.key(REPLAY))
