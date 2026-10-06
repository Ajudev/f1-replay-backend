"""RaceStateProcessor: ordering, idempotency, rebuild, snapshots, state events."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from app.models import RaceStateSnapshot
from app.race_state.errors import (
    RaceSeedError,
    RaceStateRebuildError,
    SnapshotPersistError,
)
from app.race_state.models import (
    ChangeKind,
    DriverRaceStatus,
    RacePhase,
    RaceState,
    SnapshotTrigger,
    StateEventType,
)
from app.race_state.processor import Action, decide, reducer_event
from app.race_state.reducer import apply_event, initial_state
from app.race_state.repository import RaceStateStore
from app.race_state.seed import RaceSeedLoader
from app.streaming.envelope import StreamEvent
from tests.race_state.conftest import (
    BASE_TIME,
    FailingSnapshots,
    FakeSleep,
    StateEnv,
    make_replay,
)


async def run_all(env: StateEnv, events: list[StreamEvent], upto: int | None = None) -> None:
    for event in events[: upto if upto is not None else len(events)]:
        await env.process(event)


async def reduce_independently(env: StateEnv, events: list[StreamEvent]) -> RaceState:
    """What the pure reducer yields for the same events (no Redis, no processor)."""

    seed = await RaceSeedLoader(env.factory).load_seed(env.race.session_id)
    state = initial_state(
        seed,
        replay_id=events[0].replay_id,
        run_id=events[0].run_id,
        run_published_at=events[0].published_at,
    )
    for event in events:
        state, _ = apply_event(state, reducer_event(event), config=env.state_config)
    return state


async def snapshot_rows(env: StateEnv, replay_id: UUID) -> list[RaceStateSnapshot]:
    async with env.factory() as db:
        rows = await db.scalars(
            select(RaceStateSnapshot)
            .where(RaceStateSnapshot.replay_id == replay_id)
            .order_by(RaceStateSnapshot.sequence)
        )
        return list(rows)


# -- initialization and a complete run -----------------------------------------------------


async def test_first_event_initializes_state_and_publishes_a_snapshot_event(
    state_env: StateEnv,
) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)

    await env.process(events[0])

    state = await env.state(replay_id)
    assert state is not None
    assert state.phase is RacePhase.RUNNING and state.current_lap == 1
    assert state.last_sequence == 0 and state.last_event_id == events[0].event_id
    assert state.total_events == len(events) and state.total_laps == 5
    assert {d.abbreviation: d.grid_position for d in state.drivers.values()} == {
        "VER": 1,
        "HAM": 2,
        "NOR": 3,
    }
    published = await env.state_events()
    assert [e.event_type for e in published] == ["STATE_INITIALIZED"]
    assert "state" in published[0].payload
    assert "lap_crossings" not in published[0].payload["state"]
    rows = await snapshot_rows(env, replay_id)
    assert [(r.sequence, r.trigger) for r in rows] == [(0, "INITIAL")]


async def test_a_full_run_ends_completed_and_matches_the_pure_reduction(
    state_env: StateEnv,
) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)

    await run_all(env, events)

    state = await env.state(replay_id)
    expected = await reduce_independently(env, events)
    assert state is not None
    assert state.logical_dump() == expected.logical_dump()
    assert state.phase is RacePhase.COMPLETED
    assert state.last_sequence == len(events) - 1
    assert state.fastest_lap is not None and state.fastest_lap.lap_time_ms == 88_500
    assert state.drivers[env.timeline[2].driver_id].race_status is DriverRaceStatus.FINISHED
    assert state.leader_laps_completed == 5 and state.current_lap == 5

    published = await env.state_events()
    types = [e.event_type for e in published]
    assert types[0] == "STATE_INITIALIZED" and types[-1] == "STATE_COMPLETED"
    assert set(types[1:-1]) == {"STATE_UPDATED"}
    assert "state" in published[-1].payload
    assert ChangeKind.RACE_COMPLETED.value in published[-1].payload["kinds"]
    # Only meaningful changes are published: fewer state events than raw events.
    assert len(published) < len(events)
    # State remains available after completion.
    assert await env.store.get(replay_id) is not None


# -- idempotency, ordering -------------------------------------------------------------------


async def test_processing_the_same_event_many_times_changes_nothing_after_the_first(
    state_env: StateEnv,
) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=7)
    before = await env.state(replay_id)
    published_before = len(await env.state_events())

    for _ in range(3):
        await env.process(events[6])
        await env.process(events[2])  # an older one too

    after = await env.state(replay_id)
    assert after is not None and before is not None
    assert after.model_dump_json() == before.model_dump_json()  # even updated_at is untouched
    assert len(await env.state_events()) == published_before


async def test_duplicate_position_changed_is_not_reapplied(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=7)  # includes POSITION_CHANGED at sequence 6
    assert events[6].event_type == "POSITION_CHANGED"
    before = await env.state(replay_id)

    await env.process(events[6])

    assert await env.state(replay_id) == before


async def test_event_ahead_waits_in_process_and_applies_once_the_predecessor_lands(
    state_env: StateEnv,
) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=2)  # 0 and 1 applied

    async def predecessor_lands(call: int) -> None:
        if call == 2:
            await env.process(events[2])  # a concurrent worker finishes seq 2

    sleep = FakeSleep(predecessor_lands)
    await env.process(events[3], env.make_processor(sleep=sleep))  # "102 before 101"

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 3
    assert len(sleep.calls) == 2  # waited, did not rebuild
    assert "STATE_REBUILT" not in [e.event_type for e in await env.state_events()]
    expected = await reduce_independently(env, events[:4])
    assert state.logical_dump() == expected.logical_dump()


async def test_persistent_gap_is_resolved_by_rebuilding_from_the_timeline(
    state_env: StateEnv, caplog: pytest.LogCaptureFixture
) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=3)
    before = await env.state(replay_id)
    assert before is not None

    sleep = FakeSleep()
    with caplog.at_level("WARNING"):
        await env.process(events[8], env.make_processor(sleep=sleep))  # 3..7 never arrive

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 8
    assert state.run_id == run_id and state.run_published_at == before.run_published_at
    assert sum(sleep.calls) >= env.state_config.gap_wait_ms / 1000
    assert (await env.state_events())[-1].event_type == "STATE_REBUILT"
    assert "gap resolved by rebuild" in caplog.text
    expected = await reduce_independently(env, events[:9])
    assert state.logical_dump() == expected.logical_dump()

    # The late predecessors are duplicates now.
    published = len(await env.state_events())
    for late in events[3:8]:
        await env.process(late)
    assert len(await env.state_events()) == published


async def test_incremental_gap_rebuild_equals_a_rebuild_from_scratch(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=5)
    await env.process(events[17])  # gap rebuild from the current state
    incremental = await env.state(replay_id)

    other = await make_replay(env.factory, env.race.session_id)
    other_events = env.events(other, run_id)
    await env.process(other_events[17])  # no state: full rebuild from sequence 0
    scratch = await env.state(other)

    assert incremental is not None and scratch is not None
    a, b = incremental.logical_dump(), scratch.logical_dump()
    a.pop("replay_id"), b.pop("replay_id")
    assert a == b


async def test_gap_rebuild_fails_loudly_when_the_stored_timeline_is_incomplete(
    state_env: StateEnv,
) -> None:
    from dataclasses import replace

    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=3)

    with pytest.raises(RaceStateRebuildError):
        await env.process(replace(events[8], sequence=900))
    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 2


async def test_state_conflict_is_retried_in_process(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=3)

    original = type(env.store).transaction
    state_box = {"raced": False}

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def racing_transaction(self, replay):  # type: ignore[no-untyped-def]
        async with original(self, replay) as txn:
            real_load = txn.load

            async def load():  # type: ignore[no-untyped-def]
                result = await real_load()
                if not state_box["raced"]:
                    state_box["raced"] = True
                    # A concurrent writer changes the key after our read.
                    await env.redis.client.set(
                        self.key(replay), result.model_dump_json() if result else "x"
                    )
                return result

            txn.load = load  # type: ignore[method-assign]
            yield txn

    type(env.store).transaction = racing_transaction  # type: ignore[method-assign]
    try:
        await env.process(events[3])
    finally:
        type(env.store).transaction = original  # type: ignore[method-assign]

    assert state_box["raced"]
    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 3


async def test_persistent_state_conflicts_eventually_raise(state_env: StateEnv) -> None:
    from app.race_state.errors import StateConflictError

    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=3)
    original = type(env.store).transaction
    attempts = {"n": 0}

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def always_conflicting(self, replay):  # type: ignore[no-untyped-def]
        async with original(self, replay) as txn:
            real_load = txn.load

            async def load():  # type: ignore[no-untyped-def]
                attempts["n"] += 1
                result = await real_load()
                await env.redis.client.set(self.key(replay), result.model_dump_json())
                return result

            txn.load = load  # type: ignore[method-assign]
            yield txn

    type(env.store).transaction = always_conflicting  # type: ignore[method-assign]
    try:
        with pytest.raises(StateConflictError):
            await env.process(events[3])
    finally:
        type(env.store).transaction = original  # type: ignore[method-assign]
    assert attempts["n"] == env.state_config.conflict_attempts


# -- runs ------------------------------------------------------------------------------------


async def test_events_of_an_older_run_are_ignored(state_env: StateEnv) -> None:
    env = state_env
    replay_id = await make_replay(env.factory, env.race.session_id)
    old_run, new_run = uuid4(), uuid4()
    old = env.events(replay_id, old_run, published_at=BASE_TIME)
    new = env.events(replay_id, new_run, published_at=BASE_TIME + timedelta(hours=1))
    await run_all(env, new, upto=4)
    published = len(await env.state_events())

    await env.process(old[0])  # a straggler of the previous run
    await env.process(old[5])

    state = await env.state(replay_id)
    assert state is not None and state.run_id == new_run and state.last_sequence == 3
    assert len(await env.state_events()) == published


async def test_a_new_run_starts_from_scratch(state_env: StateEnv) -> None:
    env = state_env
    replay_id = await make_replay(env.factory, env.race.session_id)
    first, second = uuid4(), uuid4()
    run_one = env.events(replay_id, first, published_at=BASE_TIME)
    run_two = env.events(replay_id, second, published_at=BASE_TIME + timedelta(hours=1))
    await run_all(env, run_one, upto=10)

    await env.process(run_two[0])

    state = await env.state(replay_id)
    assert state is not None
    assert (state.run_id, state.last_sequence, state.leader_laps_completed) == (second, 0, 0)
    assert all(d.laps_completed == 0 for d in state.drivers.values())
    assert (await env.state_events())[-1].event_type == "STATE_INITIALIZED"
    # ... and the new run then continues normally.
    await run_all(env, run_two[1:])
    final = await env.state(replay_id)
    assert final is not None and final.phase is RacePhase.COMPLETED and final.run_id == second


async def test_decide_covers_every_ordering_case() -> None:
    from tests.race_state.builders import make_seed

    run, other = uuid4(), uuid4()
    state = initial_state(make_seed(), replay_id=uuid4(), run_id=run, run_published_at=BASE_TIME)
    state.last_sequence = 5

    def ev(run_id: UUID, sequence: int, offset: int = 0) -> StreamEvent:
        return StreamEvent(
            event_id=uuid4(),
            schema_version=1,
            event_type="LAP_COMPLETED",
            replay_id=state.replay_id,
            run_id=run_id,
            session_id=state.session_id,
            sequence=sequence,
            race_time_ms=0,
            lap_number=None,
            driver_id=None,
            driver_abbreviation=None,
            published_at=BASE_TIME + timedelta(seconds=offset),
            payload={},
        )

    assert decide(None, ev(run, 0)) is Action.INITIALIZE
    assert decide(None, ev(run, 4)) is Action.REBUILD
    assert decide(state, ev(run, 5)) is Action.DUPLICATE
    assert decide(state, ev(run, 6)) is Action.APPLY
    assert decide(state, ev(run, 7)) is Action.GAP
    assert decide(state, ev(other, 3, offset=-1)) is Action.STALE
    assert decide(state, ev(other, 0, offset=1)) is Action.INITIALIZE
    assert decide(state, ev(other, 3, offset=1)) is Action.REBUILD


# -- recovery --------------------------------------------------------------------------------


async def test_state_lost_from_redis_is_rebuilt_from_the_timeline(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=13)
    await env.redis.client.delete(env.store.key(replay_id))  # Redis lost its data
    published_before = len(await env.state_events())

    await env.process(events[13])  # first event after the loss
    rebuilt = await env.state(replay_id)
    assert rebuilt is not None and rebuilt.last_sequence == 13
    published = await env.state_events()
    assert len(published) == published_before + 1  # one snapshot, not 13 replayed events
    assert published[-1].event_type == "STATE_REBUILT"
    assert published[-1].payload["rebuilt"] is True and "state" in published[-1].payload

    await run_all(env, events[14:])
    final = await env.state(replay_id)
    expected = await reduce_independently(env, events)
    assert final is not None
    assert final.logical_dump() == expected.logical_dump()
    assert (13, "REBUILT") in [(r.sequence, r.trigger) for r in await snapshot_rows(env, replay_id)]


async def test_a_worker_joining_mid_run_rebuilds_instead_of_waiting(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)

    await env.process(events[9])  # no state at all, sequence > 0

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 9
    assert (await env.state_events())[-1].event_type == "STATE_REBUILT"
    expected = await reduce_independently(env, events[:10])
    assert state.logical_dump() == expected.logical_dump()
    # A rebuild always leaves a recovery point (here the periodic trigger is due as well).
    assert [r.sequence for r in await snapshot_rows(env, replay_id)] == [9]


async def test_a_newer_run_seen_mid_stream_rebuilds_its_state(state_env: StateEnv) -> None:
    env = state_env
    replay_id = await make_replay(env.factory, env.race.session_id)
    old = env.events(replay_id, uuid4(), published_at=BASE_TIME)
    new = env.events(replay_id, uuid4(), published_at=BASE_TIME + timedelta(hours=1))
    await run_all(env, old, upto=5)

    await env.process(new[8])

    state = await env.state(replay_id)
    assert state is not None and state.run_id == new[0].run_id and state.last_sequence == 8
    assert (await env.state_events())[-1].event_type == "STATE_REBUILT"


async def test_rebuild_fails_loudly_when_the_stored_timeline_is_too_short(
    state_env: StateEnv,
) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    from dataclasses import replace

    with pytest.raises(RaceStateRebuildError):
        await env.process(replace(events[5], sequence=500))
    assert await env.state(replay_id) is None


async def test_missing_session_data_is_an_explicit_error(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    event = env.events(replay_id, run_id)[0]
    from dataclasses import replace

    with pytest.raises(RaceSeedError):
        await env.process(replace(event, session_id=uuid4()))


# -- state events ----------------------------------------------------------------------------


async def test_state_events_carry_identity_and_are_decodable(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=8)

    published = await env.state_events()
    assert published
    raw_ids = {e.event_id for e in events}
    for state_event in published:
        assert state_event.replay_id == replay_id and state_event.run_id == run_id
        assert state_event.session_id == env.race.session_id
        assert state_event.event_id not in raw_ids  # distinct from the raw event id
        assert state_event.event_type in {t.value for t in StateEventType}
        source = events[state_event.sequence]
        assert state_event.payload["source_event_id"] == str(source.event_id)
        assert state_event.payload["source_event_type"] == source.event_type
        assert state_event.payload["last_sequence"] == state_event.sequence
    assert len({e.event_id for e in published}) == len(published)
    # Raw sequence 5 (HAM lap 1 + position) produced an incremental delta.
    update = next(e for e in published if e.sequence == 4)
    assert update.event_type == "STATE_UPDATED" and "state" not in update.payload
    assert ChangeKind.LAP_COMPLETED.value in update.payload["kinds"]
    assert [d["abbreviation"] for d in update.payload["changes"]["drivers"]] == ["HAM"]


async def test_events_without_an_effect_publish_nothing(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=7)
    published = len(await env.state_events())
    assert events[6].event_type == "POSITION_CHANGED"  # already applied via LAP_COMPLETED

    # Seq 6 was a no-op change, so nothing was published for it.
    assert all(e.sequence != 6 for e in await env.state_events())
    assert published >= 1


# -- snapshots -------------------------------------------------------------------------------


async def test_snapshots_are_written_at_initial_periodic_and_final_points_only(
    state_env: StateEnv,
) -> None:
    env = state_env  # snapshot_every_laps=2, 5 laps
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)

    await run_all(env, events)

    rows = await snapshot_rows(env, replay_id)
    assert [r.trigger for r in rows] == ["INITIAL", "PERIODIC", "PERIODIC", "FINAL"]
    assert [r.sequence for r in rows] == [0, 9, 18, len(events) - 1]
    assert len(rows) < len(events)
    final = RaceState.model_validate(rows[-1].payload)
    live = await env.state(replay_id)
    assert live is not None
    assert final.phase is RacePhase.COMPLETED
    assert final.logical_dump() == live.logical_dump()
    assert rows[1].current_lap == 3 and rows[1].run_id == run_id


async def test_duplicate_snapshot_writes_are_harmless(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await env.process(events[0])
    state = await env.state(replay_id)
    assert state is not None

    from app.race_state.snapshots import DatabaseSnapshotSink

    sink = DatabaseSnapshotSink(env.factory)
    await sink.save(state, SnapshotTrigger.INITIAL)
    await sink.save(state, SnapshotTrigger.INITIAL)

    assert len(await snapshot_rows(env, replay_id)) == 1


async def test_periodic_snapshot_failure_does_not_stop_processing(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    failing = FailingSnapshots(fail_for={SnapshotTrigger.INITIAL, SnapshotTrigger.PERIODIC})
    processor = env.make_processor(failing)

    for event in events[:-1]:
        await env.process(event, processor)

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == len(events) - 2
    assert failing.saved == []


async def test_final_snapshot_failure_raises_and_commits_nothing(state_env: StateEnv) -> None:
    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    failing = FailingSnapshots(fail_for={SnapshotTrigger.FINAL})
    processor = env.make_processor(failing)
    for event in events[:-1]:
        await env.process(event, processor)
    published = len(await env.state_events())

    with pytest.raises(SnapshotPersistError):
        await env.process(events[-1], processor)

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == len(events) - 2
    assert state.phase is not RacePhase.COMPLETED
    assert len(await env.state_events()) == published

    # The retry (database back) completes the run.
    await env.process(events[-1])
    final = await env.state(replay_id)
    assert final is not None and final.phase is RacePhase.COMPLETED
    assert (await snapshot_rows(env, replay_id))[-1].trigger == "FINAL"


# -- multiple replays --------------------------------------------------------------------------


async def test_interleaved_replays_of_the_same_session_do_not_leak(state_env: StateEnv) -> None:
    env = state_env
    replay_a = await make_replay(env.factory, env.race.session_id)
    replay_b = await make_replay(env.factory, env.race.session_id)
    events_a = env.events(replay_a, uuid4())
    events_b = env.events(replay_b, uuid4())

    # Replay A runs ahead of B, their events interleaved.
    for index, a in enumerate(events_a[:14]):
        await env.process(a)
        if index < 6:
            await env.process(events_b[index])
    state_a, state_b = await env.state(replay_a), await env.state(replay_b)
    assert state_a is not None and state_b is not None
    assert state_a.replay_id == replay_a and state_b.replay_id == replay_b
    assert state_a.last_sequence == 13 and state_b.last_sequence == 5
    assert state_a.run_id != state_b.run_id
    assert env.store.key(replay_a) != env.store.key(replay_b)

    tagged = {(e.replay_id, e.run_id) for e in await env.state_events()}
    assert tagged == {(replay_a, events_a[0].run_id), (replay_b, events_b[0].run_id)}

    await run_all(env, events_b[6:])
    final_b = await env.state(replay_b)
    expected = await reduce_independently(env, events_b)
    assert final_b is not None and final_b.logical_dump() == expected.logical_dump()
    again_a = await env.state(replay_a)
    assert again_a is not None and again_a.last_sequence == 13


async def test_concurrent_workers_cannot_both_apply_the_same_sequence(
    state_env: StateEnv,
) -> None:
    import asyncio

    env = state_env
    replay_id, run_id = await make_replay(env.factory, env.race.session_id), uuid4()
    events = env.events(replay_id, run_id)
    await run_all(env, events, upto=3)

    other = RaceStateStore(env.redis, stream_config=env.stream_config, config=env.state_config)
    worker_two = env.make_processor()
    worker_two._store = other  # a second worker with its own store object
    results = await asyncio.gather(
        env.process(events[3]), env.process(events[3], worker_two), return_exceptions=True
    )

    state = await env.state(replay_id)
    assert state is not None and state.last_sequence == 3
    assert all(r is None or isinstance(r, Exception) for r in results)
    updates = [e for e in await env.state_events() if e.sequence == 3]
    assert len(updates) <= 1  # published at most once
