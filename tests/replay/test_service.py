"""Replay service against SQLite with a real generated timeline and fake time."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import EventType, ReplayStatus
from app.models import RaceEvent, ReplaySession, SessionTimeline
from app.replay.errors import (
    InvalidPlaybackSpeedError,
    InvalidReplayTransitionError,
    ReplayNotFoundError,
    ReplayTimelineUnavailableError,
)
from app.replay.service import INTERRUPTED_REASON, SHUTDOWN_REASON, ReplayService
from app.services.race_queries import SessionNotFoundError
from app.timeline.repository import TimelineRepository
from tests.replay.conftest import ReplayEnv
from tests.replay.fakes import (
    CollectingSink,
    FailingSink,
    ManualTimer,
    settle,
    wait_until_idle,
)
from tests.timeline.factories import representative_race
from tests.timeline.seed import seed_source


async def persisted(factory: async_sessionmaker[AsyncSession], replay_id) -> ReplaySession:
    async with factory() as db:
        row = await db.get(ReplaySession, replay_id)
        assert row is not None
        return row


async def test_create_validates_session_timeline_and_speed(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    view = await replay_env.service.create(replay_env.race.session_id, 5)
    assert view.state.status is ReplayStatus.CREATED
    assert view.state.playback_speed == Decimal("5.00")
    assert view.race_id == replay_env.race.race_id

    with pytest.raises(SessionNotFoundError):
        await replay_env.service.create(uuid4())
    with pytest.raises(InvalidPlaybackSpeedError):
        await replay_env.service.create(replay_env.race.session_id, 0)

    no_timeline = replace(representative_race(), season=2022, round=3)
    await seed_source(session_factory, no_timeline)
    with pytest.raises(ReplayTimelineUnavailableError):
        await replay_env.service.create(no_timeline.session_id)


async def test_full_lifecycle_emits_whole_timeline_once_and_persists(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc, sink, timer = replay_env.service, replay_env.sink, replay_env.timer
    replay_id = (await svc.create(replay_env.race.session_id, 20)).state.replay_id

    started = await svc.start(replay_id)
    assert started.state.status is ReplayStatus.RUNNING
    assert started.state.total_events == replay_env.event_count
    assert started.state.total_laps == 5
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.RUNNING and row.started_at is not None

    await timer.advance(1_000)
    await wait_until_idle(svc)
    assert sink.sequences() == list(range(replay_env.event_count))
    assert sink.events[0].event_type is EventType.RACE_STARTED

    view = await svc.get(replay_id)
    assert view.state.status is ReplayStatus.COMPLETED
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.COMPLETED
    assert row.current_sequence == replay_env.event_count - 1
    assert row.current_lap == 5 and row.ended_at is not None
    assert row.current_race_time_ms == sink.events[-1].race_time_ms
    assert svc.active_replay_ids() == []


async def test_emitted_events_match_stored_timeline(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc = replay_env.service
    replay_id = (await svc.create(replay_env.race.session_id, 20)).state.replay_id
    await svc.start(replay_id)
    await replay_env.timer.advance(1_000)
    async with session_factory() as db:
        stored = await TimelineRepository(db).load_events(replay_env.race.session_id)
    emitted = [
        (e.sequence, e.event_type, e.race_time_ms, e.driver_abbreviation)
        for e in replay_env.sink.events
    ]
    assert emitted == [
        (e.sequence, e.event_type, e.race_time_ms, e.driver_abbreviation) for e in stored
    ]


async def test_pause_resume_persist_position(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc, sink, timer = replay_env.service, replay_env.sink, replay_env.timer
    replay_id = (await svc.create(replay_env.race.session_id)).state.replay_id
    await svc.start(replay_id)
    await timer.advance(100)

    paused = await svc.pause(replay_id)
    emitted = len(sink.events)
    assert paused.state.status is ReplayStatus.PAUSED
    assert paused.state.current_race_time_ms == 100_000
    assert paused.state.emitted_event_count == emitted
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.PAUSED and row.current_race_time_ms == 100_000
    assert row.current_sequence == emitted - 1 and row.paused_at is not None

    await timer.advance(10_000)
    assert len(sink.events) == emitted
    assert (await svc.get(replay_id)).state.current_race_time_ms == 100_000

    resumed = await svc.resume(replay_id)
    assert resumed.state.status is ReplayStatus.RUNNING
    assert resumed.state.current_race_time_ms == 100_000
    assert (await persisted(session_factory, replay_id)).paused_at is None
    await timer.advance(1_000)
    assert sink.sequences() == list(range(replay_env.event_count))


async def test_invalid_commands_raise_and_leave_state_unchanged(replay_env: ReplayEnv) -> None:
    svc = replay_env.service
    replay_id = (await svc.create(replay_env.race.session_id)).state.replay_id
    for command in (svc.pause, svc.resume, svc.stop, svc.restart):
        with pytest.raises(InvalidReplayTransitionError):
            await command(replay_id)
    await svc.start(replay_id)
    with pytest.raises(InvalidReplayTransitionError):
        await svc.start(replay_id)
    with pytest.raises(InvalidReplayTransitionError):
        await svc.resume(replay_id)
    await svc.pause(replay_id)
    with pytest.raises(InvalidReplayTransitionError):
        await svc.pause(replay_id)
    assert (await svc.get(replay_id)).state.status is ReplayStatus.PAUSED

    with pytest.raises(ReplayNotFoundError):
        await svc.get(uuid4())
    with pytest.raises(ReplayNotFoundError):
        await svc.start(uuid4())


async def test_concurrent_starts_launch_one_worker(replay_env: ReplayEnv) -> None:
    svc = replay_env.service
    replay_id = (await svc.create(replay_env.race.session_id)).state.replay_id
    results = await asyncio.gather(
        *(svc.start(replay_id) for _ in range(5)), return_exceptions=True
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    errors = [r for r in results if isinstance(r, BaseException)]
    assert len(ok) == 1
    assert len(errors) == 4 and all(isinstance(e, InvalidReplayTransitionError) for e in errors)
    await settle()
    assert replay_env.sink.sequences() == [0, 1]  # one loop: t=0 events once
    assert svc.active_replay_ids() == [replay_id]


async def test_stop_then_restart_replays_from_beginning(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc, sink, timer = replay_env.service, replay_env.sink, replay_env.timer
    replay_id = (await svc.create(replay_env.race.session_id, 10)).state.replay_id
    await svc.start(replay_id)
    await timer.advance(10)
    first_run = sink.sequences()

    stopped = await svc.stop(replay_id)
    assert stopped.state.status is ReplayStatus.STOPPED
    assert stopped.state.ended_at is not None
    assert svc.active_replay_ids() == []
    with pytest.raises(InvalidReplayTransitionError):
        await svc.stop(replay_id)
    with pytest.raises(InvalidReplayTransitionError):
        await svc.resume(replay_id)
    await timer.advance(100)
    assert sink.sequences() == first_run

    restarted = await svc.restart(replay_id)
    s = restarted.state
    assert s.status is ReplayStatus.RUNNING
    assert s.playback_speed == Decimal("10.00")  # speed retained
    assert s.ended_at is None and s.paused_at is None and s.status_reason is None
    await settle()
    assert sink.sequences() == first_run + [0, 1]  # intentionally emitted again
    await timer.advance(1_000)
    assert sink.sequences() == first_run + list(range(replay_env.event_count))

    async with session_factory() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(RaceEvent)
            .where(RaceEvent.session_id == replay_env.race.session_id)
        )
    assert count == replay_env.event_count  # restart never rewrote the timeline


async def test_restart_while_running_and_after_completion(replay_env: ReplayEnv) -> None:
    svc, sink, timer = replay_env.service, replay_env.sink, replay_env.timer
    replay_id = (await svc.create(replay_env.race.session_id)).state.replay_id
    await svc.start(replay_id)
    await timer.advance(100)
    before = len(sink.events)

    restarted = await svc.restart(replay_id)
    assert restarted.state.current_race_time_ms == 0
    assert restarted.state.current_sequence in (None, 0, 1)
    await timer.advance(100)
    # Exactly one loop after restart: second pass emits the same prefix once.
    assert sink.sequences()[before:] == sink.sequences()[:before]

    await svc.change_speed(replay_id, 20)
    await timer.advance(1_000)
    await settle()
    assert (await svc.get(replay_id)).state.status is ReplayStatus.COMPLETED
    again = await svc.restart(replay_id)
    assert again.state.status is ReplayStatus.RUNNING


async def test_speed_change_preserves_position_and_persists(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc, timer = replay_env.service, replay_env.timer
    replay_id = (await svc.create(replay_env.race.session_id)).state.replay_id
    await svc.start(replay_id)
    await timer.advance(30)
    changed = await svc.change_speed(replay_id, 10)
    assert changed.state.current_race_time_ms == 30_000
    assert changed.state.playback_speed == Decimal("10.00")
    assert (await persisted(session_factory, replay_id)).playback_speed == Decimal("10.00")
    await timer.advance(1)
    assert (await svc.get(replay_id)).state.current_race_time_ms == 40_000

    with pytest.raises(InvalidPlaybackSpeedError):
        await svc.change_speed(replay_id, -2)
    same = await svc.change_speed(replay_id, 10)
    assert same.state.current_race_time_ms == 40_000

    created = (await svc.create(replay_env.race.session_id)).state.replay_id
    assert (await svc.change_speed(created, 2)).state.playback_speed == Decimal("2.00")


async def test_two_replays_are_isolated(replay_env: ReplayEnv) -> None:
    svc, sink, timer = replay_env.service, replay_env.sink, replay_env.timer
    a = (await svc.create(replay_env.race.session_id, 5)).state.replay_id
    b = (await svc.create(replay_env.other_race.session_id, 10)).state.replay_id
    await svc.start(a)
    await svc.start(b)
    await timer.advance(10)

    va, vb = await svc.get(a), await svc.get(b)
    assert va.state.current_race_time_ms == 50_000
    assert vb.state.current_race_time_ms == 100_000

    await svc.pause(a)
    await timer.advance(5)
    va, vb = await svc.get(a), await svc.get(b)
    assert va.state.status is ReplayStatus.PAUSED and va.state.current_race_time_ms == 50_000
    assert vb.state.status is ReplayStatus.RUNNING and vb.state.current_race_time_ms == 150_000
    assert all(e.session_id == replay_env.race.session_id for e in sink.events if e.replay_id == a)
    assert all(
        e.session_id == replay_env.other_race.session_id for e in sink.events if e.replay_id == b
    )
    assert sink.sequences(a) == sorted(set(sink.sequences(a)))
    assert sink.sequences(b) == sorted(set(sink.sequences(b)))


async def test_publisher_failure_persists_failed_and_allows_restart(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    sink, timer = FailingSink(fail_at=3), ManualTimer()
    svc = ReplayService(session_factory, sink=sink, timer=timer, stop_timeout=0.5)
    try:
        replay_id = (await svc.create(replay_env.race.session_id, 20)).state.replay_id
        await svc.start(replay_id)
        await timer.advance(1_000)
        await wait_until_idle(svc)
        row = await persisted(session_factory, replay_id)
        assert row.status is ReplayStatus.FAILED
        assert row.current_sequence == 2
        assert row.status_reason and "sequence 3" in row.status_reason
        assert svc.active_replay_ids() == []
        with pytest.raises(InvalidReplayTransitionError):
            await svc.resume(replay_id)
        assert (await svc.restart(replay_id)).state.status is ReplayStatus.RUNNING
    finally:
        await svc.shutdown()


async def test_orphaned_active_replay_reported_stopped(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    replay_id = (await replay_env.service.create(replay_env.race.session_id)).state.replay_id
    await replay_env.service.start(replay_id)

    # A fresh service models a backend restart: no in-memory worker exists.
    fresh = ReplayService(session_factory, sink=CollectingSink(), timer=ManualTimer())
    view = await fresh.get(replay_id)
    assert view.state.status is ReplayStatus.STOPPED
    assert view.state.status_reason == INTERRUPTED_REASON
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.STOPPED
    with pytest.raises(InvalidReplayTransitionError):
        await fresh.resume(replay_id)


async def test_recover_interrupted_marks_active_rows_stopped(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc = replay_env.service
    running = (await svc.create(replay_env.race.session_id)).state.replay_id
    paused = (await svc.create(replay_env.race.session_id)).state.replay_id
    created = (await svc.create(replay_env.race.session_id)).state.replay_id
    await svc.start(running)
    await svc.start(paused)
    await svc.pause(paused)

    fresh = ReplayService(session_factory, sink=CollectingSink(), timer=ManualTimer())
    assert await fresh.recover_interrupted() == 2
    for rid in (running, paused):
        row = await persisted(session_factory, rid)
        assert row.status is ReplayStatus.STOPPED and row.status_reason == INTERRUPTED_REASON
    assert (await persisted(session_factory, created)).status is ReplayStatus.CREATED


async def test_shutdown_stops_and_persists_live_replays(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc = replay_env.service
    a = (await svc.create(replay_env.race.session_id)).state.replay_id
    b = (await svc.create(replay_env.other_race.session_id)).state.replay_id
    await svc.start(a)
    await svc.start(b)
    await svc.pause(b)
    await svc.shutdown()
    assert svc.active_replay_ids() == []
    for rid in (a, b):
        row = await persisted(session_factory, rid)
        assert row.status is ReplayStatus.STOPPED and row.status_reason == SHUTDOWN_REASON
    assert all(t.done() for t in asyncio.all_tasks() if t.get_name().startswith("replay-"))


async def test_command_racing_completion_sees_completed(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    svc, timer = replay_env.service, replay_env.timer
    replay_id = (await svc.create(replay_env.race.session_id, 20)).state.replay_id
    await svc.start(replay_id)
    # Complete the loop, then issue commands before the completion is persisted.
    await timer.advance(1_000)
    for command in (svc.pause, svc.stop):
        with pytest.raises(InvalidReplayTransitionError) as exc_info:
            await command(replay_id)
        assert exc_info.value.current_status is ReplayStatus.COMPLETED
    await wait_until_idle(svc)
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.COMPLETED


async def test_start_rejects_timeline_removed_after_create(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    replay_id = (await replay_env.service.create(replay_env.race.session_id)).state.replay_id
    async with session_factory() as db:
        await db.execute(
            delete(SessionTimeline).where(SessionTimeline.session_id == replay_env.race.session_id)
        )
        await db.commit()
    with pytest.raises(ReplayTimelineUnavailableError):
        await replay_env.service.start(replay_id)
    assert (await replay_env.service.get(replay_id)).state.status is ReplayStatus.CREATED
    assert replay_env.service.active_replay_ids() == []
