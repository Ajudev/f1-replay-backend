"""Replay runner: ordered emission on the virtual clock, with fake time."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.enums import EventType, ReplayStatus
from app.replay.errors import InvalidReplayTransitionError, ReplayTimelineUnavailableError
from app.replay.events import ReplayEventSink
from app.replay.runner import YIELD_EVERY_EVENTS, ReplayRunner, TimelineEntry, check_timeline
from app.replay.state import ReplayCommand, ReplayState
from tests.replay.fakes import CollectingSink, FailingSink, GatedSink, ManualTimer, settle


def entry(seq: int, t: int, kind: EventType, lap: int | None = None, drv: str | None = None):
    return TimelineEntry(
        sequence=seq,
        event_type=kind,
        race_time_ms=t,
        lap_number=lap,
        driver_id=None,
        driver_abbreviation=drv,
        payload={"seq": seq},
    )


L = EventType.LAP_COMPLETED
TIMELINE = [
    entry(0, 0, EventType.RACE_STARTED),
    entry(1, 0, EventType.TRACK_STATUS_CHANGED),
    entry(2, 90_000, L, 1, "VER"),
    entry(3, 90_000, EventType.POSITION_CHANGED, 1, "VER"),
    entry(4, 90_500, L, 1, "HAM"),
    entry(5, 180_000, L, 2, "VER"),
    entry(6, 180_500, L, 2, "HAM"),
]


def initial_state(speed: str = "1") -> ReplayState:
    return ReplayState(
        replay_id=uuid4(),
        session_id=uuid4(),
        status=ReplayStatus.CREATED,
        playback_speed=Decimal(speed),
        current_race_time_ms=0,
        current_sequence=None,
        current_lap=None,
        total_events=None,
        total_laps=None,
        started_at=None,
        paused_at=None,
        ended_at=None,
        status_reason=None,
    )


class Harness:
    def __init__(self) -> None:
        self.timer = ManualTimer()
        self.finished: list[ReplayRunner] = []
        self.runners: list[ReplayRunner] = []

    async def _on_finished(self, runner: ReplayRunner) -> None:
        self.finished.append(runner)

    def start(
        self,
        sink: ReplayEventSink,
        *,
        speed: str = "1",
        entries: list[TimelineEntry] = TIMELINE,
    ) -> ReplayRunner:
        runner = ReplayRunner(
            initial_state(speed),
            entries,
            sink=sink,
            timer=self.timer,
            on_finished=self._on_finished,
        )
        runner.prepare(ReplayCommand.START)
        runner.launch()
        self.runners.append(runner)
        return runner


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    h = Harness()
    yield h
    for runner in h.runners:
        await runner.halt(timeout=0.1)
        if runner.task is not None:
            await asyncio.wait({runner.task})


async def test_emits_in_timeline_order_exactly_once(harness: Harness) -> None:
    sink = CollectingSink()
    runner = harness.start(sink)
    await settle()
    # t=0: both equal-timestamp events, in sequence order, nothing early.
    assert sink.sequences() == [0, 1]
    assert runner.snapshot().current_lap == 1

    await harness.timer.advance(89.999)
    assert sink.sequences() == [0, 1]
    await harness.timer.advance(0.001)
    assert sink.sequences() == [0, 1, 2, 3]
    await harness.timer.advance(100)
    assert sink.sequences() == list(range(7))
    assert runner.status is ReplayStatus.COMPLETED
    assert all(e.replay_id == runner.replay_id for e in sink.events)


async def test_high_speed_emits_all_due_events_in_order_without_waits(harness: Harness) -> None:
    sink = CollectingSink()
    runner = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(10)  # 200 s of race time: everything is due at once
    assert sink.sequences() == list(range(7))
    assert runner.status is ReplayStatus.COMPLETED


async def test_completion_sets_final_state_and_task_ends(harness: Harness) -> None:
    sink = CollectingSink()
    runner = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(60)
    state = runner.snapshot()
    assert state.status is ReplayStatus.COMPLETED
    assert state.current_race_time_ms == 180_500  # final event time, not the clock overshoot
    assert state.current_sequence == 6 and state.emitted_event_count == 7
    assert state.current_lap == 2 and state.total_laps == 2
    assert state.ended_at is not None
    assert runner.task is not None and runner.task.done()
    assert harness.finished == [runner]

    await harness.timer.advance(1_000)
    assert runner.snapshot().current_race_time_ms == 180_500


async def test_pause_freezes_emission_and_resume_does_not_repeat(harness: Harness) -> None:
    sink = CollectingSink()
    runner = harness.start(sink)
    await settle()
    await harness.timer.advance(90)
    assert sink.sequences() == [0, 1, 2, 3]

    runner.pause()
    paused = runner.snapshot()
    assert paused.status is ReplayStatus.PAUSED and paused.paused_at is not None
    assert paused.current_race_time_ms == 90_000
    await harness.timer.advance(500)
    assert sink.sequences() == [0, 1, 2, 3]
    assert runner.snapshot().current_race_time_ms == 90_000

    runner.resume()
    await settle()
    assert runner.snapshot().paused_at is None
    await harness.timer.advance(0.5)
    assert sink.sequences() == [0, 1, 2, 3, 4]
    await harness.timer.advance(90)
    assert sink.sequences() == list(range(7))


@pytest.mark.parametrize(("first", "second"), [("1", "10"), ("10", "2")])
async def test_speed_change_preserves_position(harness: Harness, first: str, second: str) -> None:
    sink = CollectingSink()
    runner = harness.start(sink, speed=first)
    await settle()
    real_to_80s = 80 / float(first)
    await harness.timer.advance(real_to_80s)
    assert runner.snapshot().current_race_time_ms == 80_000

    runner.set_speed(Decimal(second))
    await settle()
    assert runner.snapshot().current_race_time_ms == 80_000
    assert runner.snapshot().playback_speed == Decimal(second)

    # Next event at 90 s is now 10 race seconds away at the new speed.
    await harness.timer.advance(10 / float(second) - 0.001)
    assert sink.sequences() == [0, 1]
    await harness.timer.advance(0.001)
    assert sink.sequences() == [0, 1, 2, 3]


async def test_speed_change_while_paused(harness: Harness) -> None:
    sink = CollectingSink()
    runner = harness.start(sink)
    await settle()
    await harness.timer.advance(50)
    runner.pause()
    runner.set_speed(Decimal("20"))
    await harness.timer.advance(100)
    assert runner.snapshot().current_race_time_ms == 50_000
    runner.resume()
    await settle()
    await harness.timer.advance(2)  # 40 race seconds at 20x
    assert runner.snapshot().current_race_time_ms == 90_000
    assert sink.sequences() == [0, 1, 2, 3]


async def test_pause_during_publish_stops_after_inflight_event(harness: Harness) -> None:
    sink = GatedSink(gate_at=2)
    runner = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(10)  # everything is due
    await sink.entered.wait()
    runner.pause()
    sink.release()
    await settle()
    # The in-flight event completes; nothing after it is emitted while paused.
    assert sink.sequences() == [0, 1, 2]
    assert runner.snapshot().current_sequence == 2
    await harness.timer.advance(100)
    assert sink.sequences() == [0, 1, 2]

    runner.resume()
    await settle()
    assert sink.sequences() == list(range(7))
    assert runner.status is ReplayStatus.COMPLETED


async def test_stop_during_publish_waits_for_inflight_event(harness: Harness) -> None:
    sink = GatedSink(gate_at=2)
    runner = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(10)
    await sink.entered.wait()
    stop = asyncio.create_task(runner.stop())
    await settle()
    assert not stop.done()
    sink.release()
    await stop
    state = runner.snapshot()
    assert state.status is ReplayStatus.STOPPED and state.ended_at is not None
    assert sink.sequences() == [0, 1, 2] and state.current_sequence == 2
    assert harness.finished == []  # stop is not a natural finish
    await settle()
    assert runner.task is not None and runner.task.done()


async def test_stop_while_paused(harness: Harness) -> None:
    sink = CollectingSink()
    runner = harness.start(sink)
    await settle()
    runner.pause()
    await runner.stop()
    assert runner.status is ReplayStatus.STOPPED
    await settle()
    assert runner.task is not None and runner.task.done()
    with pytest.raises(InvalidReplayTransitionError):
        runner.resume()


async def test_stop_cancels_hung_publish_after_timeout(harness: Harness) -> None:
    sink = GatedSink(gate_at=0)  # never released
    runner = harness.start(sink)
    await sink.entered.wait()
    await runner.stop(timeout=0.01)
    assert runner.status is ReplayStatus.STOPPED
    assert runner.task is not None and runner.task.cancelled()
    assert sink.sequences() == []


async def test_publisher_failure_marks_failed_without_skipping(harness: Harness) -> None:
    sink = FailingSink(fail_at=4)
    runner = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(10)
    state = runner.snapshot()
    assert state.status is ReplayStatus.FAILED
    assert sink.sequences() == [0, 1, 2, 3]
    assert state.current_sequence == 3
    assert state.status_reason is not None and "sequence 4" in state.status_reason
    assert harness.finished == [runner]
    assert runner.task is not None and runner.task.done()


async def test_commands_after_completion_are_rejected(harness: Harness) -> None:
    runner = harness.start(CollectingSink(), speed="20")
    await settle()
    await harness.timer.advance(60)
    for command in (runner.pause, runner.resume):
        with pytest.raises(InvalidReplayTransitionError):
            command()
    with pytest.raises(InvalidReplayTransitionError):
        await runner.stop()


async def test_current_lap_tracks_leader(harness: Harness) -> None:
    entries = [
        entry(0, 0, EventType.RACE_STARTED),
        entry(1, 90_000, L, 1, "VER"),
        entry(2, 95_000, L, 1, "HAM"),
        entry(3, 180_000, L, 2, "VER"),
        entry(4, 185_000, L, 2, "HAM"),
        entry(5, 270_000, L, 3, "VER"),
        entry(6, 290_000, L, 2, "SAR"),  # lapped car must not move the race lap back
    ]
    runner = harness.start(CollectingSink(), entries=entries)
    await settle()
    laps: list[int | None] = []
    for seconds in (0, 90, 5, 85, 5, 85, 20):
        await harness.timer.advance(seconds)
        laps.append(runner.snapshot().current_lap)
    assert laps == [1, 2, 2, 3, 3, 3, 3]  # capped at total_laps (3)
    assert runner.snapshot().total_laps == 3


async def test_independent_runners_do_not_interfere(harness: Harness) -> None:
    sink = CollectingSink()
    slow = harness.start(sink, speed="1")
    fast = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(4.5)  # fast: 90 s race time; slow: 4.5 s
    assert sink.sequences(fast.replay_id) == [0, 1, 2, 3]
    assert sink.sequences(slow.replay_id) == [0, 1]
    slow.pause()
    await harness.timer.advance(10)
    assert fast.status is ReplayStatus.COMPLETED
    assert sink.sequences(fast.replay_id) == list(range(7))
    assert slow.snapshot().current_race_time_ms == 4_500
    assert slow.status is ReplayStatus.PAUSED


@pytest.mark.parametrize(
    "make",
    [
        lambda: [],
        lambda: [entry(0, 0, EventType.RACE_STARTED), entry(2, 5, L, 1)],
        lambda: [entry(0, 10, EventType.RACE_STARTED), entry(1, 5, L, 1)],
    ],
)
def test_unreplayable_timelines_rejected(make: Callable[[], list[TimelineEntry]]) -> None:
    with pytest.raises(ReplayTimelineUnavailableError):
        check_timeline(make())


async def test_large_due_batch_yields_so_pause_takes_effect_mid_batch(harness: Harness) -> None:
    class NonYieldingSink:
        def __init__(self) -> None:
            self.events: list[int] = []

        async def publish(self, event) -> None:  # never awaits
            self.events.append(event.sequence)

    batch = [entry(i, 0, EventType.POSITION_CHANGED) for i in range(YIELD_EVERY_EVENTS * 3)]
    sink = NonYieldingSink()
    runner = harness.start(sink, entries=batch)  # type: ignore[arg-type]
    asyncio.get_running_loop().call_soon(runner.pause)  # runs after the task's first step
    await settle()
    # The runner task was scheduled first: it emits one chunk, yields, then sees the pause.
    assert runner.status is ReplayStatus.PAUSED
    assert sink.events == list(range(YIELD_EVERY_EVENTS))
    assert runner.snapshot().current_sequence == YIELD_EVERY_EVENTS - 1

    runner.resume()
    await settle()
    assert sink.events == list(range(len(batch)))  # order preserved, nothing repeated
    assert runner.status is ReplayStatus.COMPLETED


async def test_publish_failing_after_pause_fails_from_paused(harness: Harness) -> None:
    class RaisesAfterPause(CollectingSink):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.proceed = asyncio.Event()

        async def publish(self, event) -> None:
            if event.sequence == 2:
                self.entered.set()
                await self.proceed.wait()
                raise ConnectionError("sink unavailable")
            await super().publish(event)

    sink = RaisesAfterPause()
    runner = harness.start(sink, speed="20")
    await settle()
    await harness.timer.advance(10)
    await sink.entered.wait()
    runner.pause()
    assert runner.status is ReplayStatus.PAUSED
    sink.proceed.set()
    await settle()
    state = runner.snapshot()
    assert state.status is ReplayStatus.FAILED  # FAIL is reachable from PAUSED
    assert state.current_sequence == 1
    assert harness.finished == [runner]
