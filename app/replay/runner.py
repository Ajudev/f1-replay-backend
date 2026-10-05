"""Per-replay execution: releases a loaded timeline according to a virtual clock.

A ``ReplayRunner`` owns one replay's clock, event pointer and background task.
It never touches the database; the replay service persists its snapshots.
All mutation happens on the event loop thread, so command methods and the
emission loop interleave only at ``await`` points.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from app.domain.enums import EventType, ReplayStatus
from app.replay.clock import ReplayTimer, VirtualRaceClock
from app.replay.errors import ReplayTimelineUnavailableError
from app.replay.events import ReplayEvent, ReplayEventSink
from app.replay.state import TERMINAL_STATUSES, ReplayCommand, ReplayState, transition

logger = logging.getLogger(__name__)

FinishedCallback = Callable[["ReplayRunner"], Awaitable[None]]

# A sink that never yields must not monopolize the event loop during a large
# already-due batch: the loop yields once per this many emitted events.
YIELD_EVERY_EVENTS = 100


@dataclass(frozen=True, slots=True)
class TimelineEntry:
    """One loaded timeline event, as the runner needs it."""

    sequence: int
    event_type: EventType
    race_time_ms: int
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None
    payload: dict[str, Any]


def check_timeline(entries: Sequence[TimelineEntry]) -> None:
    """Reject timelines the runner cannot replay safely (no re-sorting here)."""
    if not entries:
        raise ReplayTimelineUnavailableError("The session timeline has no events")
    previous_time = -1
    for index, entry in enumerate(entries):
        if entry.sequence != index:
            raise ReplayTimelineUnavailableError(
                f"Timeline sequences are not contiguous at position {index} "
                f"(found {entry.sequence}); regenerate the timeline"
            )
        if entry.race_time_ms < previous_time:
            raise ReplayTimelineUnavailableError(
                f"Timeline is not in chronological order at sequence {entry.sequence}; "
                "regenerate the timeline"
            )
        previous_time = entry.race_time_ms


def _total_laps(entries: Sequence[TimelineEntry]) -> int | None:
    laps = [
        e.lap_number
        for e in entries
        if e.event_type is EventType.LAP_COMPLETED and e.lap_number is not None
    ]
    return max(laps) if laps else None


def _now() -> datetime:
    return datetime.now(UTC)


class ReplayRunner:
    """Replays one ordered timeline. Created per run; restart builds a new runner."""

    def __init__(
        self,
        state: ReplayState,
        entries: Sequence[TimelineEntry],
        *,
        sink: ReplayEventSink,
        timer: ReplayTimer,
        on_finished: FinishedCallback,
    ) -> None:
        check_timeline(entries)
        self._entries = entries
        self._sink = sink
        self._timer = timer
        self._on_finished = on_finished
        self._clock = VirtualRaceClock(timer.monotonic, speed=float(state.playback_speed))
        self._wake = asyncio.Event()
        self._stop_requested = False
        self._loop_exited = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._index = 0
        self._leader_laps = 0
        self._state = replace(
            state,
            current_race_time_ms=0,
            current_sequence=None,
            current_lap=None,
            total_events=len(entries),
            total_laps=_total_laps(entries),
            paused_at=None,
            ended_at=None,
            status_reason=None,
        )

    @property
    def replay_id(self) -> UUID:
        return self._state.replay_id

    @property
    def status(self) -> ReplayStatus:
        return self._state.status

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    def snapshot(self) -> ReplayState:
        """Current state with the live clock position."""
        return replace(self._state, current_race_time_ms=self._clock.now_ms())

    # -- commands (synchronous: callers hold the replay's lock) -----------------

    def prepare(self, command: ReplayCommand = ReplayCommand.START) -> None:
        """Enter RUNNING (start or restart) without emitting anything yet.

        Lets the caller persist the RUNNING state before ``launch`` so no event is
        published for a replay whose start was never recorded.
        """
        if self._task is not None:
            raise RuntimeError("Replay runner already launched")
        status = transition(self._state.status, command)
        self._state = replace(self._state, status=status, started_at=_now())

    def launch(self) -> None:
        """Start the virtual clock and the emission task."""
        if self._task is not None or self._state.status is not ReplayStatus.RUNNING:
            raise RuntimeError("Replay runner must be prepared exactly once before launch")
        self._clock.start()
        self._task = asyncio.create_task(self._run(), name=f"replay-{self.replay_id}")

    def pause(self) -> None:
        status = transition(self._state.status, ReplayCommand.PAUSE)
        self._clock.pause()
        self._state = replace(self._state, status=status, paused_at=_now())
        self._wake.set()

    def resume(self) -> None:
        status = transition(self._state.status, ReplayCommand.RESUME)
        self._clock.resume()
        self._state = replace(self._state, status=status, paused_at=None)
        self._wake.set()

    def set_speed(self, speed: Decimal) -> None:
        self._clock.set_speed(float(speed))
        self._state = replace(self._state, playback_speed=speed)
        self._wake.set()  # recompute the wait for the next event

    async def stop(self, *, reason: str | None = None, timeout: float = 5.0) -> None:
        """Mark STOPPED and wait for the emission loop to exit.

        Cooperative first (an in-flight ``publish`` completes), then cancels the
        task if it does not exit within ``timeout``. ``on_finished`` is not called.
        """
        status = transition(self._state.status, ReplayCommand.STOP)
        if self._clock.running:
            self._clock.pause()
        self._state = replace(self._state, status=status, ended_at=_now(), status_reason=reason)
        await self.halt(timeout=timeout)

    async def halt(self, *, timeout: float = 5.0) -> None:
        """Make the emission loop exit without changing the replay status.

        Waits only for the loop, not for ``on_finished`` (which may be queued on
        the lock the caller holds).
        """
        self._stop_requested = True
        self._wake.set()
        task = self._task
        if task is None or task.done() or self._loop_exited.is_set():
            return
        try:
            await asyncio.wait_for(asyncio.shield(self._loop_exited.wait()), timeout)
        except TimeoutError:
            logger.warning(
                "Replay loop did not exit in %.1fs; cancelling replay_id=%s",
                timeout,
                self.replay_id,
            )
            task.cancel()
            await asyncio.wait({task})

    # -- emission loop ------------------------------------------------------------

    async def _run(self) -> None:
        try:
            finished = await self._loop()
        except Exception as exc:
            finished = self._fail(exc)
        finally:
            self._loop_exited.set()
        if finished:
            await self._on_finished(self)

    async def _loop(self) -> bool:
        """Emit events as they come due. True when the replay finished on its own."""
        entries = self._entries
        total = len(entries)
        while True:
            self._wake.clear()
            if self._stop_requested:
                return False
            if self._state.status is not ReplayStatus.RUNNING:
                await self._timer.wait(self._wake, None)
                continue

            now_ms = self._clock.now_ms()
            # Emit every already-due event back to back, re-checking control state
            # after each await so a pause or stop takes effect between events.
            emitted = 0
            while (
                self._index < total
                and entries[self._index].race_time_ms <= now_ms
                and self._state.status is ReplayStatus.RUNNING
                and not self._stop_requested
            ):
                await self._emit(entries[self._index])
                emitted += 1
                if emitted % YIELD_EVERY_EVENTS == 0:
                    await asyncio.sleep(0)  # let other tasks (e.g. pause/stop) run

            if self._stop_requested or self._state.status is not ReplayStatus.RUNNING:
                continue
            if self._index >= total:
                self._complete()
                return True

            delay = self._clock.real_seconds_until(entries[self._index].race_time_ms)
            if delay is None or delay <= 0:
                continue
            await self._timer.wait(self._wake, delay)

    async def _emit(self, entry: TimelineEntry) -> None:
        await self._sink.publish(
            ReplayEvent(
                replay_id=self._state.replay_id,
                session_id=self._state.session_id,
                sequence=entry.sequence,
                event_type=entry.event_type,
                race_time_ms=entry.race_time_ms,
                lap_number=entry.lap_number,
                driver_id=entry.driver_id,
                driver_abbreviation=entry.driver_abbreviation,
                payload=entry.payload,
            )
        )
        # Advance only after a successful publish, so a failure never skips an event
        # and nothing published is counted twice.
        self._index += 1
        current_lap = self._state.current_lap
        if entry.event_type is EventType.RACE_STARTED and current_lap is None:
            current_lap = 1
        elif (
            entry.event_type is EventType.LAP_COMPLETED
            and entry.lap_number is not None
            and entry.lap_number > self._leader_laps
        ):
            self._leader_laps = entry.lap_number
            total_laps = self._state.total_laps or entry.lap_number
            current_lap = min(entry.lap_number + 1, total_laps)
        self._state = replace(self._state, current_sequence=entry.sequence, current_lap=current_lap)

    def _complete(self) -> None:
        status = transition(self._state.status, ReplayCommand.COMPLETE)
        final_ms = self._entries[-1].race_time_ms
        self._clock.reset(final_ms)
        self._state = replace(self._state, status=status, ended_at=_now())
        logger.info(
            "Replay completed replay_id=%s session_id=%s race_time_ms=%d lap=%s events=%d",
            self.replay_id,
            self._state.session_id,
            final_ms,
            self._state.current_lap,
            self._index,
        )

    def _fail(self, exc: Exception) -> bool:
        """Record a loop failure. False if the replay was already stopped."""
        sequence = self._entries[self._index].sequence if self._index < len(self._entries) else None
        if self._state.status in TERMINAL_STATUSES:
            logger.warning(
                "Replay loop error after %s replay_id=%s sequence=%s: %s",
                self._state.status.value,
                self.replay_id,
                sequence,
                exc,
            )
            return False
        if self._clock.running:
            self._clock.pause()
        reason = f"Replay failed at sequence {sequence}: {type(exc).__name__}: {exc}"
        self._state = replace(
            self._state,
            status=transition(self._state.status, ReplayCommand.FAIL),
            ended_at=_now(),
            status_reason=reason[:500],
        )
        logger.error(
            "Replay failed replay_id=%s session_id=%s sequence=%s race_time_ms=%d lap=%s",
            self.replay_id,
            self._state.session_id,
            sequence,
            self._clock.now_ms(),
            self._state.current_lap,
            exc_info=exc,
        )
        return True
