"""Replay application service: lifecycle commands over independent replay runners.

One instance lives for the whole application (``app.state.replay_service``).
Each replay has at most one ``ReplayRunner`` (and so one emission task) in
this process. Every command and read for a replay runs under that replay's
``asyncio.Lock``, so duplicate starts, concurrent pauses and speed changes are
serialized. While a runner is registered its in-memory state is authoritative;
PostgreSQL holds the snapshot written at each lifecycle boundary.

If a lifecycle write fails the in-memory change still stands: the replay is
remembered as unsaved (``_dirty`` while a runner exists, ``_pending_saves`` for a
terminal state whose runner is already gone) and the write is retried on the next
access. Sessions are never held open while awaiting a runner.

Single-process only: replay execution and locks are in-memory, so run the API
with one worker.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.replay.clock import ReplayTimer
from app.replay.errors import (
    ReplayNotFoundError,
    ReplayPersistenceError,
    ReplayTimelineUnavailableError,
)
from app.replay.events import ReplayEventSink
from app.replay.repository import ReplayRecord, ReplayRepository
from app.replay.runner import ReplayRunner
from app.replay.state import (
    ACTIVE_STATUSES,
    DEFAULT_PLAYBACK_SPEED,
    ReplayCommand,
    ReplayState,
    transition,
    validate_playback_speed,
)
from app.services.race_queries import SessionNotFoundError

logger = logging.getLogger(__name__)

INTERRUPTED_REASON = "Interrupted: the replay worker is no longer running (backend restarted)"
SHUTDOWN_REASON = "Stopped: backend shutdown"


@dataclass(frozen=True, slots=True)
class ReplayView:
    state: ReplayState
    race_id: UUID
    created_at: datetime


#: Called synchronously after every lifecycle change (including speed changes and the
#: replay finishing on its own). Must be fast and must not raise.
ReplayListener = Callable[[ReplayView], None]


class ReplayService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        sink: ReplayEventSink,
        timer: ReplayTimer,
        stop_timeout: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._sink = sink
        self._timer = timer
        self._stop_timeout = stop_timeout
        self._runners: dict[UUID, ReplayRunner] = {}
        # Run id of the latest runner per replay; survives the runner's retirement.
        self._last_run_ids: dict[UUID, UUID] = {}
        # Live runners whose latest in-memory state failed to persist.
        self._dirty: set[UUID] = set()
        # Terminal states (runner already unregistered) that failed to persist.
        self._pending_saves: dict[UUID, ReplayState] = {}
        # Locks disappear once no command holds or awaits them.
        self._locks: weakref.WeakValueDictionary[UUID, asyncio.Lock] = weakref.WeakValueDictionary()
        # Record of each live runner's replay, so a self-finishing run can be reported
        # without a database read.
        self._records: dict[UUID, ReplayRecord] = {}
        self._listeners: list[ReplayListener] = []

    # -- queries --------------------------------------------------------------

    def active_replay_ids(self) -> list[UUID]:
        return list(self._runners)

    def current_run_id(self, replay_id: UUID) -> UUID | None:
        """Run id of the replay's latest start/restart in this process (no database).

        Stays available after the runner retires (completed, stopped, failed); ``None`` when
        the replay has not run in this process.
        """
        return self._last_run_ids.get(replay_id)

    def live_state(self, replay_id: UUID) -> ReplayState | None:
        """In-memory state of a replay running in this process (no database, no lock);
        ``None`` when no runner is registered."""
        runner = self._runners.get(replay_id)
        return runner.snapshot() if runner is not None else None

    # -- listeners ----------------------------------------------------------------

    def add_listener(self, listener: ReplayListener) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: ReplayListener) -> None:
        with suppress(ValueError):
            self._listeners.remove(listener)

    async def get(self, replay_id: UUID) -> ReplayView:
        async with self._lock(replay_id):
            record, state = await self._current(replay_id)
            return self._view(record, state)

    # -- commands -------------------------------------------------------------

    async def create(
        self, session_id: UUID, playback_speed: Decimal | float | int = DEFAULT_PLAYBACK_SPEED
    ) -> ReplayView:
        speed = validate_playback_speed(playback_speed)
        with self._storage("create", session_id=session_id):
            async with self._session_factory() as db:
                repo = ReplayRepository(db)
                if await repo.session_race_id(session_id) is None:
                    raise SessionNotFoundError(session_id)
                if not await repo.timeline_exists(session_id):
                    raise ReplayTimelineUnavailableError(
                        f"No timeline has been generated for session {session_id}; "
                        f"POST /api/v1/sessions/{session_id}/timeline to generate it"
                    )
                replay_id = await repo.create(session_id, speed)
                await db.commit()
                record = await repo.get(replay_id)
        assert record is not None
        logger.info(
            "Replay created replay_id=%s session_id=%s race_id=%s speed=%s",
            replay_id,
            session_id,
            record.race_id,
            speed,
        )
        return self._view(record, record.state)

    async def start(self, replay_id: UUID) -> ReplayView:
        return await self._launch(replay_id, ReplayCommand.START)

    async def restart(self, replay_id: UUID) -> ReplayView:
        """Reset to the beginning and run again; keeps the current playback speed."""
        return await self._launch(replay_id, ReplayCommand.RESTART)

    async def pause(self, replay_id: UUID) -> ReplayView:
        async with self._lock(replay_id):
            record, state = await self._current(replay_id)
            runner = self._runner_for(state, ReplayCommand.PAUSE)
            runner.pause()
            state = runner.snapshot()
            # The in-memory change stands even if the save fails: report it either way.
            with self._notifying(record, state):
                await self._persist(state)
            self._log_transition("paused", record, state)
            return self._view(record, state)

    async def resume(self, replay_id: UUID) -> ReplayView:
        async with self._lock(replay_id):
            record, state = await self._current(replay_id)
            runner = self._runner_for(state, ReplayCommand.RESUME)
            runner.resume()
            state = runner.snapshot()
            with self._notifying(record, state):
                await self._persist(state)
            self._log_transition("resumed", record, state)
            return self._view(record, state)

    async def stop(self, replay_id: UUID) -> ReplayView:
        """Terminate the replay. A stopped replay can only run again via restart."""
        async with self._lock(replay_id):
            record, state = await self._current(replay_id)
            runner = self._runner_for(state, ReplayCommand.STOP)
            await runner.stop(timeout=self._stop_timeout)  # no DB session is open here
            state = runner.snapshot()
            with self._notifying(record, state):
                await self._retire(state)
            self._log_transition("stopped", record, state)
            return self._view(record, state)

    async def change_speed(
        self, replay_id: UUID, playback_speed: Decimal | float | int
    ) -> ReplayView:
        """Change speed in any status; the virtual race position is preserved."""
        speed = validate_playback_speed(playback_speed)
        async with self._lock(replay_id):
            record, state = await self._current(replay_id)
            previous = state.playback_speed
            if previous == speed:
                return self._view(record, state)
            runner = self._runners.get(replay_id)
            if runner is not None:
                runner.set_speed(speed)
                state = runner.snapshot()
            else:
                state = replace(state, playback_speed=speed)
            with self._notifying(record, state):
                await self._persist(state)
            logger.info(
                "Replay speed changed replay_id=%s session_id=%s status=%s speed=%s->%s "
                "race_time_ms=%d lap=%s",
                replay_id,
                state.session_id,
                state.status.value,
                previous,
                speed,
                state.current_race_time_ms,
                state.current_lap,
            )
            return self._view(record, state)

    # -- application lifecycle -------------------------------------------------

    async def recover_interrupted(self) -> int:
        """Mark persisted RUNNING/PAUSED replays without a live runner STOPPED.

        Called at startup: replay execution does not survive a process restart.
        """
        with self._storage("recover_interrupted"):
            async with self._session_factory() as db:
                ids = await ReplayRepository(db).stop_active(
                    reason=INTERRUPTED_REASON, ended_at=datetime.now(UTC)
                )
                await db.commit()
        if ids:
            logger.warning("Marked %d interrupted replay(s) STOPPED ids=%s", len(ids), ids)
        return len(ids)

    async def shutdown(self) -> None:
        """Stop every live replay, persist it as STOPPED and wait for its task.

        Also makes a final attempt to persist terminal states that failed to save.
        """
        runners = list(self._runners.values())
        for runner in runners:
            replay_id = runner.replay_id
            async with self._lock(replay_id):
                if self._runners.get(replay_id) is not runner:
                    continue
                if runner.status in ACTIVE_STATUSES:
                    await runner.stop(reason=SHUTDOWN_REASON, timeout=self._stop_timeout)
                else:
                    await runner.halt(timeout=self._stop_timeout)
                # Failure is logged; startup recovery marks a still-active row STOPPED.
                with suppress(ReplayPersistenceError):
                    await self._retire(runner.snapshot())
        for replay_id in list(self._pending_saves):
            async with self._lock(replay_id):
                pending = self._pending_saves.get(replay_id)
                if pending is not None:
                    await self._flush(pending)
        tasks = [r.task for r in runners if r.task is not None and not r.task.done()]
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=self._stop_timeout)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending)
        if runners:
            logger.info("Replay service shut down; stopped %d replay worker(s)", len(runners))

    # -- internals --------------------------------------------------------------

    @asynccontextmanager
    async def _lock(self, replay_id: UUID) -> AsyncIterator[None]:
        lock = self._locks.get(replay_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[replay_id] = lock
        async with lock:
            yield

    @contextmanager
    def _storage(self, action: str, **context: object) -> Iterator[None]:
        """Map database/IO failures to ``ReplayPersistenceError`` (client-safe 503)."""
        try:
            yield
        except (SQLAlchemyError, OSError) as exc:
            logger.exception("Replay storage failure action=%s %s", action, context)
            raise ReplayPersistenceError("Replay storage unavailable") from exc

    async def _current(self, replay_id: UUID) -> tuple[ReplayRecord, ReplayState]:
        """Persisted record plus the authoritative current state (caller holds the lock).

        Reads in its own short session. Unsaved in-memory state is retried first;
        a failed retry is logged but never fails the read.
        """
        with self._storage("read", replay_id=replay_id):
            async with self._session_factory() as db:
                record = await ReplayRepository(db).get(replay_id)
        if record is None:
            raise ReplayNotFoundError(replay_id)
        runner = self._runners.get(replay_id)
        if runner is not None:
            state = runner.snapshot()
            if replay_id in self._dirty:
                await self._flush(state)
            return record, state
        pending = self._pending_saves.get(replay_id)
        if pending is not None:
            await self._flush(pending)
            return record, pending
        state = record.state
        if state.status in ACTIVE_STATUSES:
            # Persisted as active but nothing runs it in this process: repair it.
            state = replace(
                state,
                status=transition(state.status, ReplayCommand.STOP),
                ended_at=datetime.now(UTC),
                status_reason=INTERRUPTED_REASON,
            )
            logger.warning(
                "Replay had no live worker; marking STOPPED replay_id=%s session_id=%s",
                replay_id,
                state.session_id,
            )
            await self._flush(state)  # on failure the row is repaired on a later access
        return record, state

    def _runner_for(self, state: ReplayState, command: ReplayCommand) -> ReplayRunner:
        runner = self._runners.get(state.replay_id)
        if runner is None:
            # No live runner means CREATED or terminal; the transition check raises.
            transition(state.status, command)
            raise RuntimeError(f"Replay {state.replay_id} is {state.status} without a runner")
        return runner

    async def _launch(self, replay_id: UUID, command: ReplayCommand) -> ReplayView:
        async with self._lock(replay_id):
            record, state = await self._current(replay_id)
            transition(state.status, command)  # fail fast before loading the timeline
            with self._storage("load_timeline", replay_id=replay_id):
                async with self._session_factory() as db:
                    entries = await ReplayRepository(db).load_timeline(state.session_id)
            runner = ReplayRunner(
                state,
                entries,
                sink=self._sink,
                timer=self._timer,
                on_finished=self._on_finished,
            )
            runner.prepare(command)
            new_state = runner.snapshot()
            # Nothing is mutated yet, so a failed save leaves the replay as it was.
            await self._save(new_state)
            self._dirty.discard(replay_id)
            self._pending_saves.pop(replay_id, None)

            previous = self._runners.get(replay_id)
            if previous is not None:
                # Restart: the old loop must exit before the new one begins.
                await previous.halt(timeout=self._stop_timeout)
            self._runners[replay_id] = runner
            self._last_run_ids[replay_id] = runner.run_id
            self._records[replay_id] = record
            runner.launch()
            self._log_transition(
                "restarted" if command is ReplayCommand.RESTART else "started", record, new_state
            )
            view = self._view(record, runner.snapshot())
            self._notify(view)
            return view

    async def _on_finished(self, runner: ReplayRunner) -> None:
        """Persist COMPLETED/FAILED once the runner's loop ends on its own."""
        async with self._lock(runner.replay_id):
            if self._runners.get(runner.replay_id) is not runner:
                return  # superseded by stop/restart/shutdown
            record = self._records.get(runner.replay_id)
            state = runner.snapshot()
            # Failure is logged; the terminal state stays pending and is retried on access.
            with suppress(ReplayPersistenceError):
                await self._retire(state)
            if record is not None:
                self._notify(self._view(record, state))

    async def _retire(self, state: ReplayState) -> None:
        """Unregister the runner (terminal state) and persist; never lose the state.

        The runner is always unregistered. If the save fails the state is kept in
        ``_pending_saves`` and ``ReplayPersistenceError`` is raised.
        """
        replay_id = state.replay_id
        self._runners.pop(replay_id, None)
        self._records.pop(replay_id, None)
        self._dirty.discard(replay_id)
        self._pending_saves[replay_id] = state
        await self._save(state)
        self._pending_saves.pop(replay_id, None)

    async def _persist(self, state: ReplayState) -> None:
        """Save the state of a non-terminal change; remember it as unsaved on failure."""
        replay_id = state.replay_id
        live = replay_id in self._runners
        if not live and replay_id in self._pending_saves:
            self._pending_saves[replay_id] = state  # e.g. speed change on a pending terminal
        try:
            await self._save(state)
        except ReplayPersistenceError:
            if live:
                self._dirty.add(replay_id)
            raise
        self._dirty.discard(replay_id)
        self._pending_saves.pop(replay_id, None)

    async def _flush(self, state: ReplayState) -> None:
        """Best-effort deferred save: log a warning instead of failing the caller."""
        try:
            await self._persist(state)
        except ReplayPersistenceError:
            logger.warning(
                "Deferred replay save still failing replay_id=%s status=%s",
                state.replay_id,
                state.status.value,
            )

    async def _save(self, state: ReplayState) -> None:
        """Write ``state`` in a fresh short transaction."""
        try:
            async with self._session_factory() as db:
                await ReplayRepository(db).save(state)
                await db.commit()
        except (SQLAlchemyError, OSError) as exc:
            logger.exception(
                "Replay persist failed replay_id=%s status=%s", state.replay_id, state.status
            )
            raise ReplayPersistenceError(
                f"Replay is {state.status.value} but the state is not saved yet; "
                "GET the replay for its current state"
            ) from exc

    @contextmanager
    def _notifying(self, record: ReplayRecord, state: ReplayState) -> Iterator[None]:
        """Notify listeners of ``state`` when the block exits, even if it raised."""
        try:
            yield
        finally:
            self._notify(self._view(record, state))

    def _notify(self, view: ReplayView) -> None:
        for listener in list(self._listeners):
            try:
                listener(view)
            except Exception:
                logger.exception(
                    "Replay listener failed replay_id=%s status=%s",
                    view.state.replay_id,
                    view.state.status.value,
                )

    @staticmethod
    def _view(record: ReplayRecord, state: ReplayState) -> ReplayView:
        return ReplayView(
            state=state,
            race_id=record.race_id,
            created_at=record.created_at,
        )

    @staticmethod
    def _log_transition(action: str, record: ReplayRecord, state: ReplayState) -> None:
        logger.info(
            "Replay %s replay_id=%s session_id=%s race_id=%s status=%s speed=%s "
            "race_time_ms=%d sequence=%s lap=%s",
            action,
            state.replay_id,
            state.session_id,
            record.race_id,
            state.status.value,
            state.playback_speed,
            state.current_race_time_ms,
            state.current_sequence,
            state.current_lap,
        )
