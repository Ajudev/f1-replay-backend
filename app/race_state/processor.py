"""Stream handler that turns raw replay events into race state.

Per event (one Redis ``WATCH`` / ``GET`` / ``EXEC`` round, see ``repository``):

1. load the replay's state and decide by ``(run_id, sequence)``:

   =====================================  ===========================================
   same run, ``seq <= last_sequence``     duplicate: nothing changes, ACK
   same run, ``seq == last + 1``          apply
   same run, ``seq > last + 1``           gap: wait briefly in process, then rebuild
   other run, older ``published_at``      stale run: ignored, ACK
   other run (or none), ``seq == 0``      initialize from the seed (replaces old state)
   other run (or none), ``seq > 0``       rebuild from the persisted timeline
   =====================================  ===========================================

2. reduce (pure), 3. write a PostgreSQL snapshot if due (periodic/initial failures
are logged and skipped, the final one must succeed), 4. commit state + state event in
one Redis transaction. The consumer ACKs only after this returns.

A gap is first given ``gap_wait_ms`` (re-reading the state every ``gap_poll_ms``, WATCH
released in between) for a concurrently processing worker or the predecessor itself to
land. If it persists, the missing events ``last_sequence+1 .. seq-1`` are reduced from the
persisted timeline onto the current state (same run, same ``run_published_at``), the event
is applied and one full-snapshot ``STATE_REBUILT`` is published. The late predecessor then
arrives as a duplicate. So a failed, dead-lettered or trimmed raw event never stalls state.
A ``StateConflictError`` (another worker won the compare-and-set) is retried in process a
bounded number of times, because the re-read event may now be a duplicate or the next one.

Sequence checks are the primary idempotency guard; the consumer's idempotency store is
a secondary one. A rebuild publishes one full-snapshot ``STATE_REBUILT`` event instead of
one event per replayed timeline event.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError

from app.race_state.config import RaceStateConfig
from app.race_state.errors import (
    RaceStateRebuildError,
    SnapshotPersistError,
    StateConflictError,
)
from app.race_state.events import build_state_event
from app.race_state.models import (
    RacePhase,
    RaceSeed,
    RaceState,
    SnapshotTrigger,
    StateDelta,
    StateEventType,
)
from app.race_state.reducer import ReducerEvent, apply_event, initial_state
from app.race_state.repository import RaceStateStore
from app.race_state.snapshots import SnapshotSink
from app.streaming.consumer import ReceivedMessage
from app.streaming.envelope import StreamEvent, derive_event_id
from app.timeline.repository import StoredEvent

logger = logging.getLogger(__name__)


class Action(StrEnum):
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    APPLY = "APPLY"
    INITIALIZE = "INITIALIZE"
    REBUILD = "REBUILD"
    GAP = "GAP"


def decide(state: RaceState | None, event: StreamEvent) -> Action:
    """Ordering and idempotency rules (pure)."""
    if state is None:
        return Action.INITIALIZE if event.sequence == 0 else Action.REBUILD
    if state.run_id == event.run_id:
        if event.sequence <= state.last_sequence:
            return Action.DUPLICATE
        if event.sequence == state.last_sequence + 1:
            return Action.APPLY
        return Action.GAP
    if state.run_published_at is not None and event.published_at < state.run_published_at:
        return Action.STALE
    return Action.INITIALIZE if event.sequence == 0 else Action.REBUILD


class SeedSource(Protocol):
    async def load_seed(self, session_id: UUID) -> RaceSeed: ...

    async def load_events_between(
        self, session_id: UUID, from_sequence: int, before_sequence: int
    ) -> list[StoredEvent]: ...


def reducer_event(event: StreamEvent) -> ReducerEvent:
    return ReducerEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        sequence=event.sequence,
        race_time_ms=event.race_time_ms,
        lap_number=event.lap_number,
        driver_id=event.driver_id,
        driver_abbreviation=event.driver_abbreviation,
        payload=event.payload,
    )


class RaceStateProcessor:
    """``EventHandler`` for the ``race-state-processors`` consumer group."""

    def __init__(
        self,
        store: RaceStateStore,
        seeds: SeedSource,
        snapshots: SnapshotSink,
        *,
        config: RaceStateConfig,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._store = store
        self._seeds = seeds
        self._snapshots = snapshots
        self._config = config
        self._clock = clock
        self._sleep = sleep

    async def handle(self, message: ReceivedMessage) -> None:
        event = message.event
        waited_ms = 0
        conflicts = 0
        while True:
            try:
                outcome = await self._attempt(event, gap_wait_over=waited_ms >= self._wait_ms)
            except StateConflictError:
                conflicts += 1
                if conflicts >= self._config.conflict_attempts:
                    raise
                logger.info(
                    "State conflict, retrying replay_id=%s run_id=%s sequence=%d attempt=%d",
                    event.replay_id,
                    event.run_id,
                    event.sequence,
                    conflicts,
                )
                continue
            if outcome is not Action.GAP:
                return
            # The transaction (and its WATCH) is released; give predecessors time to land.
            poll = max(1, self._config.gap_poll_ms)
            await self._sleep(poll / 1000)
            waited_ms += poll

    @property
    def _wait_ms(self) -> int:
        return self._config.gap_wait_ms

    async def _attempt(self, event: StreamEvent, *, gap_wait_over: bool) -> Action:
        """One read-decide-commit round. Returns ``GAP`` when the event should wait."""
        async with self._store.transaction(event.replay_id) as txn:
            state = await txn.load()
            action = decide(state, event)
            if action is Action.DUPLICATE:
                logger.debug(
                    "Duplicate event ignored replay_id=%s run_id=%s sequence=%d",
                    event.replay_id,
                    event.run_id,
                    event.sequence,
                )
                return action
            if action is Action.STALE:
                logger.info(
                    "Event of a stale run ignored replay_id=%s run_id=%s current_run_id=%s "
                    "sequence=%d",
                    event.replay_id,
                    event.run_id,
                    state.run_id if state else None,
                    event.sequence,
                )
                return action
            if action is Action.GAP and not gap_wait_over:
                assert state is not None
                logger.debug(
                    "Sequence gap, waiting replay_id=%s run_id=%s expected=%d received=%d",
                    event.replay_id,
                    event.run_id,
                    state.last_sequence + 1,
                    event.sequence,
                )
                return action

            if action is Action.APPLY:
                assert state is not None
                new_state, delta = apply_event(state, reducer_event(event), config=self._config)
            elif action is Action.GAP:
                assert state is not None
                logger.warning(
                    "Sequence gap replay_id=%s run_id=%s expected=%d received=%d: "
                    "gap resolved by rebuild",
                    event.replay_id,
                    event.run_id,
                    state.last_sequence + 1,
                    event.sequence,
                )
                # Same run: continue from the current state (equivalent to a rebuild from
                # sequence 0 because the reducer is deterministic) and keep its
                # ``run_published_at``.
                base = await self._replay_history(
                    state, event, from_sequence=state.last_sequence + 1
                )
                new_state, delta = apply_event(base, reducer_event(event), config=self._config)
            else:
                seed = await self._seeds.load_seed(event.session_id)
                # For a run first seen here, this event's ``published_at`` stands in for
                # the run's start (the sequence-0 event may be unseen). It is never earlier
                # than the real start, so only a straggler published before this event can
                # be classified as stale; stragglers of an older run were published
                # before the newer run began, hence before this event. Late is safe.
                base = initial_state(
                    seed,
                    replay_id=event.replay_id,
                    run_id=event.run_id,
                    run_published_at=event.published_at,
                )
                if action is Action.REBUILD:
                    base = await self._replay_history(base, event, from_sequence=0)
                new_state, delta = apply_event(base, reducer_event(event), config=self._config)

            rebuilt = action in (Action.REBUILD, Action.GAP)
            now = self._clock()
            new_state.updated_at = now

            trigger = delta.snapshot_trigger or (SnapshotTrigger.REBUILT if rebuilt else None)
            if trigger is not None:
                await self._snapshot(new_state, trigger, event)

            state_event = self._state_event(new_state, delta, event, rebuilt, action, now)
            await txn.commit(new_state, state_event)

        self._log_applied(event, action, new_state, delta, state_event is not None)
        return action

    async def _replay_history(
        self, base: RaceState, event: StreamEvent, *, from_sequence: int
    ) -> RaceState:
        """Reduce persisted timeline events ``from_sequence .. sequence-1`` onto ``base``."""
        stored = await self._seeds.load_events_between(
            event.session_id, from_sequence, event.sequence
        )
        expected = event.sequence - from_sequence
        if len(stored) != expected or any(
            e.sequence != from_sequence + index or e.race_time_ms is None
            for index, e in enumerate(stored)
        ):
            raise RaceStateRebuildError(
                f"Cannot rebuild replay_id={event.replay_id} to sequence {event.sequence}: "
                f"the stored timeline of session {event.session_id} has {len(stored)} usable "
                f"contiguous events from {from_sequence}, expected {expected} "
                "(regenerated or incomplete timeline)"
            )
        state = base
        for stored_event in stored:
            assert stored_event.race_time_ms is not None
            state, _ = apply_event(
                state,
                ReducerEvent(
                    event_id=derive_event_id(event.run_id, stored_event.sequence),
                    event_type=stored_event.event_type.value,
                    sequence=stored_event.sequence,
                    race_time_ms=stored_event.race_time_ms,
                    lap_number=stored_event.lap_number,
                    driver_id=stored_event.driver_id,
                    driver_abbreviation=stored_event.driver_abbreviation,
                    payload=stored_event.payload,
                ),
                config=self._config,
            )
        logger.info(
            "Race state rebuilt from timeline replay_id=%s run_id=%s from=%d events=%d",
            event.replay_id,
            event.run_id,
            from_sequence,
            len(stored),
        )
        return state

    async def _snapshot(
        self, state: RaceState, trigger: SnapshotTrigger, event: StreamEvent
    ) -> None:
        try:
            await self._snapshots.save(state, trigger)
        except (SQLAlchemyError, OSError) as exc:
            if trigger is SnapshotTrigger.FINAL:
                raise SnapshotPersistError(
                    f"Final race state snapshot could not be persisted replay_id={event.replay_id}"
                ) from exc
            logger.error(
                "Race state snapshot failed, continuing replay_id=%s run_id=%s sequence=%d "
                "trigger=%s",
                event.replay_id,
                event.run_id,
                event.sequence,
                trigger.value,
                exc_info=exc,
            )

    @staticmethod
    def _state_event(
        state: RaceState,
        delta: StateDelta,
        source: StreamEvent,
        rebuilt: bool,
        action: Action,
        now: datetime,
    ) -> StreamEvent | None:
        if state.phase is RacePhase.COMPLETED and delta.snapshot_trigger is SnapshotTrigger.FINAL:
            event_type = StateEventType.STATE_COMPLETED
        elif rebuilt:
            event_type = StateEventType.STATE_REBUILT
        elif action is Action.INITIALIZE:
            event_type = StateEventType.STATE_INITIALIZED
        elif not delta.is_empty:
            event_type = StateEventType.STATE_UPDATED
        else:
            return None
        return build_state_event(
            event_type=event_type,
            state=state,
            delta=delta,
            source=source,
            rebuilt=rebuilt,
            published_at=now,
        )

    @staticmethod
    def _log_applied(
        event: StreamEvent,
        action: Action,
        state: RaceState,
        delta: StateDelta,
        published: bool,
    ) -> None:
        if action in (Action.INITIALIZE, Action.REBUILD, Action.GAP):
            logger.info(
                "Race state %s replay_id=%s run_id=%s sequence=%d lap=%s",
                "initialized" if action is Action.INITIALIZE else "rebuilt",
                event.replay_id,
                event.run_id,
                event.sequence,
                state.current_lap,
            )
        if state.phase is RacePhase.COMPLETED:
            logger.info(
                "Race state completed replay_id=%s run_id=%s sequence=%d",
                event.replay_id,
                event.run_id,
                event.sequence,
            )
        logger.debug(
            "Event applied replay_id=%s event_id=%s sequence=%d type=%s lap=%s driver=%s "
            "kinds=%s published=%s",
            event.replay_id,
            event.event_id,
            event.sequence,
            event.event_type,
            event.lap_number,
            event.driver_abbreviation,
            [k.value for k in delta.kinds],
            published,
        )
