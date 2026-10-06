"""Stream handler for the ``race-event-detectors`` group on ``race.state.events``.

Per state event, in one WATCH round on the replay's detection context:

1. load the context and decide (see ``engine``): duplicate, stale run, reset, apply or
   bootstrap;
2. let the engine advance the context and run the detectors (pure);
3. persist the detected events to PostgreSQL (idempotent: the deterministic id is the
   primary key) *before* the Redis commit, so a retry after a Redis failure can never
   duplicate rows;
4. commit the new context and the XADDs of the detected events in one Redis
   transaction. The consumer ACKs only after this returns.

A lost compare-and-set (another worker wrote the context first) is retried in process;
the re-read event is then usually a duplicate. Database and Redis failures propagate:
the message stays pending and is retried.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from pydantic import ValidationError

from app.detection.config import DetectionConfig
from app.detection.engine import STATE_EVENT_TYPES, Action, DetectionEngine
from app.detection.errors import DetectionConflictError, ReplayGoneError
from app.detection.models import StatePayload
from app.detection.repository import DetectedEventSink
from app.detection.store import DetectionContextStore
from app.race_state.repository import RaceStateStore
from app.streaming.consumer import ReceivedMessage
from app.streaming.envelope import StreamEvent
from app.streaming.errors import MalformedEventError

logger = logging.getLogger(__name__)


class DetectionProcessor:
    """``EventHandler`` for the ``race-event-detectors`` consumer group."""

    def __init__(
        self,
        engine: DetectionEngine,
        store: DetectionContextStore,
        state_store: RaceStateStore,
        sink: DetectedEventSink,
        *,
        config: DetectionConfig,
    ) -> None:
        self._engine = engine
        self._store = store
        self._state_store = state_store
        self._sink = sink
        self._config = config

    async def handle(self, message: ReceivedMessage) -> None:
        event = message.event
        if event.event_type not in STATE_EVENT_TYPES:
            logger.debug(
                "Event ignored: not a state event replay_id=%s type=%s",
                event.replay_id,
                event.event_type,
            )
            return
        try:
            payload = StatePayload.model_validate(event.payload)
        except ValidationError as exc:
            raise MalformedEventError(
                f"State event is malformed replay_id={event.replay_id} sequence={event.sequence}"
            ) from exc

        conflicts = 0
        while True:
            try:
                await self._attempt(event, payload)
                return
            except DetectionConflictError:
                conflicts += 1
                if conflicts >= self._config.conflict_attempts:
                    raise
                logger.info(
                    "Detection conflict, retrying replay_id=%s run_id=%s sequence=%d attempt=%d",
                    event.replay_id,
                    event.run_id,
                    event.sequence,
                    conflicts,
                )

    async def _attempt(self, event: StreamEvent, payload: StatePayload) -> None:
        async with self._store.transaction(event.replay_id) as txn:
            context = await txn.load()
            action = self._engine.decide(context, event, payload)
            if action is Action.DUPLICATE:
                logger.debug(
                    "Duplicate state event ignored replay_id=%s run_id=%s sequence=%d",
                    event.replay_id,
                    event.run_id,
                    event.sequence,
                )
                return
            if action is Action.STALE:
                logger.warning(
                    "State event of a stale run ignored replay_id=%s run_id=%s current_run_id=%s "
                    "sequence=%d",
                    event.replay_id,
                    event.run_id,
                    context.run_id if context else None,
                    event.sequence,
                )
                return

            bootstrap_state = None
            if action is Action.BOOTSTRAP:
                bootstrap_state = await self._state_store.get(event.replay_id)
            result = self._engine.run(
                context, event, payload, action, bootstrap_state=bootstrap_state
            )
            if result.context is None:
                return  # skipped (already logged by the engine)

            try:
                await self._sink.save(result.events)
            except ReplayGoneError:
                logger.warning(
                    "Replay no longer exists; state event skipped without publishing "
                    "replay_id=%s run_id=%s sequence=%d",
                    event.replay_id,
                    event.run_id,
                    event.sequence,
                )
                return
            now = datetime.now(UTC)
            await txn.commit(
                result.context,
                [e.to_stream_event(published_at=now) for e in result.events],
            )
        logger.debug(
            "State event processed replay_id=%s run_id=%s sequence=%d action=%s detected=%d",
            event.replay_id,
            event.run_id,
            event.sequence,
            result.action.value,
            len(result.events),
        )
