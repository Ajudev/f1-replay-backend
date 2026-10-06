"""The detection engine: state event in, detected events and new context out.

``DetectionEngine`` is deterministic and does no I/O (the clock is injected and only
stamps ``detected_at``). The stream handler around it (``processor``) loads and commits
the context and persists and publishes the result.

Why it reads ``race.state.events`` and not the raw stream: the race state processor runs
concurrently, so "the current state" read while handling a raw event is usually already
ahead of that event and the outcome would depend on timing. A state event pairs the
source raw event with the state exactly after it, so the same stream always yields the
same detections.

Per replay the engine keeps a ``DetectionContext``: the run, the last sequence handled, a
mirror of the public race state, the track status history and one memory per detector.
What happens to an incoming state event:

======================================  ==============================================
same run, ``seq <= last_sequence``      duplicate: nothing is detected or published
same run, ``seq > last_sequence``       mirror advanced (delta merged / snapshot taken)
other run (or none), snapshot event     context reset from the full state, fresh memory
other run (or none), ``STATE_UPDATED``  bootstrap the mirror from the race state store
                                        (same run, not behind), memory starts empty
other run, published before the        stale straggler of an older run: ignored
current run began
======================================  ==============================================

Sequence holes are normal (state events are only published for non-empty deltas).
``STATE_REBUILT`` means events were skipped: detectors get ``rebuilt=True``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ValidationError

from app.detection.config import DetectionConfig
from app.detection.detectors.base import Detector
from app.detection.models import (
    DetectedEvent,
    DetectionContext,
    DetectionDraft,
    DetectorInput,
    DetectorMemory,
    SourceRef,
    StatePayload,
    derive_detected_event_id,
)
from app.detection.registry import DetectorRegistry
from app.detection.track_status import TrackStatusHistory
from app.domain.enums import EventType
from app.race_state.models import DriverState, RaceState, StateEventType
from app.streaming.envelope import StreamEvent

logger = logging.getLogger(__name__)

STATE_EVENT_TYPES = frozenset(t.value for t in StateEventType)
SNAPSHOT_TYPES = frozenset(
    {
        StateEventType.STATE_INITIALIZED.value,
        StateEventType.STATE_REBUILT.value,
        StateEventType.STATE_COMPLETED.value,
    }
)


class Action(StrEnum):
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    RESET = "RESET"
    APPLY = "APPLY"
    BOOTSTRAP = "BOOTSTRAP"
    SKIP = "SKIP"


@dataclass(frozen=True, slots=True)
class EngineResult:
    action: Action
    #: New context to store; ``None`` when nothing may be written.
    context: DetectionContext | None = None
    events: list[DetectedEvent] = field(default_factory=list)


def decide(context: DetectionContext | None, event: StreamEvent, payload: StatePayload) -> Action:
    """Ordering and idempotency rules (pure). ``BOOTSTRAP`` may still turn into ``SKIP``."""
    snapshot = event.event_type in SNAPSHOT_TYPES and payload.state is not None
    if context is not None and context.run_id == event.run_id:
        if event.sequence <= context.last_sequence:
            return Action.DUPLICATE
        return Action.APPLY
    if (
        context is not None
        and context.run_published_at is not None
        and event.published_at < context.run_published_at
    ):
        return Action.STALE
    return Action.RESET if snapshot else Action.BOOTSTRAP


def merge_delta(base: RaceState, payload: StatePayload, event: StreamEvent) -> RaceState:
    """``base`` with a ``STATE_UPDATED`` delta applied (a new object; ``base`` is untouched)."""
    data = base.model_dump(mode="python", exclude={"drivers"})
    data.update(payload.changes.race)
    data["current_race_time_ms"] = event.race_time_ms
    data["last_sequence"] = event.sequence
    data["last_event_id"] = payload.source_event_id
    drivers: dict = dict(base.drivers)
    for changed in payload.changes.drivers:
        drivers[changed.driver_id] = changed
    return RaceState.model_validate({**data, "drivers": drivers})


class DetectionEngine:
    def __init__(
        self,
        registry: DetectorRegistry,
        config: DetectionConfig,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._registry = registry
        self._config = config
        self._clock = clock

    @property
    def registry(self) -> DetectorRegistry:
        return self._registry

    def decide(
        self, context: DetectionContext | None, event: StreamEvent, payload: StatePayload
    ) -> Action:
        return decide(context, event, payload)

    def run(
        self,
        context: DetectionContext | None,
        event: StreamEvent,
        payload: StatePayload,
        action: Action,
        *,
        bootstrap_state: RaceState | None = None,
    ) -> EngineResult:
        """Advance the context by one state event and run the applicable detectors."""
        if action in (Action.DUPLICATE, Action.STALE, Action.SKIP):
            return EngineResult(action)

        if action is Action.RESET:
            assert payload.state is not None
            current = payload.state
            previous = current
            new = self._new_context(event, current)
            logger.info(
                "Detection context reset replay_id=%s run_id=%s sequence=%d",
                event.replay_id,
                event.run_id,
                event.sequence,
            )
        elif action is Action.BOOTSTRAP:
            if (
                bootstrap_state is None
                or bootstrap_state.run_id != event.run_id
                or bootstrap_state.last_sequence < event.sequence
            ):
                logger.warning(
                    "State event skipped: no detection context and the race state cannot "
                    "bootstrap one replay_id=%s run_id=%s sequence=%d",
                    event.replay_id,
                    event.run_id,
                    event.sequence,
                )
                return EngineResult(Action.SKIP)
            previous = bootstrap_state
            current = merge_delta(bootstrap_state, payload, event)
            new = self._new_context(event, current)
            logger.warning(
                "Detection context bootstrapped from the race state; detector memories start "
                "empty replay_id=%s run_id=%s sequence=%d state_sequence=%d",
                event.replay_id,
                event.run_id,
                event.sequence,
                bootstrap_state.last_sequence,
            )
        else:
            assert context is not None
            previous = context.state
            current = (
                payload.state
                if event.event_type in SNAPSHOT_TYPES and payload.state is not None
                else merge_delta(previous, payload, event)
            )
            new = context.model_copy(update={"state": current, "last_sequence": event.sequence})
            new.detectors = dict(context.detectors)
            new.track_history = context.track_history.model_copy(deep=True)
            new.track_history.record(
                current.track_status,
                event.race_time_ms,
                limit=self._config.pace.track_history_limit,
            )

        new.updated_at = self._clock()
        data = self._input(event, payload, previous, current, new)
        events = self._detect(data, new)
        return EngineResult(action, new, events)

    # -- internals -----------------------------------------------------------------------------

    def _new_context(self, event: StreamEvent, state: RaceState) -> DetectionContext:
        history = TrackStatusHistory()
        history.record(
            state.track_status,
            event.race_time_ms,
            limit=self._config.pace.track_history_limit,
        )
        return DetectionContext(
            replay_id=event.replay_id,
            run_id=event.run_id,
            session_id=event.session_id,
            run_published_at=event.published_at,
            last_sequence=event.sequence,
            state=state,
            track_history=history,
        )

    def _input(
        self,
        event: StreamEvent,
        payload: StatePayload,
        previous: RaceState,
        current: RaceState,
        context: DetectionContext,
    ) -> DetectorInput:
        driver = current.drivers.get(event.driver_id) if event.driver_id else None
        return DetectorInput(
            source=SourceRef(
                event_id=payload.source_event_id,
                event_type=payload.source_event_type,
                sequence=event.sequence,
                race_time_ms=event.race_time_ms,
                lap_number=_source_lap(payload.source_event_type, driver, event.race_time_ms),
                driver_id=event.driver_id,
                driver_abbreviation=event.driver_abbreviation,
            ),
            previous=previous,
            current=current,
            rebuilt=payload.rebuilt,
            track_history=context.track_history,
            config=self._config,
            replay_id=event.replay_id,
            run_id=event.run_id,
            session_id=event.session_id,
        )

    def _detect(self, data: DetectorInput, context: DetectionContext) -> list[DetectedEvent]:
        events: dict = {}
        for detector in self._registry.for_event(data.source.event_type):
            memory = self._load_memory(detector, context)
            try:
                drafts, new_memory = detector.evaluate(data, memory)
            except Exception:
                # Detectors are independent: one failing must not block the others or the
                # context update (a retry would fail the same way, deterministically).
                logger.exception(
                    "Detector failed, skipped replay_id=%s detector=%s sequence=%d",
                    data.replay_id,
                    detector.name,
                    data.source.sequence,
                )
                continue
            context.detectors[detector.name] = DetectorMemory(
                version=detector.version, data=new_memory.model_dump(mode="json")
            )
            for draft in drafts:
                detected = self._build(detector, draft, data)
                if detected.detected_event_id in events:
                    continue
                events[detected.detected_event_id] = detected
                self._log_detected(detected)
        return list(events.values())

    @staticmethod
    def _load_memory(detector: Detector, context: DetectionContext) -> BaseModel:
        stored = context.detectors.get(detector.name)
        if stored is not None and stored.version == detector.version:
            try:
                return detector.memory_model.model_validate(stored.data)
            except ValidationError:
                logger.warning(
                    "Unreadable detector memory discarded replay_id=%s detector=%s",
                    context.replay_id,
                    detector.name,
                )
        elif stored is not None:
            logger.warning(
                "Detector memory of another version discarded replay_id=%s detector=%s "
                "stored=%d current=%d",
                context.replay_id,
                detector.name,
                stored.version,
                detector.version,
            )
        return detector.new_memory()

    def _build(
        self, detector: Detector, draft: DetectionDraft, data: DetectorInput
    ) -> DetectedEvent:
        primary, secondary = draft.primary, draft.secondary
        return DetectedEvent(
            detected_event_id=derive_detected_event_id(
                data.run_id,
                detector_name=detector.name,
                event_type=draft.event_type,
                source_sequence=data.source.sequence,
                driver_ids=(
                    primary.driver_id if primary else None,
                    secondary.driver_id if secondary else None,
                ),
                key=draft.key,
            ),
            event_type=draft.event_type,
            replay_id=data.replay_id,
            run_id=data.run_id,
            session_id=data.session_id,
            race_id=data.current.race_id,
            race_time_ms=data.source.race_time_ms,
            lap_number=draft.lap_number,
            primary_driver_id=primary.driver_id if primary else None,
            primary_driver_abbreviation=primary.abbreviation if primary else None,
            secondary_driver_id=secondary.driver_id if secondary else None,
            secondary_driver_abbreviation=secondary.abbreviation if secondary else None,
            severity=draft.severity,
            confidence=draft.confidence,
            evidence=draft.evidence,
            source_event_ids=[data.source.event_id],
            source_sequence=data.source.sequence,
            detector_name=detector.name,
            detector_version=detector.version,
            detected_at=self._clock(),
        )

    @staticmethod
    def _log_detected(event: DetectedEvent) -> None:
        summary = {
            key: value
            for key, value in event.evidence.items()
            if isinstance(value, str | int | float | bool)
        }
        logger.info(
            "Detected event replay_id=%s run_id=%s detector=%s type=%s driver=%s other=%s "
            "lap=%s evidence=%s",
            event.replay_id,
            event.run_id,
            event.detector_name,
            event.event_type.value,
            event.primary_driver_abbreviation,
            event.secondary_driver_abbreviation,
            event.lap_number,
            summary,
        )


def _source_lap(
    source_event_type: str, driver: DriverState | None, race_time_ms: int
) -> int | None:
    """Lap of the source event for its driver. State events carry the leader's lap, so
    it is recovered from the driver's own state."""
    if driver is None:
        return None
    if source_event_type == EventType.LAP_COMPLETED.value:
        laps = [r.lap_number for r in driver.recent_laps if r.race_time_ms == race_time_ms]
        return max(laps) if laps else None
    if source_event_type == EventType.PIT_EXIT.value:
        return driver.tyre_info_lap
    return driver.laps_completed
