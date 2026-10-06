"""Scripted races for detector tests.

``Scenario`` drives the real race state reducer with synthetic raw events, wraps every
non-empty delta in a real state event (``build_state_event``), round-trips it through the
stream envelope JSON and feeds it to a ``DetectionEngine``. The context is round-tripped
through JSON after every step, exactly like the Redis store does, so serialization bugs
surface in the detector tests too.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple
from uuid import UUID, uuid5

from app.detection.config import DetectionConfig
from app.detection.detectors.base import Detector
from app.detection.engine import Action, DetectionEngine, decide
from app.detection.models import DetectedEvent, DetectionContext, StatePayload
from app.detection.registry import build_default_registry
from app.domain.enums import DetectedEventType, EventType, TrackStatus
from app.race_state.config import RaceStateConfig
from app.race_state.events import build_state_event
from app.race_state.models import (
    RaceSeed,
    RaceState,
    SeedDriver,
    StateDelta,
    StateEventType,
)
from app.race_state.reducer import ReducerEvent, apply_event, initial_state
from app.streaming.envelope import StreamEvent

REPLAY = UUID(int=1000)
RUN = UUID(int=2000)
SESSION = UUID(int=3000)
BASE_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
LAP_MS = 100_000  # race time reserved per lap: laps never overlap
DRIVERS = ("AAA", "BBB", "CCC", "DDD")
STATE_CONFIG = RaceStateConfig(lap_history=10, snapshot_every_laps=0)
ALL_DETECTORS = ("battle", "overtake", "pace_degradation", "pace_anomaly", "personal_best", "stint")


def driver_id(abbreviation: str) -> UUID:
    return UUID(int=100 + sum(ord(c) for c in abbreviation))


class Entry(NamedTuple):
    """One driver completing a lap: finishing ``offset`` ms into the lap's slot."""

    abbr: str
    offset: int = 0
    lap_time: int | None = 90_000
    stint: int | None = 1
    compound: str | None = "MEDIUM"
    age: int | None = None
    pit_in: bool | None = False
    pit_out: bool | None = False
    deleted: bool = False
    status: str = "GREEN"
    pos: int | None = None  # override the position (default: order of crossing)


def make_seed(
    abbreviations: Iterable[str] = DRIVERS, total_laps: int = 60, total_events: int = 100_000
) -> RaceSeed:
    return RaceSeed(
        session_id=SESSION,
        race_id=UUID(int=4000),
        season=2024,
        round=1,
        session_type="RACE",
        total_laps=total_laps,
        total_events=total_events,
        drivers=tuple(
            SeedDriver(driver_id(a), a, 10 + i, a.title(), "Team", i + 1)
            for i, a in enumerate(abbreviations)
        ),
    )


def config_for(
    detectors: Iterable[str] | None, base: DetectionConfig | None = None
) -> DetectionConfig:
    base = base or DetectionConfig()
    if detectors is None:
        return base
    return replace(base, disabled_detectors=frozenset(set(ALL_DETECTORS) - set(detectors)))


class Scenario:
    def __init__(
        self,
        abbreviations: Iterable[str] = DRIVERS,
        *,
        detectors: Iterable[str] | None = None,
        config: DetectionConfig | None = None,
        replay_id: UUID = REPLAY,
        run_id: UUID = RUN,
        start: bool = True,
        green: bool = True,
        extra: Iterable[Detector] = (),
    ) -> None:
        self.config = config_for(detectors, config)
        registry = build_default_registry(self.config)
        for detector in extra:
            registry.register(detector)
        self.engine = DetectionEngine(registry, self.config, clock=lambda: BASE_TIME)
        #: ``False``: build state events but do not feed them to the engine.
        self.deliver = True
        self.replay_id, self.run_id = replay_id, run_id
        self.seed = make_seed(abbreviations)
        self.state: RaceState = initial_state(self.seed, replay_id=replay_id, run_id=run_id)
        self.sequence = 0
        self.context: DetectionContext | None = None
        self.state_events: list[StreamEvent] = []
        self.detected: list[DetectedEvent] = []
        self.actions: list[Action] = []
        if start:
            self.start(green=green)

    # -- raw events -> state events -----------------------------------------------------------

    def start(self, *, green: bool = True) -> list[DetectedEvent]:
        grid = [
            {"driver_id": str(d.driver_id), "abbreviation": d.abbreviation, "grid_position": i + 1}
            for i, d in enumerate(self.seed.drivers)
        ]
        found = self.emit(EventType.RACE_STARTED, 0, payload={"grid": grid, "season": 2024})
        return found + (self.track(TrackStatus.GREEN, 0) if green else [])

    def track(self, status: TrackStatus, race_time: int) -> list[DetectedEvent]:
        return self.emit(
            EventType.TRACK_STATUS_CHANGED, race_time, payload={"status": status.value}
        )

    def pit_entry(self, abbr: str, race_time: int, lap: int) -> list[DetectedEvent]:
        return self.emit(EventType.PIT_ENTRY, race_time, abbr, lap)

    def pit_exit(
        self,
        abbr: str,
        race_time: int,
        lap: int,
        *,
        stint: int | None,
        compound: str | None = "HARD",
        duration: int | None = 22_000,
    ) -> list[DetectedEvent]:
        payload = {
            "stint_number": stint,
            "compound": compound,
            "tyre_age_laps": 0,
            "pit_lane_duration_ms": duration,
        }
        return self.emit(EventType.PIT_EXIT, race_time, abbr, lap, payload=payload)

    def round(self, lap: int, entries: list[Entry]) -> list[DetectedEvent]:
        """Everybody in ``entries`` completes ``lap`` in the order given (positions 1..n)."""
        found: list[DetectedEvent] = []
        ordered = sorted(enumerate(entries), key=lambda item: (item[1].offset, item[0]))
        for index, entry in ordered:
            payload: dict[str, Any] = {
                "lap_time_ms": entry.lap_time,
                "position": entry.pos if entry.pos is not None else index + 1,
                "compound": entry.compound,
                "tyre_age_laps": entry.age if entry.age is not None else lap,
                "stint_number": entry.stint,
                "is_deleted": entry.deleted,
                "is_accurate": True,
                "is_pit_in_lap": entry.pit_in,
                "is_pit_out_lap": entry.pit_out,
                "track_status": entry.status,
                "completion_time_source": "LAP_END_TIME",
            }
            found += self.emit(
                EventType.LAP_COMPLETED,
                lap * LAP_MS + entry.offset,
                entry.abbr,
                lap,
                payload=payload,
            )
        return found

    def emit(
        self,
        event_type: EventType,
        race_time: int,
        abbr: str | None = None,
        lap: int | None = None,
        payload: dict[str, Any] | None = None,
        *,
        rebuilt: bool = False,
        deliver: bool | None = None,
    ) -> list[DetectedEvent]:
        driver = driver_id(abbr) if abbr else None
        reducer_event = ReducerEvent(
            event_id=uuid5(self.run_id, str(self.sequence)),
            event_type=event_type.value,
            sequence=self.sequence,
            race_time_ms=race_time,
            lap_number=lap,
            driver_id=driver,
            driver_abbreviation=abbr,
            payload=payload or {},
        )
        source = StreamEvent(
            event_id=reducer_event.event_id,
            schema_version=1,
            event_type=event_type.value,
            replay_id=self.replay_id,
            run_id=self.run_id,
            session_id=SESSION,
            sequence=self.sequence,
            race_time_ms=race_time,
            lap_number=lap,
            driver_id=driver,
            driver_abbreviation=abbr,
            published_at=BASE_TIME + timedelta(milliseconds=self.sequence),
            payload=payload or {},
        )
        self.sequence += 1
        self.state, delta = apply_event(self.state, reducer_event, config=STATE_CONFIG)
        if delta.is_empty:
            return []
        if rebuilt:
            kind = StateEventType.STATE_REBUILT
        elif source.sequence == 0:
            kind = StateEventType.STATE_INITIALIZED
        else:
            kind = StateEventType.STATE_UPDATED
        state_event = self.wrap(kind, delta, source, rebuilt=rebuilt)
        self.state_events.append(state_event)
        return self.feed(state_event) if (self.deliver if deliver is None else deliver) else []

    def wrap(
        self, kind: StateEventType, delta: StateDelta, source: StreamEvent, *, rebuilt: bool = False
    ) -> StreamEvent:
        built = build_state_event(
            event_type=kind,
            state=self.state,
            delta=delta,
            source=source,
            rebuilt=rebuilt,
            published_at=BASE_TIME + timedelta(milliseconds=source.sequence),
        )
        return StreamEvent.from_json(built.to_json())  # as transported on the stream

    # -- detection side ---------------------------------------------------------------------------

    def feed(
        self, event: StreamEvent, *, bootstrap_state: RaceState | None = None
    ) -> list[DetectedEvent]:
        payload = StatePayload.model_validate(event.payload)
        action = decide(self.context, event, payload)
        result = self.engine.run(
            self.context, event, payload, action, bootstrap_state=bootstrap_state
        )
        self.actions.append(result.action)
        if result.context is not None:
            # What the Redis store does: the next step only sees the serialized context.
            self.context = DetectionContext.model_validate_json(result.context.model_dump_json())
        self.detected.extend(result.events)
        return result.events

    def of_type(self, *types: DetectedEventType) -> list[DetectedEvent]:
        return [e for e in self.detected if e.event_type in types]

    def types(self) -> list[str]:
        return [e.event_type.value for e in self.detected]


def logical(events: Iterable[DetectedEvent]) -> list[dict[str, Any]]:
    return [e.logical_dump() for e in events]


def drive(
    scenario: Scenario,
    times: list[int | None],
    *,
    abbr: str = "AAA",
    first_lap: int = 2,
    **entry: Any,
) -> list[DetectedEvent]:
    """``abbr`` completes consecutive laps with the given lap times (alone in the field)."""
    found: list[DetectedEvent] = []
    for index, lap_time in enumerate(times):
        found += scenario.round(first_lap + index, [Entry(abbr, 0, lap_time=lap_time, **entry)])
    return found
