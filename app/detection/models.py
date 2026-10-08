"""Detection value objects: detected events, detector input, stream payload, context.

``DetectedEvent`` is the published/persisted contract (``schema_version`` 1). Detectors
never build it directly: they return ``DetectionDraft`` objects and the engine fills in
identity, provenance and the deterministic id.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field

from app.detection.config import DetectionConfig
from app.detection.track_status import TrackStatusHistory
from app.domain.enums import DetectedEventType, Severity
from app.race_state.models import DriverState, LapRecord, RaceState
from app.streaming.envelope import STREAM_EVENT_SCHEMA_VERSION, StreamEvent

DETECTED_EVENT_SCHEMA_VERSION = 1
CONTEXT_SCHEMA_VERSION = 1


# -- detected event (published and persisted) ---------------------------------------------


class DetectedEvent(BaseModel):
    """One detection: what was observed, about whom, and the numbers behind it."""

    model_config = ConfigDict(frozen=True)

    detected_event_id: UUID
    event_type: DetectedEventType
    schema_version: int = DETECTED_EVENT_SCHEMA_VERSION
    replay_id: UUID
    run_id: UUID
    session_id: UUID
    race_id: UUID | None = None
    race_time_ms: int
    lap_number: int | None = None
    primary_driver_id: UUID | None = None
    primary_driver_abbreviation: str | None = None
    secondary_driver_id: UUID | None = None
    secondary_driver_abbreviation: str | None = None
    severity: Severity | None = None
    #: Only set when objectively derived; no detector currently does.
    confidence: float | None = None
    #: Machine-readable facts behind the detection (numbers in milliseconds).
    evidence: dict[str, Any] = Field(default_factory=dict)
    source_event_ids: list[UUID] = Field(default_factory=list)
    source_sequence: int
    detector_name: str
    detector_version: int
    #: Wall clock; excluded from determinism comparisons.
    detected_at: datetime

    def logical_dump(self) -> dict[str, Any]:
        """JSON dump without wall-clock fields, for determinism comparisons."""
        return self.model_dump(mode="json", exclude={"detected_at"})

    def to_stream_event(self, *, published_at: datetime) -> StreamEvent:
        """Envelope for ``race.detected.events`` (same shape as every other stream)."""
        return StreamEvent(
            event_id=self.detected_event_id,
            schema_version=STREAM_EVENT_SCHEMA_VERSION,
            event_type=self.event_type.value,
            replay_id=self.replay_id,
            run_id=self.run_id,
            session_id=self.session_id,
            sequence=self.source_sequence,
            race_time_ms=self.race_time_ms,
            lap_number=self.lap_number,
            driver_id=self.primary_driver_id,
            driver_abbreviation=self.primary_driver_abbreviation,
            published_at=published_at,
            payload=self.model_dump(mode="json"),
        )


def derive_detected_event_id(
    run_id: UUID,
    *,
    detector_name: str,
    event_type: DetectedEventType,
    source_sequence: int,
    driver_ids: tuple[UUID | None, ...],
    key: str = "",
) -> UUID:
    """Deterministic id: the same run, detector, source event and subjects always give
    the same id, so a retried or reprocessed event never creates a second row."""
    drivers = ",".join(str(d) if d else "-" for d in driver_ids)
    return uuid5(
        run_id, f"detected:{detector_name}:{event_type.value}:{source_sequence}:{drivers}:{key}"
    )


# -- detector side ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DriverRef:
    driver_id: UUID
    abbreviation: str

    @classmethod
    def of(cls, driver: DriverState) -> DriverRef:
        return cls(driver.driver_id, driver.abbreviation)


@dataclass(frozen=True, slots=True)
class DetectionDraft:
    """What a detector reports; the engine turns it into a ``DetectedEvent``."""

    event_type: DetectedEventType
    primary: DriverRef | None
    secondary: DriverRef | None = None
    lap_number: int | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    severity: Severity | None = None
    confidence: float | None = None
    #: Distinguishes several detections of one detector for the same source event and
    #: drivers (the logical window). Part of the deterministic id.
    key: str = ""


@dataclass(frozen=True, slots=True)
class SourceRef:
    """The raw event the state transition came from."""

    event_id: UUID
    event_type: str
    sequence: int
    race_time_ms: int
    #: Lap of the source event for its driver (not the leader's lap).
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None


@dataclass(frozen=True, slots=True)
class DetectorInput:
    """Everything a detector may look at. Read-only: never mutate the states."""

    source: SourceRef
    #: Race state before this event (equal to ``current`` when there is no earlier one).
    previous: RaceState
    #: Race state exactly after this event.
    current: RaceState
    #: Intermediate events were skipped (state rebuilt); consecutive-lap logic must verify.
    rebuilt: bool
    track_history: TrackStatusHistory
    config: DetectionConfig
    replay_id: UUID
    run_id: UUID
    session_id: UUID

    @property
    def driver(self) -> DriverState | None:
        """The source event's driver in the current state."""
        if self.source.driver_id is None:
            return None
        return self.current.drivers.get(self.source.driver_id)

    def lap_record(self, driver: DriverState, lap_number: int) -> LapRecord | None:
        return next((r for r in driver.recent_laps if r.lap_number == lap_number), None)

    def source_lap_record(self) -> tuple[DriverState, LapRecord] | None:
        """For ``LAP_COMPLETED``: the source driver and the lap record the event wrote."""
        driver = self.driver
        if driver is None or self.source.lap_number is None:
            return None
        record = self.lap_record(driver, self.source.lap_number)
        return (driver, record) if record is not None else None


# -- stream payload of state events ------------------------------------------------------------


class StateChanges(BaseModel):
    model_config = ConfigDict(extra="ignore")

    race: dict[str, Any] = Field(default_factory=dict)
    drivers: list[DriverState] = Field(default_factory=list)
    position_changes: list[UUID] = Field(default_factory=list)
    pit_changes: list[UUID] = Field(default_factory=list)


class StatePayload(BaseModel):
    """Payload of a ``race.state.events`` event (see ``app.race_state.events``)."""

    model_config = ConfigDict(extra="ignore")

    source_event_id: UUID
    source_event_type: str
    last_sequence: int
    rebuilt: bool = False
    kinds: list[str] = Field(default_factory=list)
    changes: StateChanges = Field(default_factory=StateChanges)
    #: Full public state; only on ``STATE_INITIALIZED`` / ``STATE_REBUILT`` / ``STATE_COMPLETED``.
    state: RaceState | None = None


# -- per-replay detection context ---------------------------------------------------------------


class DetectorMemory(BaseModel):
    """One detector's persisted memory, tagged with the version that wrote it."""

    version: int
    data: dict[str, Any] = Field(default_factory=dict)


class DetectionContext(BaseModel):
    """Everything the engine keeps per replay between state events (one Redis document)."""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = CONTEXT_SCHEMA_VERSION
    replay_id: UUID
    run_id: UUID
    session_id: UUID
    #: ``published_at`` of the first state event of this run seen here (stale-run guard).
    run_published_at: datetime | None = None
    last_sequence: int = -1
    #: Public race state mirrored from the state events (as of ``last_sequence``).
    state: RaceState
    track_history: TrackStatusHistory = Field(default_factory=TrackStatusHistory)
    detectors: dict[str, DetectorMemory] = Field(default_factory=dict)
    updated_at: datetime | None = None
