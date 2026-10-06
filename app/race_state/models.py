"""Race state contract: JSON-serializable Pydantic models (no Redis, no SQLAlchemy).

The same document is stored in Redis, persisted as PostgreSQL snapshots and
rebuilt by the reducer, so everything here must round-trip through JSON.

Notes on semantics (see README "Race state"):

- Positions are lap-end positions copied from ``LAP_COMPLETED`` / ``POSITION_CHANGED``
  events. Two drivers can briefly share a position number between their crossings.
- Gaps and intervals have ``LAP_END`` granularity and are ``null`` whenever they
  cannot be derived from the retained crossing window. Nothing is fabricated.
- No sector fields exist: ``SECTOR_COMPLETED`` is never emitted by the timeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.domain.enums import TrackStatus

#: Bump when the stored state document changes incompatibly. A stored document with
#: another version is ignored (treated as missing) and rebuilt from the timeline.
RACE_STATE_SCHEMA_VERSION = 1


class RacePhase(StrEnum):
    PRE_RACE = "PRE_RACE"
    RUNNING = "RUNNING"
    CHEQUERED = "CHEQUERED"  # the leader completed the final lap
    COMPLETED = "COMPLETED"  # the last timeline event was applied


class PitStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    IN_PIT = "IN_PIT"
    ON_TRACK = "ON_TRACK"


class DriverRaceStatus(StrEnum):
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    DID_NOT_FINISH = "DID_NOT_FINISH"  # only assigned when the race is finalized


class GapBasis(StrEnum):
    LAP_END = "LAP_END"


class ChangeKind(StrEnum):
    """What a state transition changed. Declaration order is the output order."""

    RACE_STARTED = "RACE_STARTED"
    LAP_COMPLETED = "LAP_COMPLETED"
    POSITION_CHANGED = "POSITION_CHANGED"
    PIT_STATUS_CHANGED = "PIT_STATUS_CHANGED"
    TRACK_STATUS_CHANGED = "TRACK_STATUS_CHANGED"
    FASTEST_LAP_CHANGED = "FASTEST_LAP_CHANGED"
    RACE_COMPLETED = "RACE_COMPLETED"


class SnapshotTrigger(StrEnum):
    INITIAL = "INITIAL"
    PERIODIC = "PERIODIC"
    FINAL = "FINAL"
    REBUILT = "REBUILT"


class StateEventType(StrEnum):
    """Event types published on the state stream."""

    STATE_INITIALIZED = "STATE_INITIALIZED"  # full snapshot
    STATE_UPDATED = "STATE_UPDATED"  # incremental delta
    STATE_REBUILT = "STATE_REBUILT"  # full snapshot after a rebuild from the timeline
    STATE_COMPLETED = "STATE_COMPLETED"  # full final snapshot


class StateSource(StrEnum):
    LIVE = "live"
    SNAPSHOT = "snapshot"


class LapRecord(BaseModel):
    lap_number: int
    lap_time_ms: int | None = None
    position: int | None = None
    compound: str | None = None
    tyre_age_laps: int | None = None
    stint_number: int | None = None
    is_pit_in_lap: bool | None = None
    is_pit_out_lap: bool | None = None
    is_deleted: bool | None = None
    is_accurate: bool | None = None
    track_status: str | None = None
    race_time_ms: int
    completion_time_source: str | None = None


class FastestLap(BaseModel):
    driver_id: UUID
    abbreviation: str | None = None
    lap_number: int | None = None
    lap_time_ms: int
    race_time_ms: int


class DriverState(BaseModel):
    driver_id: UUID
    abbreviation: str
    driver_number: int | None = None
    full_name: str | None = None
    team_name: str | None = None
    grid_position: int | None = None

    position: int | None = None
    previous_position: int | None = None

    laps_completed: int = 0
    #: ``laps_completed + 1`` (capped at ``total_laps``); ``null`` before the race
    #: starts and once the driver has finished.
    current_lap: int | None = None
    last_lap_time_ms: int | None = None
    last_lap_race_time_ms: int | None = None
    best_lap_time_ms: int | None = None
    best_lap_number: int | None = None

    gap_to_leader_ms: int | None = None
    interval_to_ahead_ms: int | None = None
    gap_basis: GapBasis | None = None
    laps_behind_leader: int | None = None

    compound: str | None = None
    tyre_age_laps: int | None = None
    stint_number: int | None = None
    #: Lap number the tyre information came from.
    tyre_info_lap: int | None = None

    pit_status: PitStatus = PitStatus.UNKNOWN
    pit_stop_count: int = 0
    last_pit_entry_lap: int | None = None
    last_pit_entry_race_time_ms: int | None = None
    last_pit_exit_race_time_ms: int | None = None
    last_pit_lane_duration_ms: int | None = None

    race_status: DriverRaceStatus = DriverRaceStatus.NOT_STARTED
    recent_laps: list[LapRecord] = Field(default_factory=list)


class LapCrossing(BaseModel):
    """When a driver completed a lap and the position reported with it."""

    race_time_ms: int
    position: int | None = None


class RaceState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = RACE_STATE_SCHEMA_VERSION
    replay_id: UUID
    run_id: UUID
    session_id: UUID
    race_id: UUID | None = None
    season: int | None = None
    round: int | None = None
    session_type: str | None = None

    phase: RacePhase = RacePhase.PRE_RACE
    #: Race time of the last applied event.
    current_race_time_ms: int = 0
    #: Lap the leader is on (``min(leader_laps_completed + 1, total_laps)``); ``null``
    #: before the race starts. Same rule as the replay's ``current_lap``.
    current_lap: int | None = None
    total_laps: int | None = None
    leader_laps_completed: int = 0
    leader_driver_id: UUID | None = None
    leader_driver_abbreviation: str | None = None
    track_status: TrackStatus | None = None
    fastest_lap: FastestLap | None = None

    last_sequence: int = -1
    last_event_id: UUID | None = None
    total_events: int = 0
    last_snapshot_lap: int = 0
    #: ``published_at`` of this run's first event (stale-run detection).
    run_published_at: datetime | None = None

    drivers: dict[UUID, DriverState] = Field(default_factory=dict)
    #: Bounded window ``lap -> driver -> crossing`` used to derive gaps.
    lap_crossings: dict[int, dict[UUID, LapCrossing]] = Field(default_factory=dict)

    #: Wall clock of the last write; set by the processor, never by the reducer, and
    #: excluded from determinism comparisons.
    updated_at: datetime | None = None

    def drivers_by_position(self) -> list[DriverState]:
        """Drivers ordered by position; unknown positions last, then laps (desc),
        earliest last crossing, abbreviation."""
        return sorted(self.drivers.values(), key=driver_sort_key)

    def logical_dump(self) -> dict[str, Any]:
        """JSON dump without wall-clock-derived fields (``updated_at`` and
        ``run_published_at``) for determinism comparisons. ``run_published_at`` is only
        approximate after a rebuild, which cannot see the run's first event."""
        return self.model_dump(mode="json", exclude={"updated_at", "run_published_at"})


def driver_sort_key(driver: DriverState) -> tuple[bool, int, int, bool, int, str]:
    return (
        driver.position is None,
        driver.position or 0,
        -driver.laps_completed,
        driver.last_lap_race_time_ms is None,
        driver.last_lap_race_time_ms or 0,
        driver.abbreviation,
    )


class StateDelta(BaseModel):
    """What one applied event changed. Empty means nothing is worth publishing."""

    kinds: list[ChangeKind] = Field(default_factory=list)
    #: Changed race-level fields with their new JSON values.
    race: dict[str, Any] = Field(default_factory=dict)
    #: Full state of every driver that changed.
    drivers: list[DriverState] = Field(default_factory=list)
    #: Snapshot due after this transition (deterministic; not published).
    snapshot_trigger: SnapshotTrigger | None = None

    @property
    def is_empty(self) -> bool:
        return not self.kinds and not self.race and not self.drivers


@dataclass(frozen=True, slots=True)
class SeedDriver:
    driver_id: UUID
    abbreviation: str
    driver_number: int | None = None
    full_name: str | None = None
    team_name: str | None = None
    grid_position: int | None = None


@dataclass(frozen=True, slots=True)
class RaceSeed:
    """Static session data a run's state is initialized from (loaded once per run)."""

    session_id: UUID
    race_id: UUID | None
    season: int | None
    round: int | None
    session_type: str | None
    total_laps: int | None
    total_events: int
    drivers: tuple[SeedDriver, ...] = field(default_factory=tuple)
