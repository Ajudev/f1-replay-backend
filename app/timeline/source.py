"""Plain input records for the timeline builder (no ORM, no FastF1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from app.domain.enums import SessionType, TrackStatus


@dataclass(frozen=True, slots=True)
class SourceDriver:
    id: UUID
    abbreviation: str
    grid_position: int | None


@dataclass(frozen=True, slots=True)
class SourceLap:
    """A normalized lap. All ``*_time_ms`` values are session-relative."""

    driver_id: UUID
    lap_number: int
    lap_time_ms: int | None
    position: int | None
    compound: str | None
    tyre_age_laps: int | None
    stint_number: int | None
    is_deleted: bool | None
    is_accurate: bool | None
    lap_start_time_ms: int | None
    lap_end_time_ms: int | None
    pit_in_time_ms: int | None
    pit_out_time_ms: int | None
    is_pit_in_lap: bool
    is_pit_out_lap: bool


@dataclass(frozen=True, slots=True)
class SourceTrackStatus:
    """A track status period start (session-relative time)."""

    sequence: int
    session_time_ms: int
    status: TrackStatus
    source_code: str


@dataclass(frozen=True, slots=True)
class TimelineSource:
    session_id: UUID
    race_id: UUID
    session_type: SessionType
    season: int
    round: int
    drivers: list[SourceDriver] = field(default_factory=list)
    laps: list[SourceLap] = field(default_factory=list)
    track_statuses: list[SourceTrackStatus] = field(default_factory=list)
