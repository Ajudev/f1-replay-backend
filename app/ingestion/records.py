"""Plain dataclasses for adapter and normalizer output.

No pandas, FastF1, or ORM imports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from uuid import UUID

from app.domain.enums import SessionType, TrackStatus


@dataclass(frozen=True, slots=True)
class ExtractedDriver:
    driver_number: int | None
    abbreviation: str | None
    first_name: str | None
    last_name: str | None
    full_name: str | None
    team_name: str | None
    grid_position: int | None
    finish_position: int | None
    result_status: str | None


@dataclass(frozen=True, slots=True)
class ExtractedLap:
    driver_abbreviation: str | None
    driver_number: int | None
    lap_number: int | None
    lap_time_ms: int | None
    position: int | None
    compound: str | None
    tyre_age_laps: int | None
    stint_number: int | None
    pit_in_time_ms: int | None
    pit_out_time_ms: int | None
    sector1_time_ms: int | None
    sector2_time_ms: int | None
    sector3_time_ms: int | None
    lap_start_time_ms: int | None
    is_deleted: bool | None
    is_accurate: bool | None
    team_name: str | None
    lap_end_time_ms: int | None = None


@dataclass(frozen=True, slots=True)
class ExtractedTrackStatus:
    race_time_ms: int | None
    source_code: str | None
    message: str | None


@dataclass(frozen=True, slots=True)
class ExtractedSession:
    season: int | None
    round: int | None
    event_name: str | None
    official_event_name: str | None
    country: str | None
    location: str | None
    event_date: date | None
    session_type: SessionType
    session_name: str | None
    session_start: datetime | None
    drivers: list[ExtractedDriver] = field(default_factory=list)
    laps: list[ExtractedLap] = field(default_factory=list)
    track_statuses: list[ExtractedTrackStatus] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class NormalizedDriver:
    driver_number: int | None
    abbreviation: str
    full_name: str
    first_name: str | None
    last_name: str | None
    team_name: str | None
    grid_position: int | None
    finish_position: int | None
    result_status: str | None


@dataclass(frozen=True, slots=True)
class NormalizedSector:
    sector_number: int
    sector_time_ms: int


@dataclass(frozen=True, slots=True)
class NormalizedLap:
    driver_abbreviation: str
    lap_number: int
    lap_time_ms: int | None
    position: int | None
    compound: str | None
    tyre_age_laps: int | None
    stint_number: int | None
    is_deleted: bool | None
    is_accurate: bool | None
    lap_start_time_ms: int | None
    pit_in_time_ms: int | None
    pit_out_time_ms: int | None
    is_pit_in_lap: bool
    is_pit_out_lap: bool
    pit_duration_ms: int | None
    lap_end_time_ms: int | None = None
    sectors: list[NormalizedSector] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class NormalizedStint:
    driver_abbreviation: str
    stint_number: int
    compound: str
    start_lap: int
    end_lap: int
    tyre_age_at_start: int | None


@dataclass(frozen=True, slots=True)
class NormalizedTrackStatus:
    race_time_ms: int
    status: TrackStatus
    source_code: str
    message: str | None
    sequence: int


@dataclass(frozen=True, slots=True)
class NormalizedSession:
    season: int
    round: int
    event_name: str
    official_event_name: str | None
    country: str | None
    location: str | None
    event_date: date | None
    session_type: SessionType
    session_name: str
    session_start: datetime | None
    drivers: list[NormalizedDriver] = field(default_factory=list)
    laps: list[NormalizedLap] = field(default_factory=list)
    stints: list[NormalizedStint] = field(default_factory=list)
    track_statuses: list[NormalizedTrackStatus] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped_count: int = 0


@dataclass(frozen=True, slots=True)
class PersistedImport:
    """Result of a repository persist attempt."""

    already_present: bool
    replaced: bool
    race_id: UUID | None
    session_id: UUID | None
    season: int | None = None
    round: int | None = None
    event_name: str | None = None
    session_type: SessionType | None = None
    driver_count: int = 0
    lap_count: int = 0
    sector_count: int = 0
    stint_count: int = 0
    track_status_count: int = 0


@dataclass(frozen=True, slots=True)
class ExistingSessionInfo:
    """An already-imported session looked up without FastF1."""

    race_id: UUID
    session_id: UUID
    season: int
    round: int
    event_name: str
    session_type: SessionType
    driver_count: int
    lap_count: int
    sector_count: int
    stint_count: int
    track_status_count: int


@dataclass(frozen=True, slots=True)
class ImportResult:
    """Service-level import outcome returned to the API."""

    status: str  # imported | already_imported | replaced
    race_id: UUID
    session_id: UUID
    season: int
    round: int
    event_name: str
    session_type: SessionType
    driver_count: int
    lap_count: int
    sector_count: int
    stint_count: int
    track_status_count: int
    warnings: list[str]
    skipped_count: int
