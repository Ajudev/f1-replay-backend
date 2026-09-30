"""Pydantic schemas for race import and query endpoints."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from app.domain.enums import SessionType, TrackStatus


class RaceImportRequest(BaseModel):
    """Request body for importing a historical session via FastF1."""

    season: int = Field(description="Championship season year")
    round: int | None = Field(default=None, description="Championship round number")
    event_name: str | None = Field(default=None, description="FastF1 event name")
    session_type: SessionType = Field(default=SessionType.RACE)
    replace: bool = Field(
        default=False,
        description="When true, rebuild an already-imported session",
    )

    @model_validator(mode="after")
    def require_exactly_one_event_selector(self) -> Self:
        has_round = self.round is not None
        has_name = self.event_name is not None and self.event_name.strip() != ""
        if has_round == has_name:
            raise ValueError("Provide exactly one of 'round' or 'event_name'")
        return self


class RaceImportResponse(BaseModel):
    """Result of a race session import."""

    status: Literal["imported", "already_imported", "replaced"]
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


class RaceSummary(BaseModel):
    id: UUID
    season: int
    round: int
    name: str
    official_name: str | None
    country: str | None
    location: str | None
    event_date: date | None


class SessionSummary(BaseModel):
    id: UUID
    session_type: SessionType
    name: str
    start_time: datetime | None
    end_time: datetime | None


class RaceDetail(BaseModel):
    id: UUID
    season: int
    round: int
    name: str
    official_name: str | None
    country: str | None
    location: str | None
    event_date: date | None
    sessions: list[SessionSummary]


class DriverSummary(BaseModel):
    id: UUID
    driver_number: int | None
    abbreviation: str
    full_name: str
    first_name: str | None
    last_name: str | None
    team_name: str | None
    grid_position: int | None
    finish_position: int | None
    result_status: str | None


class SessionCounts(BaseModel):
    laps: int
    sectors: int
    stints: int
    track_status_periods: int


class SessionDetail(BaseModel):
    id: UUID
    race_id: UUID
    session_type: SessionType
    name: str
    start_time: datetime | None
    end_time: datetime | None
    drivers: list[DriverSummary]
    counts: SessionCounts


class SectorOut(BaseModel):
    sector_number: int
    sector_time_ms: int | None


class LapOut(BaseModel):
    id: UUID
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
    sectors: list[SectorOut]


class LapPage(BaseModel):
    items: list[LapOut]
    total: int
    limit: int
    offset: int


class StintOut(BaseModel):
    id: UUID
    driver_abbreviation: str
    stint_number: int
    compound: str
    start_lap: int
    end_lap: int | None
    tyre_age_at_start: int | None


class TrackStatusOut(BaseModel):
    id: UUID
    race_time_ms: int
    status: TrackStatus
    source_code: str
    message: str | None
    sequence: int
