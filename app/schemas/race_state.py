"""Response schemas for the race state endpoints."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from app.domain.enums import ReplayStatus, TrackStatus
from app.race_state.models import (
    DriverState,
    FastestLap,
    RacePhase,
    RaceState,
    StateSource,
)


class RaceStateResponse(BaseModel):
    replay_id: UUID
    replay_status: ReplayStatus
    source: StateSource = Field(description="'live' (Redis) or 'snapshot' (PostgreSQL fallback)")
    run_id: UUID
    session_id: UUID
    race_id: UUID | None
    season: int | None
    round: int | None
    session_type: str | None
    phase: RacePhase
    current_race_time_ms: int
    current_lap: int | None = Field(description="Lap the leader is on")
    total_laps: int | None
    leader_laps_completed: int
    leader_driver_id: UUID | None
    leader_driver_abbreviation: str | None
    track_status: TrackStatus | None
    fastest_lap: FastestLap | None
    last_sequence: int = Field(description="Sequence of the last applied timeline event")
    last_event_id: UUID | None
    total_events: int
    updated_at: datetime | None
    drivers: list[DriverState] = Field(description="Ordered by position")

    @classmethod
    def build(
        cls, state: RaceState, source: StateSource, replay_status: ReplayStatus
    ) -> RaceStateResponse:
        return cls(
            replay_id=state.replay_id,
            replay_status=replay_status,
            source=source,
            run_id=state.run_id,
            session_id=state.session_id,
            race_id=state.race_id,
            season=state.season,
            round=state.round,
            session_type=state.session_type,
            phase=state.phase,
            current_race_time_ms=state.current_race_time_ms,
            current_lap=state.current_lap,
            total_laps=state.total_laps,
            leader_laps_completed=state.leader_laps_completed,
            leader_driver_id=state.leader_driver_id,
            leader_driver_abbreviation=state.leader_driver_abbreviation,
            track_status=state.track_status,
            fastest_lap=state.fastest_lap,
            last_sequence=state.last_sequence,
            last_event_id=state.last_event_id,
            total_events=state.total_events,
            updated_at=state.updated_at,
            drivers=state.drivers_by_position(),
        )


class DriverStateResponse(BaseModel):
    replay_id: UUID
    replay_status: ReplayStatus
    source: StateSource
    phase: RacePhase
    current_race_time_ms: int
    current_lap: int | None
    last_sequence: int
    driver: DriverState
