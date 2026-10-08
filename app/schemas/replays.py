"""Request/response schemas for replay sessions."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from app.domain.enums import ReplayStatus
from app.replay.service import ReplayView
from app.replay.state import SUPPORTED_PLAYBACK_SPEEDS

_SPEEDS = ", ".join(str(s) for s in SUPPORTED_PLAYBACK_SPEEDS)


class ReplayCreateRequest(BaseModel):
    session_id: UUID = Field(description="Session to replay (its timeline must be generated)")
    playback_speed: Decimal = Field(
        default=Decimal("1"), gt=0, allow_inf_nan=False, description=f"Supported: {_SPEEDS}"
    )


class ReplaySpeedRequest(BaseModel):
    playback_speed: Decimal = Field(gt=0, allow_inf_nan=False, description=f"Supported: {_SPEEDS}")


class ReplayResponse(BaseModel):
    id: UUID
    session_id: UUID
    race_id: UUID
    status: ReplayStatus
    is_completed: bool = Field(description="True once the whole timeline has been replayed")
    status_reason: str | None
    playback_speed: float
    current_race_time_ms: int = Field(description="Virtual race clock position")
    current_sequence: int | None = Field(description="Sequence of the last emitted event")
    emitted_event_count: int
    total_events: int | None
    current_lap: int | None = Field(description="Race lap the leader is on")
    total_laps: int | None
    created_at: datetime
    started_at: datetime | None
    paused_at: datetime | None
    ended_at: datetime | None

    @classmethod
    def from_view(cls, view: ReplayView) -> ReplayResponse:
        state = view.state
        return cls(
            id=state.replay_id,
            session_id=state.session_id,
            race_id=view.race_id,
            status=state.status,
            is_completed=state.status is ReplayStatus.COMPLETED,
            status_reason=state.status_reason,
            playback_speed=float(state.playback_speed),
            current_race_time_ms=state.current_race_time_ms,
            current_sequence=state.current_sequence,
            emitted_event_count=state.emitted_event_count,
            total_events=state.total_events,
            current_lap=state.current_lap,
            total_laps=state.total_laps,
            created_at=view.created_at,
            started_at=state.started_at,
            paused_at=state.paused_at,
            ended_at=state.ended_at,
        )
