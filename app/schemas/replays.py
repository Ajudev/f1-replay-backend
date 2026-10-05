"""Request/response schemas for replay sessions."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from app.domain.enums import ReplayStatus


class ReplayCreateRequest(BaseModel):
    session_id: UUID
    playback_speed: Decimal = Field(default=Decimal("1"), description="Supported: 1, 2, 5, 10, 20")


class ReplaySpeedRequest(BaseModel):
    playback_speed: Decimal = Field(description="Supported: 1, 2, 5, 10, 20")


class ReplayResponse(BaseModel):
    id: UUID
    session_id: UUID
    race_id: UUID
    status: ReplayStatus
    status_reason: str | None
    playback_speed: float
    current_race_time_ms: int
    current_sequence: int | None = Field(description="Sequence of the last emitted event")
    emitted_event_count: int
    total_events: int | None
    current_lap: int | None = Field(description="Race lap the leader is on")
    total_laps: int | None
    created_at: datetime
    started_at: datetime | None
    paused_at: datetime | None
    ended_at: datetime | None
