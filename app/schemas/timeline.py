"""Request/response schemas for the historical race timeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

from app.domain.enums import EventType


class TimelineGenerateRequest(BaseModel):
    regenerate: bool = False


class TimelineSummaryResponse(BaseModel):
    status: Literal["generated", "regenerated", "already_generated", "available"]
    session_id: UUID
    schema_version: int
    is_current_schema_version: bool
    generated_at: datetime
    race_start_session_time_ms: int
    event_count: int
    counts_by_type: dict[str, int]
    warnings: list[str]


class TimelineEventOut(BaseModel):
    id: UUID
    sequence: int
    event_type: EventType
    race_time_ms: int | None
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None
    payload: dict[str, Any]


class TimelineEventPage(BaseModel):
    items: list[TimelineEventOut]
    total: int
    limit: int
    offset: int
