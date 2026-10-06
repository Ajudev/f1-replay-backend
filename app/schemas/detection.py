"""Response schemas for the detected event endpoint."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from app.domain.enums import DetectedEventType, Severity
from app.models import DetectedEvent as DetectedEventRow


class DetectedEventOut(BaseModel):
    detected_event_id: UUID
    event_type: DetectedEventType
    schema_version: int
    replay_id: UUID
    run_id: UUID
    session_id: UUID
    race_id: UUID | None
    race_time_ms: int
    lap_number: int | None
    primary_driver_id: UUID | None
    primary_driver_abbreviation: str | None
    secondary_driver_id: UUID | None
    secondary_driver_abbreviation: str | None
    severity: Severity | None
    confidence: float | None
    evidence: dict[str, Any]
    source_event_ids: list[UUID]
    source_sequence: int
    detector_name: str
    detector_version: int
    detected_at: datetime

    @classmethod
    def from_row(cls, row: DetectedEventRow) -> DetectedEventOut:
        return cls(
            detected_event_id=row.id,
            event_type=DetectedEventType(row.event_type),
            schema_version=row.schema_version,
            replay_id=row.replay_id,
            run_id=row.run_id,
            session_id=row.session_id,
            race_id=row.race_id,
            race_time_ms=row.race_time_ms,
            lap_number=row.lap_number,
            primary_driver_id=row.primary_driver_id,
            primary_driver_abbreviation=row.primary_driver_abbreviation,
            secondary_driver_id=row.secondary_driver_id,
            secondary_driver_abbreviation=row.secondary_driver_abbreviation,
            severity=Severity(row.severity) if row.severity else None,
            confidence=row.confidence,
            evidence=row.evidence,
            source_event_ids=[UUID(i) for i in row.source_event_ids],
            source_sequence=row.source_sequence,
            detector_name=row.detector_name,
            detector_version=row.detector_version,
            detected_at=row.detected_at,
        )


class DetectedEventPageOut(BaseModel):
    replay_id: UUID
    run_id: UUID | None = Field(description="Run the items belong to (latest run by default)")
    items: list[DetectedEventOut]
    total: int
    limit: int
    offset: int
