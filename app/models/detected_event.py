"""Persisted detected events (the durable, queryable record of detections)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base


class DetectedEvent(Base):
    """One detection, keyed by its deterministic id so a repeated write is a no-op."""

    __tablename__ = "detected_events"
    __table_args__ = (
        Index("ix_detected_events_replay_run_sequence", "replay_id", "run_id", "source_sequence"),
        Index("ix_detected_events_replay_event_type", "replay_id", "event_type"),
        CheckConstraint("source_sequence >= 0", name="ck_detected_events_source_sequence_ge_0"),
        CheckConstraint("race_time_ms >= 0", name="ck_detected_events_race_time_ms_ge_0"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    replay_id: Mapped[UUID] = mapped_column(
        ForeignKey("replay_sessions.id", ondelete="CASCADE"), nullable=False
    )
    # Not foreign keys: run ids are never persisted elsewhere; the session is implied by
    # the replay and kept for the record.
    run_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    session_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    race_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    lap_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    race_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    source_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    primary_driver_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    primary_driver_abbreviation: Mapped[str | None] = mapped_column(String(8), nullable=True)
    secondary_driver_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    secondary_driver_abbreviation: Mapped[str | None] = mapped_column(String(8), nullable=True)
    severity: Mapped[str | None] = mapped_column(String(16), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    evidence: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False
    )
    source_event_ids: Mapped[list[str]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False
    )
    detector_name: Mapped[str] = mapped_column(String(64), nullable=False)
    detector_version: Mapped[int] = mapped_column(Integer, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
