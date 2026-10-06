"""Persisted race state snapshots (recovery points and a fallback for reads)."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base, UUIDPrimaryKeyMixin


class RaceStateSnapshot(UUIDPrimaryKeyMixin, Base):
    """The full race state of one replay run at one event sequence.

    Written only at meaningful points (race start, every N laps, completion, after a
    rebuild), never per event.
    """

    __tablename__ = "race_state_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "replay_id", "run_id", "sequence", name="uq_race_state_snapshots_replay_run_sequence"
        ),
        Index("ix_race_state_snapshots_replay_id_created_at", "replay_id", "created_at"),
        CheckConstraint("sequence >= 0", name="ck_race_state_snapshots_sequence_ge_0"),
        CheckConstraint("race_time_ms >= 0", name="ck_race_state_snapshots_race_time_ms_ge_0"),
    )

    replay_id: Mapped[UUID] = mapped_column(
        ForeignKey("replay_sessions.id", ondelete="CASCADE"), nullable=False
    )
    # Not a foreign key: run ids are never persisted elsewhere.
    run_id: Mapped[UUID] = mapped_column(nullable=False)
    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    race_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    current_lap: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False)
    state_schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
