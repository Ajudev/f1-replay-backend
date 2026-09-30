"""Replay session persistence model (data only; no replay behavior)."""

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.domain.enums import ReplayStatus

if TYPE_CHECKING:
    from app.models.race_session import RaceSession


class ReplaySession(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Persisted replay configuration and progress for a race session."""

    __tablename__ = "replay_sessions"
    __table_args__ = (
        CheckConstraint("playback_speed > 0", name="ck_replay_sessions_playback_speed_gt_0"),
        CheckConstraint(
            "current_race_time_ms >= 0",
            name="ck_replay_sessions_current_race_time_ms_ge_0",
        ),
        CheckConstraint(
            "current_sequence IS NULL OR current_sequence >= 0",
            name="ck_replay_sessions_current_sequence_ge_0",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[ReplayStatus] = mapped_column(
        Enum(
            ReplayStatus,
            name="replay_status",
            native_enum=False,
            length=64,
            validate_strings=True,
        ),
        nullable=False,
        default=ReplayStatus.PENDING,
        server_default=text("'PENDING'"),
    )
    playback_speed: Mapped[Decimal] = mapped_column(
        Numeric(6, 2),
        nullable=False,
        default=Decimal("1.00"),
        server_default=text("1"),
    )
    current_race_time_ms: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    current_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    session: Mapped["RaceSession"] = relationship(back_populates="replay_sessions")

    def __repr__(self) -> str:
        return f"<ReplaySession id={self.id!s} status={self.status}>"
