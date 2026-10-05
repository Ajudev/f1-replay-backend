"""Track status period persistence model."""

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.domain.enums import TrackStatus

if TYPE_CHECKING:
    from app.models.race_session import RaceSession


class TrackStatusPeriod(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Session-scoped interval start for a track status change.

    This is a normalized fact, not a RaceEvent.
    """

    __tablename__ = "track_status_periods"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "sequence",
            name="uq_track_status_periods_session_id_sequence",
        ),
        CheckConstraint(
            "race_time_ms >= 0",
            name="ck_track_status_periods_race_time_ms_ge_0",
        ),
        CheckConstraint(
            "sequence >= 0",
            name="ck_track_status_periods_sequence_ge_0",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    race_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[TrackStatus] = mapped_column(
        Enum(
            TrackStatus,
            name="track_status",
            native_enum=False,
            length=64,
            validate_strings=True,
        ),
        nullable=False,
    )
    source_code: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)

    session: Mapped["RaceSession"] = relationship(back_populates="track_status_periods")

    def __repr__(self) -> str:
        return f"<TrackStatusPeriod id={self.id!s} status={self.status} seq={self.sequence}>"
