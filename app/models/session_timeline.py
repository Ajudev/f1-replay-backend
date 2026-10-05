"""Metadata for a generated historical race timeline."""

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.race_session import RaceSession


class SessionTimeline(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """One row per session whose timeline has been generated.

    The events themselves live in ``race_events``.
    """

    __tablename__ = "session_timelines"
    __table_args__ = (
        UniqueConstraint("session_id", name="uq_session_timelines_session_id"),
        CheckConstraint("schema_version >= 1", name="ck_session_timelines_schema_version_ge_1"),
        CheckConstraint("event_count >= 0", name="ck_session_timelines_event_count_ge_0"),
        CheckConstraint(
            "race_start_session_time_ms >= 0",
            name="ck_session_timelines_race_start_ge_0",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    race_start_session_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    warnings: Mapped[list[Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=False,
        default=list,
        server_default=text("'[]'"),
    )

    session: Mapped["RaceSession"] = relationship(back_populates="timeline")

    def __repr__(self) -> str:
        return f"<SessionTimeline id={self.id!s} events={self.event_count}>"
