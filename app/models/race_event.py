"""Race event persistence model."""

from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.domain.enums import EventType

if TYPE_CHECKING:
    from app.models.driver import Driver
    from app.models.race_session import RaceSession


class RaceEvent(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Structured machine-readable race event (no presentation strings)."""

    __tablename__ = "race_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_race_events_driver_session",
            ondelete="CASCADE",
        ),
        UniqueConstraint("session_id", "sequence", name="uq_race_events_session_id_sequence"),
        Index("ix_race_events_session_id_race_time_ms", "session_id", "race_time_ms"),
        CheckConstraint(
            "lap_number IS NULL OR lap_number >= 1",
            name="ck_race_events_lap_number_ge_1",
        ),
        CheckConstraint(
            "race_time_ms IS NULL OR race_time_ms >= 0",
            name="ck_race_events_race_time_ms_ge_0",
        ),
        CheckConstraint("sequence >= 0", name="ck_race_events_sequence_ge_0"),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[EventType] = mapped_column(
        Enum(
            EventType,
            name="event_type",
            native_enum=False,
            length=64,
            validate_strings=True,
        ),
        nullable=False,
    )
    driver_id: Mapped[UUID | None] = mapped_column(nullable=True, index=True)
    lap_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    race_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=False,
        default=dict,
        server_default=text("'{}'"),
    )

    session: Mapped["RaceSession"] = relationship(
        back_populates="race_events",
        overlaps="driver",
    )
    driver: Mapped["Driver | None"] = relationship(overlaps="session,race_events")

    def __repr__(self) -> str:
        return f"<RaceEvent id={self.id!s} type={self.event_type} seq={self.sequence}>"
