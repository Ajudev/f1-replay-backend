"""RaceSession persistence model (table ``sessions``)."""

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, Enum, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.domain.enums import SessionType

if TYPE_CHECKING:
    from app.models.driver import Driver
    from app.models.lap import Lap
    from app.models.race import Race
    from app.models.race_event import RaceEvent
    from app.models.replay_session import ReplaySession
    from app.models.tyre_stint import TyreStint


class RaceSession(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A practice, qualifying, sprint, or race session within a weekend."""

    __tablename__ = "sessions"
    __table_args__ = (
        UniqueConstraint("race_id", "session_type", name="uq_sessions_race_id_session_type"),
    )

    race_id: Mapped[UUID] = mapped_column(
        ForeignKey("races.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    session_type: Mapped[SessionType] = mapped_column(
        Enum(
            SessionType,
            name="session_type",
            native_enum=False,
            length=64,
            validate_strings=True,
        ),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    race: Mapped["Race"] = relationship(back_populates="sessions")
    drivers: Mapped[list["Driver"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
    )
    laps: Mapped[list["Lap"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
        overlaps="driver,laps",
    )
    tyre_stints: Mapped[list["TyreStint"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
        overlaps="driver,tyre_stints",
    )
    race_events: Mapped[list["RaceEvent"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
        overlaps="driver",
    )
    replay_sessions: Mapped[list["ReplaySession"]] = relationship(
        back_populates="session",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<RaceSession id={self.id!s} type={self.session_type}>"
