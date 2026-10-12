"""A driver's car position and telemetry at one point of the race-time grid."""

from uuid import UUID

from sqlalchemy import (
    Boolean,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DriverTrackSample(Base):
    """One grid point (``race_time_ms``) of one driver; position and telemetry are
    independently nullable because either source may be missing at that time."""

    __tablename__ = "driver_track_samples"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_driver_track_samples_driver_session",
            ondelete="CASCADE",
        ),
        # Lap traces; whole-session loads use the primary key order.
        Index(
            "ix_driver_track_samples_session_driver_lap", "session_id", "driver_id", "lap_number"
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"), primary_key=True
    )
    driver_id: Mapped[UUID] = mapped_column(primary_key=True)
    #: Race time (0 = race start); slightly negative before the start.
    race_time_ms: Mapped[int] = mapped_column(Integer, primary_key=True)
    lap_number: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)

    x: Mapped[int | None] = mapped_column(Integer, nullable=True)
    y: Mapped[int | None] = mapped_column(Integer, nullable=True)
    off_track: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    speed_kph: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    rpm: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gear: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    throttle_pct: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    brake: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    #: Raw FastF1 ``DRS`` code; mapped with ``DrsState.from_code`` when read.
    drs_code: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
