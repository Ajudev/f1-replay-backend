"""Lap persistence model."""

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    false,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.driver import Driver
    from app.models.race_session import RaceSession
    from app.models.sector import Sector


class Lap(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A single lap completed by a driver in a session."""

    __tablename__ = "laps"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_laps_driver_session",
            ondelete="CASCADE",
        ),
        UniqueConstraint("driver_id", "lap_number", name="uq_laps_driver_id_lap_number"),
        Index("ix_laps_session_id_lap_number", "session_id", "lap_number"),
        CheckConstraint("lap_number >= 1", name="ck_laps_lap_number_ge_1"),
        CheckConstraint(
            "lap_time_ms IS NULL OR lap_time_ms >= 0",
            name="ck_laps_lap_time_ms_ge_0",
        ),
        CheckConstraint(
            "position IS NULL OR position >= 1",
            name="ck_laps_position_ge_1",
        ),
        CheckConstraint(
            "tyre_age_laps IS NULL OR tyre_age_laps >= 0",
            name="ck_laps_tyre_age_laps_ge_0",
        ),
        CheckConstraint(
            "pit_duration_ms IS NULL OR pit_duration_ms >= 0",
            name="ck_laps_pit_duration_ms_ge_0",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    driver_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    lap_number: Mapped[int] = mapped_column(Integer, nullable=False)
    lap_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    compound: Mapped[str | None] = mapped_column(String(32), nullable=True)
    tyre_age_laps: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_pit_in_lap: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    is_pit_out_lap: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    pit_duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    driver: Mapped["Driver"] = relationship(back_populates="laps", overlaps="laps,session")
    session: Mapped["RaceSession"] = relationship(
        back_populates="laps",
        overlaps="driver,laps",
    )
    sectors: Mapped[list["Sector"]] = relationship(
        back_populates="lap",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Lap id={self.id!s} lap={self.lap_number}>"
