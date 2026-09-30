"""Tyre stint persistence model."""

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.driver import Driver
    from app.models.race_session import RaceSession


class TyreStint(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A continuous tyre stint for a driver within a session."""

    __tablename__ = "tyre_stints"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_tyre_stints_driver_session",
            ondelete="CASCADE",
        ),
        UniqueConstraint("driver_id", "stint_number", name="uq_tyre_stints_driver_id_stint_number"),
        CheckConstraint("stint_number >= 1", name="ck_tyre_stints_stint_number_ge_1"),
        CheckConstraint("start_lap >= 1", name="ck_tyre_stints_start_lap_ge_1"),
        CheckConstraint(
            "end_lap IS NULL OR end_lap >= 1",
            name="ck_tyre_stints_end_lap_ge_1",
        ),
        CheckConstraint(
            "end_lap IS NULL OR end_lap >= start_lap",
            name="ck_tyre_stints_end_lap_ge_start_lap",
        ),
        CheckConstraint(
            "tyre_age_at_start IS NULL OR tyre_age_at_start >= 0",
            name="ck_tyre_stints_tyre_age_at_start_ge_0",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    driver_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    stint_number: Mapped[int] = mapped_column(Integer, nullable=False)
    compound: Mapped[str] = mapped_column(String(32), nullable=False)
    start_lap: Mapped[int] = mapped_column(Integer, nullable=False)
    end_lap: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tyre_age_at_start: Mapped[int | None] = mapped_column(Integer, nullable=True)

    driver: Mapped["Driver"] = relationship(
        back_populates="tyre_stints",
        overlaps="session,tyre_stints",
    )
    session: Mapped["RaceSession"] = relationship(
        back_populates="tyre_stints",
        overlaps="driver,tyre_stints",
    )

    def __repr__(self) -> str:
        return f"<TyreStint id={self.id!s} stint={self.stint_number}>"
