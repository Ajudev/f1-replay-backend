"""Driver participation persistence model."""

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.lap import Lap
    from app.models.race_session import RaceSession
    from app.models.tyre_stint import TyreStint


class Driver(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A driver's participation in one session (not a global career identity)."""

    __tablename__ = "drivers"
    __table_args__ = (
        UniqueConstraint("session_id", "abbreviation", name="uq_drivers_session_id_abbreviation"),
        UniqueConstraint("id", "session_id", name="uq_drivers_id_session_id"),
        Index(
            "uq_drivers_session_id_driver_number",
            "session_id",
            "driver_number",
            unique=True,
            postgresql_where=text("driver_number IS NOT NULL"),
            sqlite_where=text("driver_number IS NOT NULL"),
        ),
        CheckConstraint(
            "length(abbreviation) = 3",
            name="ck_drivers_abbreviation_length",
        ),
        CheckConstraint(
            "grid_position IS NULL OR grid_position >= 1",
            name="ck_drivers_grid_position_ge_1",
        ),
        CheckConstraint(
            "finish_position IS NULL OR finish_position >= 1",
            name="ck_drivers_finish_position_ge_1",
        ),
    )

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    driver_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    abbreviation: Mapped[str] = mapped_column(String(3), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    team_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    grid_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    finish_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_status: Mapped[str | None] = mapped_column(String(64), nullable=True)

    session: Mapped["RaceSession"] = relationship(back_populates="drivers")
    laps: Mapped[list["Lap"]] = relationship(
        back_populates="driver",
        overlaps="laps,session",
    )
    tyre_stints: Mapped[list["TyreStint"]] = relationship(
        back_populates="driver",
        overlaps="session,tyre_stints",
    )

    @validates("abbreviation")
    def _normalize_abbreviation(self, _key: str, value: str) -> str:
        if len(value) != 3:
            raise ValueError("abbreviation must be exactly 3 characters")
        return value.upper()

    def __repr__(self) -> str:
        return f"<Driver id={self.id!s} abbr={self.abbreviation}>"
