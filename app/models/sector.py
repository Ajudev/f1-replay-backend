"""Sector persistence model."""

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.lap import Lap


class Sector(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A sector time within a lap (sector_number in 1..3)."""

    __tablename__ = "sectors"
    __table_args__ = (
        UniqueConstraint("lap_id", "sector_number", name="uq_sectors_lap_id_sector_number"),
        CheckConstraint("sector_number IN (1, 2, 3)", name="ck_sectors_sector_number"),
        CheckConstraint(
            "sector_time_ms IS NULL OR sector_time_ms >= 0",
            name="ck_sectors_sector_time_ms_ge_0",
        ),
    )

    lap_id: Mapped[UUID] = mapped_column(
        ForeignKey("laps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sector_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sector_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    lap: Mapped["Lap"] = relationship(back_populates="sectors")

    def __repr__(self) -> str:
        return f"<Sector id={self.id!s} sector={self.sector_number}>"
