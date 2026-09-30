"""Race (Grand Prix weekend) persistence model."""

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, Date, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.models.race_session import RaceSession


class Race(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """A Grand Prix weekend identified by season and round."""

    __tablename__ = "races"
    __table_args__ = (
        UniqueConstraint("season", "round", name="uq_races_season_round"),
        CheckConstraint("round >= 1", name="ck_races_round_ge_1"),
    )

    season: Mapped[int] = mapped_column(Integer, nullable=False)
    round: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    official_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    circuit_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country: Mapped[str | None] = mapped_column(String(128), nullable=True)
    location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    sessions: Mapped[list["RaceSession"]] = relationship(
        back_populates="race",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Race id={self.id!s} season={self.season} round={self.round}>"
