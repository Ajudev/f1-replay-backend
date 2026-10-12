"""Circuit layout imported with a session's track data."""

from typing import Any
from uuid import UUID

from sqlalchemy import Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class CircuitLayout(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """One row per session whose track data (positions, telemetry) has been imported.

    Coordinates are raw FastF1 units (1/10 m); the frontend applies ``rotation_deg``.
    """

    __tablename__ = "circuit_layouts"
    __table_args__ = (UniqueConstraint("session_id", name="uq_circuit_layouts_session_id"),)

    session_id: Mapped[UUID] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False
    )
    circuit_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    #: ``[[x, y], ...]`` in driving order.
    outline: Mapped[list[Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False
    )
    #: ``[{"number", "letter", "x", "y", "angle_deg"}, ...]``; null when unavailable.
    corners: Mapped[list[Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=True
    )
    rotation_deg: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_x: Mapped[int] = mapped_column(Integer, nullable=False)
    max_x: Mapped[int] = mapped_column(Integer, nullable=False)
    min_y: Mapped[int] = mapped_column(Integer, nullable=False)
    max_y: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_interval_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    position_sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    telemetry_sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    warnings: Mapped[list[Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False, default=list
    )
