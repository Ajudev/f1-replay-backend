"""Track data: circuit layouts, driver track samples and team colours.

Revision ID: 007_track_data
Revises: 006_detected_events
Create Date: 2026-10-12

Adds ``drivers.team_color`` (nullable, metadata-only change), ``circuit_layouts`` (one
row per session with imported track data) and ``driver_track_samples`` (car position and
telemetry per driver on a fixed race-time grid). Both tables are new and empty, so their
indexes are created inline.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "007_track_data"
down_revision: str | None = "006_detected_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("drivers", sa.Column("team_color", sa.String(length=6), nullable=True))

    op.create_table(
        "circuit_layouts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("circuit_name", sa.String(length=255), nullable=True),
        sa.Column("outline", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("corners", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("rotation_deg", sa.Float(), nullable=True),
        sa.Column("min_x", sa.Integer(), nullable=False),
        sa.Column("max_x", sa.Integer(), nullable=False),
        sa.Column("min_y", sa.Integer(), nullable=False),
        sa.Column("max_y", sa.Integer(), nullable=False),
        sa.Column("sample_interval_ms", sa.Integer(), nullable=False),
        sa.Column("position_sample_count", sa.Integer(), nullable=False),
        sa.Column("telemetry_sample_count", sa.Integer(), nullable=False),
        sa.Column("warnings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", name="uq_circuit_layouts_session_id"),
    )

    op.create_table(
        "driver_track_samples",
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("driver_id", sa.Uuid(), nullable=False),
        sa.Column("race_time_ms", sa.Integer(), nullable=False),
        sa.Column("lap_number", sa.SmallInteger(), nullable=True),
        sa.Column("x", sa.Integer(), nullable=True),
        sa.Column("y", sa.Integer(), nullable=True),
        sa.Column("off_track", sa.Boolean(), nullable=True),
        sa.Column("speed_kph", sa.SmallInteger(), nullable=True),
        sa.Column("rpm", sa.Integer(), nullable=True),
        sa.Column("gear", sa.SmallInteger(), nullable=True),
        sa.Column("throttle_pct", sa.SmallInteger(), nullable=True),
        sa.Column("brake", sa.Boolean(), nullable=True),
        sa.Column("drs_code", sa.SmallInteger(), nullable=True),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_driver_track_samples_driver_session",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("session_id", "driver_id", "race_time_ms"),
    )
    op.create_index(
        "ix_driver_track_samples_session_driver_lap",
        "driver_track_samples",
        ["session_id", "driver_id", "lap_number"],
    )


def downgrade() -> None:
    op.drop_index("ix_driver_track_samples_session_driver_lap", table_name="driver_track_samples")
    op.drop_table("driver_track_samples")
    op.drop_table("circuit_layouts")
    op.drop_column("drivers", "team_color")
