"""Add lap completion time and session timeline metadata.

Revision ID: 003_race_timeline
Revises: 002_ingestion_support
Create Date: 2026-10-01

Adds ``laps.lap_end_time_ms`` and the ``session_timelines`` table. The
``race_events.event_type`` column is a non-native enum (VARCHAR(64) without a
CHECK constraint), so the new RACE_STARTED value needs no DDL.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "003_race_timeline"
down_revision: str | None = "002_ingestion_support"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("laps", sa.Column("lap_end_time_ms", sa.Integer(), nullable=True))
    op.create_check_constraint(
        "ck_laps_lap_end_time_ms_ge_0",
        "laps",
        "lap_end_time_ms IS NULL OR lap_end_time_ms >= 0",
    )

    op.create_table(
        "session_timelines",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("race_start_session_time_ms", sa.Integer(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column(
            "warnings",
            sa.JSON().with_variant(postgresql.JSONB(), "postgresql"),
            nullable=False,
            server_default=sa.text("'[]'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "schema_version >= 1",
            name="ck_session_timelines_schema_version_ge_1",
        ),
        sa.CheckConstraint("event_count >= 0", name="ck_session_timelines_event_count_ge_0"),
        sa.CheckConstraint(
            "race_start_session_time_ms >= 0",
            name="ck_session_timelines_race_start_ge_0",
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", name="uq_session_timelines_session_id"),
    )


def downgrade() -> None:
    op.drop_table("session_timelines")
    op.drop_constraint("ck_laps_lap_end_time_ms_ge_0", "laps", type_="check")
    op.drop_column("laps", "lap_end_time_ms")
