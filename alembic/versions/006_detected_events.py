"""Detected events.

Revision ID: 006_detected_events
Revises: 005_race_state_snapshots
Create Date: 2026-10-06

Adds ``detected_events``: the structured, evidence-backed detections produced by the
event detection engine. The primary key is the deterministic detected event id, which
makes writes idempotent.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "006_detected_events"
down_revision: str | None = "005_race_state_snapshots"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "detected_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("replay_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("race_id", sa.Uuid(), nullable=True),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("lap_number", sa.Integer(), nullable=True),
        sa.Column("race_time_ms", sa.Integer(), nullable=False),
        sa.Column("source_sequence", sa.Integer(), nullable=False),
        sa.Column("primary_driver_id", sa.Uuid(), nullable=True),
        sa.Column("primary_driver_abbreviation", sa.String(length=8), nullable=True),
        sa.Column("secondary_driver_id", sa.Uuid(), nullable=True),
        sa.Column("secondary_driver_abbreviation", sa.String(length=8), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_event_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("detector_name", sa.String(length=64), nullable=False),
        sa.Column("detector_version", sa.Integer(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("source_sequence >= 0", name="ck_detected_events_source_sequence_ge_0"),
        sa.CheckConstraint("race_time_ms >= 0", name="ck_detected_events_race_time_ms_ge_0"),
        sa.ForeignKeyConstraint(["replay_id"], ["replay_sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_detected_events_replay_run_sequence",
        "detected_events",
        ["replay_id", "run_id", "source_sequence"],
    )
    op.create_index(
        "ix_detected_events_replay_event_type", "detected_events", ["replay_id", "event_type"]
    )


def downgrade() -> None:
    op.drop_index("ix_detected_events_replay_event_type", table_name="detected_events")
    op.drop_index("ix_detected_events_replay_run_sequence", table_name="detected_events")
    op.drop_table("detected_events")
