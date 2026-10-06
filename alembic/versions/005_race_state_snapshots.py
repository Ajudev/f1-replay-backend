"""Race state snapshots.

Revision ID: 005_race_state_snapshots
Revises: 004_replay_lifecycle
Create Date: 2026-10-05

Adds ``race_state_snapshots``: full race state documents persisted at meaningful
points (race start, periodically by lap, at completion, after a rebuild).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "005_race_state_snapshots"
down_revision: str | None = "004_replay_lifecycle"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "race_state_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("replay_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("race_time_ms", sa.Integer(), nullable=False),
        sa.Column("current_lap", sa.Integer(), nullable=True),
        sa.Column("trigger", sa.String(length=32), nullable=False),
        sa.Column("state_schema_version", sa.Integer(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("sequence >= 0", name="ck_race_state_snapshots_sequence_ge_0"),
        sa.CheckConstraint("race_time_ms >= 0", name="ck_race_state_snapshots_race_time_ms_ge_0"),
        sa.ForeignKeyConstraint(["replay_id"], ["replay_sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "replay_id", "run_id", "sequence", name="uq_race_state_snapshots_replay_run_sequence"
        ),
    )
    op.create_index(
        "ix_race_state_snapshots_replay_id_created_at",
        "race_state_snapshots",
        ["replay_id", "created_at"],
    )
    op.create_index(
        op.f("ix_race_state_snapshots_session_id"), "race_state_snapshots", ["session_id"]
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_race_state_snapshots_session_id"), table_name="race_state_snapshots")
    op.drop_index("ix_race_state_snapshots_replay_id_created_at", table_name="race_state_snapshots")
    op.drop_table("race_state_snapshots")
