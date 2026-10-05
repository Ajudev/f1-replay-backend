"""Replay lifecycle columns and status values.

Revision ID: 004_replay_lifecycle
Revises: 003_race_timeline
Create Date: 2026-10-05

Renames the initial replay status ``PENDING`` to ``CREATED`` and adds the
``FAILED`` status. ``replay_sessions.status`` is a non-native enum
(VARCHAR(64) without a CHECK constraint), so only data and the server default
change. Adds lap/progress tracking, end timestamp and a status reason.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "004_replay_lifecycle"
down_revision: str | None = "003_race_timeline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("UPDATE replay_sessions SET status = 'CREATED' WHERE status = 'PENDING'")
    op.alter_column("replay_sessions", "status", server_default=sa.text("'CREATED'"))

    op.add_column("replay_sessions", sa.Column("current_lap", sa.Integer(), nullable=True))
    op.add_column("replay_sessions", sa.Column("total_events", sa.Integer(), nullable=True))
    op.add_column("replay_sessions", sa.Column("total_laps", sa.Integer(), nullable=True))
    op.add_column(
        "replay_sessions",
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column("replay_sessions", sa.Column("status_reason", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_replay_sessions_current_lap_ge_1",
        "replay_sessions",
        "current_lap IS NULL OR current_lap >= 1",
    )


def downgrade() -> None:
    op.drop_constraint("ck_replay_sessions_current_lap_ge_1", "replay_sessions", type_="check")
    op.drop_column("replay_sessions", "status_reason")
    op.drop_column("replay_sessions", "ended_at")
    op.drop_column("replay_sessions", "total_laps")
    op.drop_column("replay_sessions", "total_events")
    op.drop_column("replay_sessions", "current_lap")

    # FAILED has no earlier equivalent; STOPPED is the closest terminal state.
    op.execute("UPDATE replay_sessions SET status = 'STOPPED' WHERE status = 'FAILED'")
    op.execute("UPDATE replay_sessions SET status = 'PENDING' WHERE status = 'CREATED'")
    op.alter_column("replay_sessions", "status", server_default=sa.text("'PENDING'"))
