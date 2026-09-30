"""Add ingestion support columns and track_status_periods.

Revision ID: 002_ingestion_support
Revises: 001_initial_schema
Create Date: 2026-09-30

Adds race official_name, driver result identity fields, lap pit/stint
metadata, and the track_status_periods table.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "002_ingestion_support"
down_revision: str | None = "001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("races", sa.Column("official_name", sa.String(length=255), nullable=True))

    op.add_column("drivers", sa.Column("first_name", sa.String(length=128), nullable=True))
    op.add_column("drivers", sa.Column("last_name", sa.String(length=128), nullable=True))
    op.add_column("drivers", sa.Column("team_name", sa.String(length=128), nullable=True))
    op.add_column("drivers", sa.Column("grid_position", sa.Integer(), nullable=True))
    op.add_column("drivers", sa.Column("finish_position", sa.Integer(), nullable=True))
    op.add_column("drivers", sa.Column("result_status", sa.String(length=64), nullable=True))
    op.create_check_constraint(
        "ck_drivers_grid_position_ge_1",
        "drivers",
        "grid_position IS NULL OR grid_position >= 1",
    )
    op.create_check_constraint(
        "ck_drivers_finish_position_ge_1",
        "drivers",
        "finish_position IS NULL OR finish_position >= 1",
    )

    op.add_column("laps", sa.Column("stint_number", sa.Integer(), nullable=True))
    op.add_column("laps", sa.Column("is_deleted", sa.Boolean(), nullable=True))
    op.add_column("laps", sa.Column("is_accurate", sa.Boolean(), nullable=True))
    op.add_column("laps", sa.Column("lap_start_time_ms", sa.Integer(), nullable=True))
    op.add_column("laps", sa.Column("pit_in_time_ms", sa.Integer(), nullable=True))
    op.add_column("laps", sa.Column("pit_out_time_ms", sa.Integer(), nullable=True))
    op.create_check_constraint(
        "ck_laps_stint_number_ge_1",
        "laps",
        "stint_number IS NULL OR stint_number >= 1",
    )
    op.create_check_constraint(
        "ck_laps_lap_start_time_ms_ge_0",
        "laps",
        "lap_start_time_ms IS NULL OR lap_start_time_ms >= 0",
    )
    op.create_check_constraint(
        "ck_laps_pit_in_time_ms_ge_0",
        "laps",
        "pit_in_time_ms IS NULL OR pit_in_time_ms >= 0",
    )
    op.create_check_constraint(
        "ck_laps_pit_out_time_ms_ge_0",
        "laps",
        "pit_out_time_ms IS NULL OR pit_out_time_ms >= 0",
    )

    op.create_table(
        "track_status_periods",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("race_time_ms", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "GREEN",
                "YELLOW",
                "SAFETY_CAR",
                "VIRTUAL_SAFETY_CAR",
                "VIRTUAL_SAFETY_CAR_ENDING",
                "RED_FLAG",
                "UNKNOWN",
                name="track_status",
                native_enum=False,
                length=64,
            ),
            nullable=False,
        ),
        sa.Column("source_code", sa.String(length=16), nullable=False),
        sa.Column("message", sa.String(length=255), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
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
            "race_time_ms >= 0",
            name="ck_track_status_periods_race_time_ms_ge_0",
        ),
        sa.CheckConstraint(
            "sequence >= 0",
            name="ck_track_status_periods_sequence_ge_0",
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id",
            "sequence",
            name="uq_track_status_periods_session_id_sequence",
        ),
    )


def downgrade() -> None:
    op.drop_table("track_status_periods")

    op.drop_constraint("ck_laps_pit_out_time_ms_ge_0", "laps", type_="check")
    op.drop_constraint("ck_laps_pit_in_time_ms_ge_0", "laps", type_="check")
    op.drop_constraint("ck_laps_lap_start_time_ms_ge_0", "laps", type_="check")
    op.drop_constraint("ck_laps_stint_number_ge_1", "laps", type_="check")
    op.drop_column("laps", "pit_out_time_ms")
    op.drop_column("laps", "pit_in_time_ms")
    op.drop_column("laps", "lap_start_time_ms")
    op.drop_column("laps", "is_accurate")
    op.drop_column("laps", "is_deleted")
    op.drop_column("laps", "stint_number")

    op.drop_constraint("ck_drivers_finish_position_ge_1", "drivers", type_="check")
    op.drop_constraint("ck_drivers_grid_position_ge_1", "drivers", type_="check")
    op.drop_column("drivers", "result_status")
    op.drop_column("drivers", "finish_position")
    op.drop_column("drivers", "grid_position")
    op.drop_column("drivers", "team_name")
    op.drop_column("drivers", "last_name")
    op.drop_column("drivers", "first_name")

    op.drop_column("races", "official_name")
