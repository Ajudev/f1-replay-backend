"""Initial Phase 1 schema.

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-09-30

Creates races, sessions, drivers, laps, sectors, tyre_stints, race_events,
and replay_sessions with constraints, indexes, and foreign keys.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "races",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("circuit_name", sa.String(length=255), nullable=True),
        sa.Column("country", sa.String(length=128), nullable=True),
        sa.Column("location", sa.String(length=255), nullable=True),
        sa.Column("event_date", sa.Date(), nullable=True),
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
        sa.CheckConstraint("round >= 1", name="ck_races_round_ge_1"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("season", "round", name="uq_races_season_round"),
    )

    op.create_table(
        "sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("race_id", sa.Uuid(), nullable=False),
        sa.Column(
            "session_type",
            sa.Enum(
                "PRACTICE_1",
                "PRACTICE_2",
                "PRACTICE_3",
                "QUALIFYING",
                "SPRINT_QUALIFYING",
                "SPRINT",
                "RACE",
                name="session_type",
                native_enum=False,
                length=64,
            ),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
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
        sa.ForeignKeyConstraint(["race_id"], ["races.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("race_id", "session_type", name="uq_sessions_race_id_session_type"),
    )
    op.create_index(op.f("ix_sessions_race_id"), "sessions", ["race_id"], unique=False)

    op.create_table(
        "drivers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("driver_number", sa.Integer(), nullable=True),
        sa.Column("abbreviation", sa.String(length=3), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=False),
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
        sa.CheckConstraint("length(abbreviation) = 3", name="ck_drivers_abbreviation_length"),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "session_id", name="uq_drivers_id_session_id"),
        sa.UniqueConstraint(
            "session_id",
            "abbreviation",
            name="uq_drivers_session_id_abbreviation",
        ),
    )
    op.create_index(op.f("ix_drivers_session_id"), "drivers", ["session_id"], unique=False)
    op.create_index(
        "uq_drivers_session_id_driver_number",
        "drivers",
        ["session_id", "driver_number"],
        unique=True,
        postgresql_where=sa.text("driver_number IS NOT NULL"),
        sqlite_where=sa.text("driver_number IS NOT NULL"),
    )

    op.create_table(
        "laps",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("driver_id", sa.Uuid(), nullable=False),
        sa.Column("lap_number", sa.Integer(), nullable=False),
        sa.Column("lap_time_ms", sa.Integer(), nullable=True),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("compound", sa.String(length=32), nullable=True),
        sa.Column("tyre_age_laps", sa.Integer(), nullable=True),
        sa.Column(
            "is_pit_in_lap",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "is_pit_out_lap",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("pit_duration_ms", sa.Integer(), nullable=True),
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
        sa.CheckConstraint("lap_number >= 1", name="ck_laps_lap_number_ge_1"),
        sa.CheckConstraint(
            "lap_time_ms IS NULL OR lap_time_ms >= 0",
            name="ck_laps_lap_time_ms_ge_0",
        ),
        sa.CheckConstraint(
            "position IS NULL OR position >= 1",
            name="ck_laps_position_ge_1",
        ),
        sa.CheckConstraint(
            "tyre_age_laps IS NULL OR tyre_age_laps >= 0",
            name="ck_laps_tyre_age_laps_ge_0",
        ),
        sa.CheckConstraint(
            "pit_duration_ms IS NULL OR pit_duration_ms >= 0",
            name="ck_laps_pit_duration_ms_ge_0",
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_laps_driver_session",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("driver_id", "lap_number", name="uq_laps_driver_id_lap_number"),
    )
    op.create_index(op.f("ix_laps_session_id"), "laps", ["session_id"], unique=False)
    op.create_index(op.f("ix_laps_driver_id"), "laps", ["driver_id"], unique=False)
    op.create_index("ix_laps_session_id_lap_number", "laps", ["session_id", "lap_number"])

    op.create_table(
        "sectors",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("lap_id", sa.Uuid(), nullable=False),
        sa.Column("sector_number", sa.Integer(), nullable=False),
        sa.Column("sector_time_ms", sa.Integer(), nullable=True),
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
        sa.CheckConstraint("sector_number IN (1, 2, 3)", name="ck_sectors_sector_number"),
        sa.CheckConstraint(
            "sector_time_ms IS NULL OR sector_time_ms >= 0",
            name="ck_sectors_sector_time_ms_ge_0",
        ),
        sa.ForeignKeyConstraint(["lap_id"], ["laps.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lap_id", "sector_number", name="uq_sectors_lap_id_sector_number"),
    )
    op.create_index(op.f("ix_sectors_lap_id"), "sectors", ["lap_id"], unique=False)

    op.create_table(
        "tyre_stints",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("driver_id", sa.Uuid(), nullable=False),
        sa.Column("stint_number", sa.Integer(), nullable=False),
        sa.Column("compound", sa.String(length=32), nullable=False),
        sa.Column("start_lap", sa.Integer(), nullable=False),
        sa.Column("end_lap", sa.Integer(), nullable=True),
        sa.Column("tyre_age_at_start", sa.Integer(), nullable=True),
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
        sa.CheckConstraint("stint_number >= 1", name="ck_tyre_stints_stint_number_ge_1"),
        sa.CheckConstraint("start_lap >= 1", name="ck_tyre_stints_start_lap_ge_1"),
        sa.CheckConstraint(
            "end_lap IS NULL OR end_lap >= 1",
            name="ck_tyre_stints_end_lap_ge_1",
        ),
        sa.CheckConstraint(
            "end_lap IS NULL OR end_lap >= start_lap",
            name="ck_tyre_stints_end_lap_ge_start_lap",
        ),
        sa.CheckConstraint(
            "tyre_age_at_start IS NULL OR tyre_age_at_start >= 0",
            name="ck_tyre_stints_tyre_age_at_start_ge_0",
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_tyre_stints_driver_session",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "driver_id",
            "stint_number",
            name="uq_tyre_stints_driver_id_stint_number",
        ),
    )
    op.create_index(op.f("ix_tyre_stints_session_id"), "tyre_stints", ["session_id"], unique=False)
    op.create_index(op.f("ix_tyre_stints_driver_id"), "tyre_stints", ["driver_id"], unique=False)

    op.create_table(
        "race_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column(
            "event_type",
            sa.Enum(
                "LAP_COMPLETED",
                "SECTOR_COMPLETED",
                "POSITION_CHANGED",
                "PIT_ENTRY",
                "PIT_EXIT",
                "FASTEST_LAP",
                "TRACK_STATUS_CHANGED",
                "BATTLE_FORMING",
                "PACE_DEGRADATION",
                "PACE_ANOMALY",
                name="event_type",
                native_enum=False,
                length=64,
            ),
            nullable=False,
        ),
        sa.Column("driver_id", sa.Uuid(), nullable=True),
        sa.Column("lap_number", sa.Integer(), nullable=True),
        sa.Column("race_time_ms", sa.Integer(), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column(
            "payload",
            sa.JSON().with_variant(postgresql.JSONB(), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'"),
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
            "lap_number IS NULL OR lap_number >= 1",
            name="ck_race_events_lap_number_ge_1",
        ),
        sa.CheckConstraint(
            "race_time_ms IS NULL OR race_time_ms >= 0",
            name="ck_race_events_race_time_ms_ge_0",
        ),
        sa.CheckConstraint("sequence >= 0", name="ck_race_events_sequence_ge_0"),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["driver_id", "session_id"],
            ["drivers.id", "drivers.session_id"],
            name="fk_race_events_driver_session",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id",
            "sequence",
            name="uq_race_events_session_id_sequence",
        ),
    )
    op.create_index(op.f("ix_race_events_session_id"), "race_events", ["session_id"], unique=False)
    op.create_index(op.f("ix_race_events_driver_id"), "race_events", ["driver_id"], unique=False)
    op.create_index(
        "ix_race_events_session_id_race_time_ms",
        "race_events",
        ["session_id", "race_time_ms"],
    )

    op.create_table(
        "replay_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "RUNNING",
                "PAUSED",
                "COMPLETED",
                "STOPPED",
                name="replay_status",
                native_enum=False,
                length=64,
            ),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "playback_speed",
            sa.Numeric(precision=6, scale=2),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "current_race_time_ms",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("current_sequence", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
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
            "playback_speed > 0",
            name="ck_replay_sessions_playback_speed_gt_0",
        ),
        sa.CheckConstraint(
            "current_race_time_ms >= 0",
            name="ck_replay_sessions_current_race_time_ms_ge_0",
        ),
        sa.CheckConstraint(
            "current_sequence IS NULL OR current_sequence >= 0",
            name="ck_replay_sessions_current_sequence_ge_0",
        ),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_replay_sessions_session_id"),
        "replay_sessions",
        ["session_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_replay_sessions_session_id"), table_name="replay_sessions")
    op.drop_table("replay_sessions")

    op.drop_index("ix_race_events_session_id_race_time_ms", table_name="race_events")
    op.drop_index(op.f("ix_race_events_driver_id"), table_name="race_events")
    op.drop_index(op.f("ix_race_events_session_id"), table_name="race_events")
    op.drop_table("race_events")

    op.drop_index(op.f("ix_tyre_stints_driver_id"), table_name="tyre_stints")
    op.drop_index(op.f("ix_tyre_stints_session_id"), table_name="tyre_stints")
    op.drop_table("tyre_stints")

    op.drop_index(op.f("ix_sectors_lap_id"), table_name="sectors")
    op.drop_table("sectors")

    op.drop_index("ix_laps_session_id_lap_number", table_name="laps")
    op.drop_index(op.f("ix_laps_driver_id"), table_name="laps")
    op.drop_index(op.f("ix_laps_session_id"), table_name="laps")
    op.drop_table("laps")

    op.drop_index("uq_drivers_session_id_driver_number", table_name="drivers")
    op.drop_index(op.f("ix_drivers_session_id"), table_name="drivers")
    op.drop_table("drivers")

    op.drop_index(op.f("ix_sessions_race_id"), table_name="sessions")
    op.drop_table("sessions")

    op.drop_table("races")
