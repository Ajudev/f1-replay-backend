"""Domain enums for race sessions, events, and replay lifecycle.

These types must not depend on FastAPI, SQLAlchemy, FastF1, or Redis.
"""

from enum import StrEnum


class EventType(StrEnum):
    """Machine-readable race event types."""

    LAP_COMPLETED = "LAP_COMPLETED"
    SECTOR_COMPLETED = "SECTOR_COMPLETED"
    POSITION_CHANGED = "POSITION_CHANGED"
    PIT_ENTRY = "PIT_ENTRY"
    PIT_EXIT = "PIT_EXIT"
    FASTEST_LAP = "FASTEST_LAP"
    TRACK_STATUS_CHANGED = "TRACK_STATUS_CHANGED"
    BATTLE_FORMING = "BATTLE_FORMING"
    PACE_DEGRADATION = "PACE_DEGRADATION"
    PACE_ANOMALY = "PACE_ANOMALY"


class SessionType(StrEnum):
    """Grand Prix weekend session types."""

    PRACTICE_1 = "PRACTICE_1"
    PRACTICE_2 = "PRACTICE_2"
    PRACTICE_3 = "PRACTICE_3"
    QUALIFYING = "QUALIFYING"
    SPRINT_QUALIFYING = "SPRINT_QUALIFYING"
    SPRINT = "SPRINT"
    RACE = "RACE"


class ReplayStatus(StrEnum):
    """Replay session lifecycle status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    STOPPED = "STOPPED"
