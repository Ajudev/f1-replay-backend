"""Domain enums for race sessions, events, and replay lifecycle.

These types must not depend on FastAPI, SQLAlchemy, FastF1, or Redis.
"""

from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):
    """Machine-readable race event types."""

    RACE_STARTED = "RACE_STARTED"
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


class DetectedEventType(StrEnum):
    """Analytical events produced by the event detection engine.

    Distinct from ``EventType`` (raw historical timeline events).
    """

    BATTLE_FORMING = "BATTLE_FORMING"
    RAPIDLY_CLOSING = "RAPIDLY_CLOSING"
    OVERTAKE = "OVERTAKE"
    PACE_DEGRADATION = "PACE_DEGRADATION"
    PACE_ANOMALY = "PACE_ANOMALY"
    PERSONAL_BEST = "PERSONAL_BEST"
    NEW_STINT = "NEW_STINT"
    BATTLE_ENDED = "BATTLE_ENDED"


class Severity(StrEnum):
    """Coarse magnitude of a detection, only where a detector documents its bands."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


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
    """Replay session lifecycle status (transitions live in ``app.replay.state``)."""

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TrackStatus(StrEnum):
    """Track status derived from FastF1 track-status codes."""

    GREEN = "GREEN"
    YELLOW = "YELLOW"
    SAFETY_CAR = "SAFETY_CAR"
    VIRTUAL_SAFETY_CAR = "VIRTUAL_SAFETY_CAR"
    VIRTUAL_SAFETY_CAR_ENDING = "VIRTUAL_SAFETY_CAR_ENDING"
    RED_FLAG = "RED_FLAG"
    UNKNOWN = "UNKNOWN"


class DrsState(StrEnum):
    """DRS flap state derived from the raw FastF1 ``DRS`` channel code."""

    OFF = "OFF"
    ELIGIBLE = "ELIGIBLE"
    OPEN = "OPEN"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_code(cls, code: int | None) -> DrsState | None:
        """``0``/``1`` off, ``8`` eligible (detected, not open), ``10``/``12``/``14`` open."""
        if code is None:
            return None
        if code in (0, 1):
            return cls.OFF
        if code == 8:
            return cls.ELIGIBLE
        if code in (10, 12, 14):
            return cls.OPEN
        return cls.UNKNOWN
