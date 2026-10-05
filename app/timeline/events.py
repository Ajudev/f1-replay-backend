"""Timeline event value object, ordering priorities and schema version."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from app.domain.enums import EventType

#: Bump when payload shapes or ordering rules change incompatibly.
TIMELINE_SCHEMA_VERSION = 1

#: Order of events that share an identical ``race_time_ms``. Lower sorts first.
#: SECTOR_COMPLETED is reserved (sector events are not generated; see README).
EVENT_PRIORITY: dict[EventType, int] = {
    EventType.RACE_STARTED: 0,
    EventType.TRACK_STATUS_CHANGED: 10,
    EventType.PIT_ENTRY: 20,
    EventType.SECTOR_COMPLETED: 25,
    EventType.LAP_COMPLETED: 30,
    EventType.POSITION_CHANGED: 40,
    EventType.FASTEST_LAP: 50,
    EventType.PIT_EXIT: 60,
}

#: Event types the timeline builder can currently emit.
TIMELINE_EVENT_TYPES: frozenset[EventType] = frozenset(
    {
        EventType.RACE_STARTED,
        EventType.TRACK_STATUS_CHANGED,
        EventType.PIT_ENTRY,
        EventType.LAP_COMPLETED,
        EventType.POSITION_CHANGED,
        EventType.FASTEST_LAP,
        EventType.PIT_EXIT,
    }
)


@dataclass(frozen=True, slots=True)
class TimelineEvent:
    """One structured, machine-readable historical race event.

    ``race_time_ms`` is integer milliseconds since the race start (>= 0).
    ``sequence`` is the event's 0-based position in the final ordering.
    """

    session_id: UUID
    event_type: EventType
    race_time_ms: int
    sequence: int
    driver_id: UUID | None
    lap_number: int | None
    payload: dict[str, Any] = field(default_factory=dict)
