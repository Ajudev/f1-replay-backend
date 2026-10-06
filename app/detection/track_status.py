"""Track status history for lap-window checks.

The race state only holds the status at the moment of an event. A lap lasts a minute
or more, so the status at the end of a lap says little about the lap: a safety car
may have come and gone inside it. The detection context therefore keeps the (bounded)
list of status changes with their race times, and a lap's whole time window is checked
against it.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from app.domain.enums import TrackStatus


class TrackStatusChange(BaseModel):
    race_time_ms: int
    status: TrackStatus


class WindowCondition(StrEnum):
    GREEN = "GREEN"  # nothing abnormal during the window
    ABNORMAL = "ABNORMAL"  # an excluded status was active at some point
    UNKNOWN = "UNKNOWN"  # the status of (part of) the window is not known


class TrackStatusHistory(BaseModel):
    changes: list[TrackStatusChange] = Field(default_factory=list)

    def record(self, status: TrackStatus | None, race_time_ms: int, *, limit: int) -> bool:
        """Append ``status`` when it differs from the latest known one."""
        if status is None:
            return False
        if self.changes and self.changes[-1].status is status:
            return False
        self.changes.append(TrackStatusChange(race_time_ms=race_time_ms, status=status))
        del self.changes[:-limit]
        return True

    def status_at(self, race_time_ms: int) -> TrackStatus | None:
        """Status in effect at ``race_time_ms`` (``None`` before the first known change)."""
        current: TrackStatus | None = None
        for change in self.changes:
            if change.race_time_ms > race_time_ms:
                break
            current = change.status
        return current

    def classify_window(
        self,
        start_ms: int | None,
        end_ms: int,
        abnormal: frozenset[TrackStatus],
        *,
        lap_status: str | None = None,
    ) -> WindowCondition:
        """Condition of ``(start_ms, end_ms]``.

        ``lap_status`` is the status the lap itself was recorded with. It is the
        fallback when the history does not reach back to the window start, and it can
        only make the answer worse (an abnormal lap status is never overridden).
        """
        start = end_ms if start_ms is None else min(start_ms, end_ms)
        own = _parse(lap_status)
        statuses: set[TrackStatus] = set()
        base = self.status_at(start)
        if base is not None:
            statuses.add(base)
        statuses.update(c.status for c in self.changes if start < c.race_time_ms <= end_ms)
        if own is not None and own is not TrackStatus.UNKNOWN and own in abnormal:
            statuses.add(own)
        if statuses & abnormal:
            return WindowCondition.ABNORMAL
        if start_ms is None:
            return WindowCondition.UNKNOWN
        if base is None and (own is None or own is TrackStatus.UNKNOWN):
            return WindowCondition.UNKNOWN
        if TrackStatus.UNKNOWN in statuses:
            return WindowCondition.UNKNOWN
        return WindowCondition.GREEN


def _parse(value: str | None) -> TrackStatus | None:
    if value is None:
        return None
    try:
        return TrackStatus(value)
    except ValueError:
        return TrackStatus.UNKNOWN
