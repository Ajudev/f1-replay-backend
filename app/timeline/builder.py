"""Pure timeline construction: normalized race data in, ordered events out.

No database, no async, no FastF1/pandas. The result depends only on the
*content* of the source, never on input list order, dict order or UUID values.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID

from app.domain.enums import EventType, SessionType, TrackStatus
from app.timeline.errors import (
    TimelineBuildError,
    TimelineValidationError,
    UnsupportedSessionTypeError,
)
from app.timeline.events import EVENT_PRIORITY, TimelineEvent
from app.timeline.source import SourceDriver, SourceLap, TimelineSource
from app.timeline.validation import validate_timeline

SUPPORTED_SESSION_TYPES = frozenset({SessionType.RACE, SessionType.SPRINT})

COMPLETION_LAP_END = "LAP_END_TIME"
COMPLETION_NEXT_LAP_START = "NEXT_LAP_START"
COMPLETION_FALLBACK = "LAP_START_PLUS_LAP_TIME"


@dataclass(frozen=True, slots=True)
class BuiltTimeline:
    events: list[TimelineEvent]
    warnings: list[str] = field(default_factory=list)
    race_start_session_time_ms: int = 0


@dataclass(slots=True)
class _Pending:
    """Event plus the extra sort discriminator (not persisted)."""

    event: TimelineEvent
    tiebreak: int = 0


def _sort_key(item: _Pending, abbr_by_id: dict[UUID, str]) -> tuple[int, int, int, str, int]:
    ev = item.event
    return (
        ev.race_time_ms,
        EVENT_PRIORITY[ev.event_type],
        ev.lap_number if ev.lap_number is not None else 0,
        abbr_by_id.get(ev.driver_id, "") if ev.driver_id is not None else "",
        item.tiebreak,
    )


def compute_epoch(source: TimelineSource) -> int:
    """Race start in session time: earliest lap-1 start across drivers."""
    starts = [
        lap.lap_start_time_ms
        for lap in source.laps
        if lap.lap_number == 1 and lap.lap_start_time_ms is not None
    ]
    if not starts:
        raise TimelineBuildError(
            "Cannot determine race start: no lap 1 start time is stored for this session"
        )
    return min(starts)


def build_timeline(source: TimelineSource) -> BuiltTimeline:
    """Build, order and validate the historical timeline for a race/sprint session."""
    if source.session_type not in SUPPORTED_SESSION_TYPES:
        raise UnsupportedSessionTypeError(
            f"Timelines are only supported for RACE and SPRINT sessions, "
            f"not {source.session_type.value}"
        )

    epoch = compute_epoch(source)
    warnings: list[str] = []
    session_id = source.session_id

    drivers = sorted(source.drivers, key=lambda d: d.abbreviation)
    abbr_by_id = {d.id: d.abbreviation for d in drivers}
    laps = sorted(
        source.laps,
        key=lambda lap: (abbr_by_id.get(lap.driver_id, ""), lap.lap_number),
    )

    pending: list[_Pending] = []

    def add(
        event_type: EventType,
        race_time_ms: int,
        driver_id: UUID | None,
        lap_number: int | None,
        payload: dict[str, Any],
        tiebreak: int = 0,
    ) -> TimelineEvent:
        event = TimelineEvent(
            session_id=session_id,
            event_type=event_type,
            race_time_ms=race_time_ms,
            sequence=-1,
            driver_id=driver_id,
            lap_number=lap_number,
            payload=payload,
        )
        pending.append(_Pending(event, tiebreak))
        return event

    # RACE_STARTED
    add(
        EventType.RACE_STARTED,
        0,
        None,
        None,
        {
            "session_type": source.session_type.value,
            "race_id": str(source.race_id),
            "season": source.season,
            "round": source.round,
            "race_start_session_time_ms": epoch,
            "driver_count": len(drivers),
            "grid": [
                {
                    "driver_id": str(d.id),
                    "abbreviation": d.abbreviation,
                    "grid_position": d.grid_position,
                }
                for d in sorted(
                    drivers,
                    key=lambda d: (d.grid_position is None, d.grid_position or 0, d.abbreviation),
                )
            ],
        },
    )

    status_times, status_values = _add_track_status(source, epoch, add)

    # LAP_COMPLETED
    skipped_laps = 0
    completion_by_lap: dict[tuple[UUID, int], int] = {}
    start_by_lap = {
        (lap.driver_id, lap.lap_number): lap.lap_start_time_ms
        for lap in laps
        if lap.lap_start_time_ms is not None
    }
    for lap in laps:
        completed = _completion(lap, start_by_lap.get((lap.driver_id, lap.lap_number + 1)))
        if completed is None:
            skipped_laps += 1
            continue
        completion_ms, completion_source = completed
        rel = completion_ms - epoch
        completion_by_lap[(lap.driver_id, lap.lap_number)] = rel
        idx = bisect_right(status_times, rel) - 1
        add(
            EventType.LAP_COMPLETED,
            rel,
            lap.driver_id,
            lap.lap_number,
            {
                "lap_time_ms": lap.lap_time_ms,
                "position": lap.position,
                "compound": lap.compound,
                "tyre_age_laps": lap.tyre_age_laps,
                "stint_number": lap.stint_number,
                "is_deleted": lap.is_deleted,
                "is_accurate": lap.is_accurate,
                "is_pit_in_lap": lap.is_pit_in_lap,
                "is_pit_out_lap": lap.is_pit_out_lap,
                "track_status": status_values[idx].value if idx >= 0 else None,
                "completion_time_source": completion_source,
            },
        )
    if skipped_laps:
        warnings.append(f"{skipped_laps} lap(s) have no completion time; LAP_COMPLETED not emitted")

    _add_pit_events(laps, epoch, warnings, add)
    _add_position_changes(drivers, laps, completion_by_lap, add)
    _add_fastest_laps(pending, abbr_by_id, add)

    pending.sort(key=lambda item: _sort_key(item, abbr_by_id))
    events = [replace(item.event, sequence=index) for index, item in enumerate(pending)]

    problems = validate_timeline(events, source)
    if problems:
        raise TimelineValidationError(problems)
    return BuiltTimeline(events=events, warnings=warnings, race_start_session_time_ms=epoch)


def _completion(lap: SourceLap, next_lap_start: int | None) -> tuple[int, str] | None:
    """Lap end time, else the driver's next lap start, else start + lap time."""
    if lap.lap_end_time_ms is not None:
        return lap.lap_end_time_ms, COMPLETION_LAP_END
    if next_lap_start is not None:
        return next_lap_start, COMPLETION_NEXT_LAP_START
    if lap.lap_start_time_ms is not None and lap.lap_time_ms is not None:
        return lap.lap_start_time_ms + lap.lap_time_ms, COMPLETION_FALLBACK
    return None


def _add_track_status(
    source: TimelineSource,
    epoch: int,
    add: Any,
) -> tuple[list[int], list[TrackStatus]]:
    """Emit collapsed status changes; return (times, statuses) for lookups."""
    periods = sorted(source.track_statuses, key=lambda p: (p.session_time_ms, p.sequence))
    times: list[int] = []
    values: list[TrackStatus] = []
    current: TrackStatus | None = None

    at_start = [p for p in periods if p.session_time_ms - epoch <= 0]
    later = [p for p in periods if p.session_time_ms - epoch > 0]

    if at_start:
        initial = at_start[-1]
        add(
            EventType.TRACK_STATUS_CHANGED,
            0,
            None,
            None,
            {
                "status": initial.status.value,
                "previous_status": None,
                "source_code": initial.source_code,
            },
            initial.sequence,
        )
        current = initial.status
        times.append(0)
        values.append(current)

    for period in later:
        if period.status == current:
            continue
        rel = period.session_time_ms - epoch
        add(
            EventType.TRACK_STATUS_CHANGED,
            rel,
            None,
            None,
            {
                "status": period.status.value,
                "previous_status": current.value if current is not None else None,
                "source_code": period.source_code,
            },
            period.sequence,
        )
        current = period.status
        times.append(rel)
        values.append(current)
    return times, values


def _add_pit_events(
    laps: list[SourceLap],
    epoch: int,
    warnings: list[str],
    add: Any,
) -> None:
    pit_in_by_lap = {
        (lap.driver_id, lap.lap_number): lap.pit_in_time_ms
        for lap in laps
        if lap.pit_in_time_ms is not None
    }
    excluded = 0
    for lap in laps:
        base = {
            "stint_number": lap.stint_number,
            "compound": lap.compound,
            "tyre_age_laps": lap.tyre_age_laps,
        }
        if lap.pit_in_time_ms is not None:
            if lap.pit_in_time_ms < epoch:
                excluded += 1
            else:
                add(
                    EventType.PIT_ENTRY,
                    lap.pit_in_time_ms - epoch,
                    lap.driver_id,
                    lap.lap_number,
                    dict(base),
                )
        if lap.pit_out_time_ms is not None:
            if lap.pit_out_time_ms < epoch:
                excluded += 1
                continue
            duration: int | None = None
            for key in (
                (lap.driver_id, lap.lap_number - 1),
                (lap.driver_id, lap.lap_number),
            ):
                pit_in = pit_in_by_lap.get(key)
                if pit_in is not None and pit_in >= epoch and lap.pit_out_time_ms >= pit_in:
                    duration = lap.pit_out_time_ms - pit_in
                    break
            add(
                EventType.PIT_EXIT,
                lap.pit_out_time_ms - epoch,
                lap.driver_id,
                lap.lap_number,
                {**base, "pit_lane_duration_ms": duration},
            )
    if excluded:
        warnings.append(
            f"{excluded} pit time(s) before race start excluded (pre-race pit lane activity)"
        )


def _add_position_changes(
    drivers: list[SourceDriver],
    laps: list[SourceLap],
    completion_by_lap: dict[tuple[UUID, int], int],
    add: Any,
) -> None:
    """Emit POSITION_CHANGED at lap-end granularity.

    Laps without a completion time are ignored (no timestamp to attach to).
    """
    grid = {d.id: d.grid_position for d in drivers}
    last_position: dict[UUID, tuple[int, str]] = {
        driver_id: (pos, "GRID") for driver_id, pos in grid.items() if pos is not None
    }
    for lap in laps:  # sorted by (driver, lap_number)
        rel = completion_by_lap.get((lap.driver_id, lap.lap_number))
        if rel is None or lap.position is None:
            continue
        previous = last_position.get(lap.driver_id)
        if previous is not None and previous[0] != lap.position:
            add(
                EventType.POSITION_CHANGED,
                rel,
                lap.driver_id,
                lap.lap_number,
                {
                    "previous_position": previous[0],
                    "new_position": lap.position,
                    "previous_position_source": previous[1],
                    "granularity": "LAP_END",
                },
            )
        last_position[lap.driver_id] = (lap.position, "LAP")


def _add_fastest_laps(
    pending: list[_Pending],
    abbr_by_id: dict[UUID, str],
    add: Any,
) -> None:
    lap_events = sorted(
        (p for p in pending if p.event.event_type == EventType.LAP_COMPLETED),
        key=lambda item: _sort_key(item, abbr_by_id),
    )
    best: tuple[int, UUID, int] | None = None  # (time, driver_id, lap_number)
    for item in lap_events:
        ev = item.event
        lap_time = ev.payload["lap_time_ms"]
        if lap_time is None or ev.payload["is_deleted"] is True:
            continue
        if best is not None and lap_time >= best[0]:
            continue
        assert ev.driver_id is not None and ev.lap_number is not None
        add(
            EventType.FASTEST_LAP,
            ev.race_time_ms,
            ev.driver_id,
            ev.lap_number,
            {
                "lap_time_ms": lap_time,
                "previous_fastest_lap_time_ms": best[0] if best else None,
                "previous_holder_driver_id": str(best[1]) if best else None,
                "previous_holder_abbreviation": abbr_by_id.get(best[1]) if best else None,
                "previous_lap_number": best[2] if best else None,
            },
        )
        best = (lap_time, ev.driver_id, ev.lap_number)
