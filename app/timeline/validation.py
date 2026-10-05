"""Structural validation of a built timeline."""

from __future__ import annotations

from collections import defaultdict
from uuid import UUID

from app.domain.enums import EventType
from app.timeline.events import TimelineEvent
from app.timeline.source import TimelineSource

_MAX_PROBLEMS = 50


def validate_timeline(events: list[TimelineEvent], source: TimelineSource) -> list[str]:
    """Return a list of problems (empty when the timeline is valid).

    Per-driver lap completion times must be non-decreasing with lap number
    (equal times are tolerated; a decrease is an error).
    """
    problems: list[str] = []

    def report(message: str) -> None:
        if len(problems) < _MAX_PROBLEMS:
            problems.append(message)

    if not events:
        return ["timeline is empty"]

    started = [e for e in events if e.event_type == EventType.RACE_STARTED]
    if len(started) != 1:
        report(f"expected exactly one RACE_STARTED, found {len(started)}")
    first = events[0]
    if first.event_type != EventType.RACE_STARTED or first.sequence != 0 or first.race_time_ms != 0:
        report("first event must be RACE_STARTED with sequence 0 at race_time_ms 0")

    if [e.sequence for e in events] != list(range(len(events))):
        report("sequences are not contiguous and unique starting at 0")

    previous_time = 0
    for event in events:
        if event.race_time_ms < 0:
            report(
                f"negative race_time_ms {event.race_time_ms} for {event.event_type.value} "
                f"(lap {event.lap_number})"
            )
        elif event.race_time_ms < previous_time:
            report(f"race_time_ms decreases at sequence {event.sequence}")
        previous_time = max(previous_time, event.race_time_ms)

    driver_ids = {d.id for d in source.drivers}
    laps_known = {(lap.driver_id, lap.lap_number) for lap in source.laps}
    max_lap = max((lap.lap_number for lap in source.laps), default=0)

    seen: set[tuple[UUID, int]] = set()
    completions: dict[UUID, list[tuple[int, int]]] = defaultdict(list)
    for event in events:
        if event.driver_id is not None and event.driver_id not in driver_ids:
            report(f"{event.event_type.value} references unknown driver {event.driver_id}")
        if event.lap_number is not None and not 1 <= event.lap_number <= max_lap:
            report(f"lap_number {event.lap_number} outside 1..{max_lap}")
        if event.event_type != EventType.LAP_COMPLETED:
            continue
        if event.driver_id is None or event.lap_number is None:
            report("LAP_COMPLETED without driver or lap")
            continue
        key = (event.driver_id, event.lap_number)
        if key not in laps_known:
            report(f"LAP_COMPLETED for non-existent lap {event.lap_number}")
        if key in seen:
            report(f"duplicate LAP_COMPLETED for driver {event.driver_id} lap {event.lap_number}")
        seen.add(key)
        completions[event.driver_id].append((event.lap_number, event.race_time_ms))

    for driver_id, items in completions.items():
        items.sort()
        for (lap_a, time_a), (lap_b, time_b) in zip(items, items[1:], strict=False):
            if time_b < time_a:
                report(
                    f"driver {driver_id}: lap {lap_b} completes before lap {lap_a} "
                    f"({time_b} < {time_a})"
                )
    return problems
