"""Pure, deterministic race state reducer.

``apply_event(state, event, config=...)`` returns a new state and a delta. It does no
I/O, reads no wall clock and never mutates its input, so the same events always
produce the same state whether they arrive live from the stream or are replayed
from the persisted timeline during a rebuild.

Semantics worth knowing:

- ``current_lap`` follows the replay engine: ``min(leader_laps_completed + 1,
  total_laps)``; the leader is the driver with the most completed laps, ties broken
  by the earliest crossing of that lap.
- Gaps are ``LAP_END``: when a driver completes lap N, ``gap_to_leader_ms`` is that
  crossing minus the first crossing of lap N by any driver, ``interval_to_ahead_ms``
  is the crossing minus the crossing of the driver reported one position ahead on
  that lap. Both are ``null`` when the needed crossing is not in the retained window.
- Mid-race retirements are invisible (there is no retirement event); drivers that
  never cross the line after the leader's chequered flag become ``DID_NOT_FINISH`` only
  when the race is finalized (the last timeline event is applied).
- The tyre information of an older lap never overwrites newer information.
- A pit lane entry is cleared (``ON_TRACK``) by ``PIT_EXIT`` or by completing a lap
  later than the lap of the pit entry (a full lap cannot be driven inside the pit lane).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from app.domain.enums import EventType, TrackStatus
from app.race_state.config import RaceStateConfig
from app.race_state.models import (
    RACE_STATE_SCHEMA_VERSION,
    ChangeKind,
    DriverRaceStatus,
    DriverState,
    FastestLap,
    GapBasis,
    LapCrossing,
    LapRecord,
    PitStatus,
    RacePhase,
    RaceSeed,
    RaceState,
    SnapshotTrigger,
    StateDelta,
)

logger = logging.getLogger(__name__)

#: Race-level fields reported in ``StateDelta.race`` when they change.
RACE_DELTA_FIELDS = frozenset(
    {
        "phase",
        "current_lap",
        "total_laps",
        "leader_laps_completed",
        "leader_driver_id",
        "leader_driver_abbreviation",
        "track_status",
        "fastest_lap",
    }
)


@dataclass(frozen=True, slots=True)
class ReducerEvent:
    """Transport-neutral input: built from a stream event or a persisted timeline row."""

    event_id: UUID
    event_type: str
    sequence: int
    race_time_ms: int
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None
    payload: dict[str, Any]


def initial_state(
    seed: RaceSeed,
    *,
    replay_id: UUID,
    run_id: UUID,
    run_published_at: datetime | None = None,
) -> RaceState:
    """State before any event: seeded drivers at their grid positions, PRE_RACE."""
    drivers = {
        d.driver_id: DriverState(
            driver_id=d.driver_id,
            abbreviation=d.abbreviation,
            driver_number=d.driver_number,
            full_name=d.full_name,
            team_name=d.team_name,
            grid_position=d.grid_position,
            position=d.grid_position,
        )
        for d in seed.drivers
    }
    return RaceState(
        schema_version=RACE_STATE_SCHEMA_VERSION,
        replay_id=replay_id,
        run_id=run_id,
        session_id=seed.session_id,
        race_id=seed.race_id,
        season=seed.season,
        round=seed.round,
        session_type=seed.session_type,
        total_laps=seed.total_laps,
        total_events=seed.total_events,
        run_published_at=run_published_at,
        drivers=drivers,
    )


def apply_event(
    state: RaceState, event: ReducerEvent, *, config: RaceStateConfig
) -> tuple[RaceState, StateDelta]:
    """Apply one event. Returns ``(new_state, delta)``; ``state`` is left untouched."""
    new = state.model_copy(deep=True)
    handler = _HANDLERS.get(event.event_type)
    if handler is not None:
        handler(new, event, config)
    elif event.event_type == EventType.SECTOR_COMPLETED.value:
        logger.debug("Sector event ignored sequence=%d (sectors are not tracked)", event.sequence)
    else:
        logger.warning(
            "Unsupported event type ignored replay_id=%s sequence=%d type=%s",
            state.replay_id,
            event.sequence,
            event.event_type,
        )

    new.current_race_time_ms = event.race_time_ms
    new.last_sequence = event.sequence
    new.last_event_id = event.event_id
    finalized = event.sequence == new.total_events - 1 and new.phase is not RacePhase.COMPLETED
    if finalized:
        _finalize(new)
    for driver in new.drivers.values():
        _sync_driver_lap(driver, new.total_laps)

    delta = _diff(state, new, event)
    delta.snapshot_trigger = _snapshot_trigger(state, new, event, finalized, config)
    if delta.snapshot_trigger is SnapshotTrigger.PERIODIC:
        new.last_snapshot_lap = (new.leader_laps_completed // config.snapshot_every_laps) * (
            config.snapshot_every_laps
        )
    return new, delta


# -- event handlers ----------------------------------------------------------------------


def _on_race_started(state: RaceState, event: ReducerEvent, _config: RaceStateConfig) -> None:
    payload = event.payload
    state.phase = RacePhase.RUNNING
    state.current_lap = 1
    if state.race_id is None:
        state.race_id = _uuid(payload.get("race_id"))
    if state.season is None:
        state.season = _int(payload.get("season"))
    if state.round is None:
        state.round = _int(payload.get("round"))
    if state.session_type is None:
        state.session_type = _str(payload.get("session_type"))

    grid = payload.get("grid")
    for entry in grid if isinstance(grid, list) else []:
        if not isinstance(entry, dict):
            continue
        driver_id = _uuid(entry.get("driver_id"))
        abbreviation = _str(entry.get("abbreviation"))
        if driver_id is None:
            continue
        grid_position = _int(entry.get("grid_position"))
        driver = state.drivers.get(driver_id)
        if driver is None:
            if abbreviation is None:
                continue  # an unnamed driver cannot be represented
            driver = state.drivers[driver_id] = DriverState(
                driver_id=driver_id, abbreviation=abbreviation
            )
        if driver.grid_position is None:
            driver.grid_position = grid_position
        if driver.position is None:
            driver.position = grid_position
    for driver in state.drivers.values():
        driver.race_status = DriverRaceStatus.RUNNING


def _on_track_status(state: RaceState, event: ReducerEvent, _config: RaceStateConfig) -> None:
    raw = event.payload.get("status")
    try:
        state.track_status = TrackStatus(raw)
    except ValueError:
        logger.warning(
            "Unknown track status %r at sequence=%d; recorded as UNKNOWN", raw, event.sequence
        )
        state.track_status = TrackStatus.UNKNOWN


def _on_lap_completed(state: RaceState, event: ReducerEvent, config: RaceStateConfig) -> None:
    driver = _driver_for(state, event)
    lap_number = event.lap_number
    if driver is None or lap_number is None:
        return
    payload = event.payload
    lap_time = _int(payload.get("lap_time_ms"))
    is_deleted = _bool(payload.get("is_deleted"))
    position = _int(payload.get("position"))

    if driver.race_status is DriverRaceStatus.NOT_STARTED:
        driver.race_status = DriverRaceStatus.RUNNING

    record = LapRecord(
        lap_number=lap_number,
        lap_time_ms=lap_time,
        position=position,
        compound=_str(payload.get("compound")),
        tyre_age_laps=_int(payload.get("tyre_age_laps")),
        stint_number=_int(payload.get("stint_number")),
        is_pit_in_lap=_bool(payload.get("is_pit_in_lap")),
        is_pit_out_lap=_bool(payload.get("is_pit_out_lap")),
        is_deleted=is_deleted,
        is_accurate=_bool(payload.get("is_accurate")),
        track_status=_str(payload.get("track_status")),
        race_time_ms=event.race_time_ms,
        completion_time_source=_str(payload.get("completion_time_source")),
    )
    laps = [r for r in driver.recent_laps if r.lap_number != lap_number]
    laps.append(record)
    laps.sort(key=lambda r: r.lap_number)
    driver.recent_laps = laps[-config.lap_history :]

    if (
        lap_time is not None
        and is_deleted is not True
        and (driver.best_lap_time_ms is None or lap_time < driver.best_lap_time_ms)
    ):
        driver.best_lap_time_ms = lap_time
        driver.best_lap_number = lap_number

    _apply_tyre(
        driver,
        lap_number=lap_number,
        stint=record.stint_number,
        compound=record.compound,
        age=record.tyre_age_laps,
    )

    newest = lap_number >= driver.laps_completed
    if newest:
        driver.laps_completed = lap_number
        driver.last_lap_time_ms = lap_time
        driver.last_lap_race_time_ms = event.race_time_ms
        if position is not None:
            _set_position(driver, position)
        if driver.pit_status is PitStatus.UNKNOWN or (
            driver.pit_status is PitStatus.IN_PIT
            and driver.last_pit_entry_lap is not None
            and lap_number > driver.last_pit_entry_lap
        ):
            driver.pit_status = PitStatus.ON_TRACK

    _refresh_leader(state)
    if newest:
        _record_crossing(state, driver, lap_number, event.race_time_ms, position, config)
    _prune_crossings(state, config)

    if (
        state.total_laps is not None
        and state.leader_laps_completed >= state.total_laps
        and newest
        and driver.laps_completed >= 1
    ):
        driver.race_status = DriverRaceStatus.FINISHED
        if state.phase is RacePhase.RUNNING:
            state.phase = RacePhase.CHEQUERED


def _on_position_changed(state: RaceState, event: ReducerEvent, _config: RaceStateConfig) -> None:
    driver = _driver_for(state, event)
    new_position = _int(event.payload.get("new_position"))
    if driver is None or new_position is None:
        return
    # LAP_COMPLETED (earlier at the same time) usually applied this already; setting
    # an unchanged position is a no-op, never a toggle.
    _set_position(driver, new_position)


def _on_pit_entry(state: RaceState, event: ReducerEvent, _config: RaceStateConfig) -> None:
    driver = _driver_for(state, event)
    if driver is None:
        return
    driver.pit_status = PitStatus.IN_PIT
    driver.last_pit_entry_lap = event.lap_number
    driver.last_pit_entry_race_time_ms = event.race_time_ms


def _on_pit_exit(state: RaceState, event: ReducerEvent, _config: RaceStateConfig) -> None:
    driver = _driver_for(state, event)
    if driver is None:
        return
    payload = event.payload
    driver.pit_status = PitStatus.ON_TRACK
    driver.last_pit_exit_race_time_ms = event.race_time_ms
    driver.last_pit_lane_duration_ms = _int(payload.get("pit_lane_duration_ms"))
    driver.pit_stop_count += 1
    _apply_tyre(
        driver,
        lap_number=event.lap_number,
        stint=_int(payload.get("stint_number")),
        compound=_str(payload.get("compound")),
        age=_int(payload.get("tyre_age_laps")),
        force_new_stint=True,
    )


def _on_fastest_lap(state: RaceState, event: ReducerEvent, _config: RaceStateConfig) -> None:
    driver = _driver_for(state, event)
    lap_time = _int(event.payload.get("lap_time_ms"))
    if driver is None or lap_time is None:
        return
    state.fastest_lap = FastestLap(
        driver_id=driver.driver_id,
        abbreviation=driver.abbreviation,
        lap_number=event.lap_number,
        lap_time_ms=lap_time,
        race_time_ms=event.race_time_ms,
    )
    if driver.best_lap_time_ms is None or lap_time < driver.best_lap_time_ms:
        driver.best_lap_time_ms = lap_time
        driver.best_lap_number = event.lap_number


_HANDLERS = {
    EventType.RACE_STARTED.value: _on_race_started,
    EventType.TRACK_STATUS_CHANGED.value: _on_track_status,
    EventType.LAP_COMPLETED.value: _on_lap_completed,
    EventType.POSITION_CHANGED.value: _on_position_changed,
    EventType.PIT_ENTRY.value: _on_pit_entry,
    EventType.PIT_EXIT.value: _on_pit_exit,
    EventType.FASTEST_LAP.value: _on_fastest_lap,
}


# -- helpers -----------------------------------------------------------------------------


def _driver_for(state: RaceState, event: ReducerEvent) -> DriverState | None:
    if event.driver_id is None:
        return None
    driver = state.drivers.get(event.driver_id)
    if driver is None:
        logger.warning(
            "Event for unknown driver ignored replay_id=%s sequence=%d type=%s driver_id=%s",
            state.replay_id,
            event.sequence,
            event.event_type,
            event.driver_id,
        )
    return driver


def _set_position(driver: DriverState, position: int) -> None:
    if driver.position != position:
        driver.previous_position = driver.position
        driver.position = position


def _apply_tyre(
    driver: DriverState,
    *,
    lap_number: int | None,
    stint: int | None,
    compound: str | None,
    age: int | None,
    force_new_stint: bool = False,
) -> None:
    """Take tyre information from an event; older laps never overwrite newer ones.

    A different stint number replaces compound, age and stint wholesale (even with
    nulls, so a previous stint's compound is not carried over). Otherwise only the
    non-null values are merged. ``force_new_stint`` (``PIT_EXIT``) always replaces.
    """
    if lap_number is None:
        return
    if driver.tyre_info_lap is not None and lap_number < driver.tyre_info_lap:
        return
    new_stint = force_new_stint or (stint is not None and stint != driver.stint_number)
    if new_stint:
        driver.stint_number = stint
        driver.compound = compound
        driver.tyre_age_laps = age
    else:
        if stint is not None:
            driver.stint_number = stint
        if compound is not None:
            driver.compound = compound
        if age is not None:
            driver.tyre_age_laps = age
    driver.tyre_info_lap = lap_number


def _refresh_leader(state: RaceState) -> None:
    """Leader: most completed laps, then earliest crossing of that lap, then abbreviation."""
    laps = max((d.laps_completed for d in state.drivers.values()), default=0)
    state.leader_laps_completed = laps
    candidates = [d for d in state.drivers.values() if d.laps_completed == laps and laps > 0]
    if not candidates:
        state.leader_driver_id = None
        state.leader_driver_abbreviation = None
    else:
        leader = min(
            candidates,
            key=lambda d: (
                d.last_lap_race_time_ms is None,
                d.last_lap_race_time_ms or 0,
                d.abbreviation,
            ),
        )
        state.leader_driver_id = leader.driver_id
        state.leader_driver_abbreviation = leader.abbreviation
    if state.phase is RacePhase.PRE_RACE:
        state.current_lap = None
    elif state.total_laps:
        state.current_lap = max(1, min(laps + 1, state.total_laps))
    else:
        state.current_lap = laps + 1


def _record_crossing(
    state: RaceState,
    driver: DriverState,
    lap_number: int,
    race_time_ms: int,
    position: int | None,
    config: RaceStateConfig,
) -> None:
    """Store the crossing and derive this driver's lap-end gap and interval."""
    floor = state.leader_laps_completed - config.lap_history
    driver.laps_behind_leader = max(0, state.leader_laps_completed - lap_number)
    driver.gap_to_leader_ms = None
    driver.interval_to_ahead_ms = None
    driver.gap_basis = None
    if lap_number <= floor:
        return  # older than the retained window: not derivable, never guessed
    crossings = state.lap_crossings.setdefault(lap_number, {})
    crossings[driver.driver_id] = LapCrossing(race_time_ms=race_time_ms, position=position)

    first = min(c.race_time_ms for c in crossings.values())
    driver.gap_to_leader_ms = race_time_ms - first
    driver.gap_basis = GapBasis.LAP_END
    if position is not None and position > 1:
        ahead = [
            c
            for other_id, c in crossings.items()
            if other_id != driver.driver_id and c.position == position - 1
        ]
        if len(ahead) == 1 and ahead[0].race_time_ms <= race_time_ms:
            driver.interval_to_ahead_ms = race_time_ms - ahead[0].race_time_ms


def _prune_crossings(state: RaceState, config: RaceStateConfig) -> None:
    floor = state.leader_laps_completed - config.lap_history
    for lap in [lap for lap in state.lap_crossings if lap <= floor]:
        del state.lap_crossings[lap]


def _sync_driver_lap(driver: DriverState, total_laps: int | None) -> None:
    if driver.race_status in (DriverRaceStatus.NOT_STARTED, DriverRaceStatus.FINISHED):
        driver.current_lap = None
        return
    lap = driver.laps_completed + 1
    driver.current_lap = min(lap, total_laps) if total_laps else lap


def _finalize(state: RaceState) -> None:
    state.phase = RacePhase.COMPLETED
    for driver in state.drivers.values():
        if driver.race_status is not DriverRaceStatus.FINISHED:
            driver.race_status = DriverRaceStatus.DID_NOT_FINISH


def _snapshot_trigger(
    old: RaceState,
    new: RaceState,
    event: ReducerEvent,
    finalized: bool,
    config: RaceStateConfig,
) -> SnapshotTrigger | None:
    if finalized:
        return SnapshotTrigger.FINAL
    if event.event_type == EventType.RACE_STARTED.value and old.phase is RacePhase.PRE_RACE:
        return SnapshotTrigger.INITIAL
    every = config.snapshot_every_laps
    if every > 0:
        bucket = (new.leader_laps_completed // every) * every
        if bucket > 0 and bucket > old.last_snapshot_lap:
            return SnapshotTrigger.PERIODIC
    return None


def _diff(old: RaceState, new: RaceState, event: ReducerEvent) -> StateDelta:
    old_race = old.model_dump(mode="json", include=set(RACE_DELTA_FIELDS))
    new_race = new.model_dump(mode="json", include=set(RACE_DELTA_FIELDS))
    race = {k: v for k, v in new_race.items() if old_race.get(k) != v}

    changed = [
        driver for driver_id, driver in new.drivers.items() if old.drivers.get(driver_id) != driver
    ]

    kinds: set[ChangeKind] = set()
    position_changes: list[UUID] = []
    pit_changes: list[UUID] = []
    if event.event_type == EventType.RACE_STARTED.value and old.phase is RacePhase.PRE_RACE:
        kinds.add(ChangeKind.RACE_STARTED)
    if event.event_type == EventType.LAP_COMPLETED.value and changed:
        kinds.add(ChangeKind.LAP_COMPLETED)
    for driver in changed:
        before = old.drivers.get(driver.driver_id)
        if before is not None and before.position != driver.position:
            kinds.add(ChangeKind.POSITION_CHANGED)
            position_changes.append(driver.driver_id)
        if before is not None and before.pit_status != driver.pit_status:
            kinds.add(ChangeKind.PIT_STATUS_CHANGED)
            pit_changes.append(driver.driver_id)
    if "track_status" in race:
        kinds.add(ChangeKind.TRACK_STATUS_CHANGED)
    if "fastest_lap" in race:
        kinds.add(ChangeKind.FASTEST_LAP_CHANGED)
    if old.phase is not RacePhase.COMPLETED and new.phase is RacePhase.COMPLETED:
        kinds.add(ChangeKind.RACE_COMPLETED)

    return StateDelta(
        kinds=[k for k in ChangeKind if k in kinds],
        race=race,
        drivers=changed,
        position_changes=position_changes,
        pit_changes=pit_changes,
    )


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _uuid(value: object) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return None
    return None
