"""Pure reducer behavior on small synthetic races."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

from app.domain.enums import EventType, TrackStatus
from app.race_state.config import RaceStateConfig
from app.race_state.models import (
    ChangeKind,
    DriverRaceStatus,
    PitStatus,
    RacePhase,
    SnapshotTrigger,
)
from app.race_state.reducer import apply_event, initial_state
from tests.race_state.builders import (
    CONFIG,
    HAM,
    NOR,
    REPLAY,
    RUN,
    VER,
    Sim,
    lap_payload,
    make_seed,
)

# -- initialization ---------------------------------------------------------------------


def test_initial_state_seeds_drivers_at_their_grid_positions() -> None:
    state = initial_state(make_seed(), replay_id=REPLAY, run_id=RUN)

    assert state.phase is RacePhase.PRE_RACE
    assert state.last_sequence == -1
    assert state.current_race_time_ms == 0
    assert state.current_lap is None
    assert state.total_laps == 5
    assert [d.abbreviation for d in state.drivers_by_position()] == ["VER", "HAM", "NOR"]
    ham = state.drivers[HAM]
    assert (ham.driver_number, ham.full_name, ham.team_name) == (44, "Lewis Hamilton", "Mercedes")
    assert ham.position == ham.grid_position == 2
    # Nothing is fabricated before the race starts.
    assert ham.compound is None and ham.tyre_age_laps is None and ham.stint_number is None
    assert ham.pit_status is PitStatus.UNKNOWN
    assert ham.race_status is DriverRaceStatus.NOT_STARTED


def test_race_started_merges_grid_without_dropping_seeded_metadata() -> None:
    sim = Sim(started=False)
    stranger = UUID(int=99)
    grid = [
        {"driver_id": str(VER), "abbreviation": "VER", "grid_position": 7},  # seed wins
        {"driver_id": str(stranger), "abbreviation": "ZZZ", "grid_position": 4},
    ]
    delta = sim.emit(EventType.RACE_STARTED, 0, payload={"grid": grid})

    state = sim.state
    assert state.phase is RacePhase.RUNNING and state.current_lap == 1
    assert state.drivers[VER].full_name == "Max Verstappen"
    assert state.drivers[VER].grid_position == 1
    assert set(state.drivers) == {VER, HAM, NOR, stranger}
    assert state.drivers[stranger].position == 4
    assert all(d.race_status is DriverRaceStatus.RUNNING for d in state.drivers.values())
    assert ChangeKind.RACE_STARTED in delta.kinds
    assert delta.snapshot_trigger is SnapshotTrigger.INITIAL


# -- laps -----------------------------------------------------------------------------------


def test_current_lap_follows_the_leader_and_ignores_lapped_cars() -> None:
    sim = Sim()
    assert sim.state.current_lap == 1
    sim.lap(VER, 1, 90_000, position=1)
    assert sim.state.current_lap == 2 and sim.state.leader_driver_id == VER
    sim.lap(NOR, 1, 90_500, position=2)
    sim.lap(VER, 2, 180_000, position=1)
    assert sim.state.current_lap == 3
    # A lapped car completing lap 1 does not move the race lap.
    sim.lap(HAM, 1, 190_000, position=3)
    assert sim.state.current_lap == 3
    assert sim.state.drivers[HAM].laps_completed == 1
    assert sim.state.drivers[HAM].current_lap == 2
    assert sim.state.drivers[HAM].laps_behind_leader == 1


def test_leader_tie_is_broken_by_earliest_crossing_and_lap_is_capped() -> None:
    sim = Sim(seed=make_seed(total_laps=2))
    sim.lap(NOR, 1, 90_000, position=1)
    sim.lap(VER, 1, 90_400, position=2)
    sim.lap(VER, 2, 180_000, position=1)
    sim.lap(NOR, 2, 180_300, position=2)

    state = sim.state
    assert state.leader_laps_completed == 2
    assert state.leader_driver_id == VER  # crossed lap 2 first
    assert state.current_lap == 2  # min(2 + 1, total_laps)
    assert state.phase is RacePhase.CHEQUERED
    assert state.drivers[VER].race_status is DriverRaceStatus.FINISHED
    assert state.drivers[NOR].race_status is DriverRaceStatus.FINISHED
    assert state.drivers[HAM].race_status is DriverRaceStatus.RUNNING
    assert state.drivers[VER].current_lap is None


def test_unknown_total_laps_leaves_current_lap_uncapped() -> None:
    sim = Sim(seed=make_seed(total_laps=None))
    sim.lap(VER, 1, 90_000)
    sim.lap(VER, 2, 180_000)
    assert sim.state.current_lap == 3
    assert sim.state.phase is RacePhase.RUNNING


def test_last_and_best_lap_exclude_deleted_laps() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, lap_time=92_000)
    sim.lap(VER, 2, 180_000, lap_time=88_000, deleted=True)
    sim.lap(VER, 3, 270_000, lap_time=90_500)
    sim.lap(VER, 4, 360_000, lap_time=None)

    ver = sim.state.drivers[VER]
    assert ver.best_lap_time_ms == 90_500 and ver.best_lap_number == 3
    assert ver.last_lap_time_ms is None  # lap 4 has no time; not carried over
    assert ver.laps_completed == 4


def test_recent_laps_are_bounded_ordered_and_never_duplicated() -> None:
    sim = Sim(config=RaceStateConfig(lap_history=3))
    for n in range(1, 6):
        sim.lap(VER, n, n * 90_000, lap_time=90_000 + n)
    # The same lap again (an event processed twice at reducer level) replaces its entry.
    sim.emit(EventType.LAP_COMPLETED, 5 * 90_000, VER, 5, lap_payload(lap_time=1))

    laps = sim.state.drivers[VER].recent_laps
    assert [r.lap_number for r in laps] == [3, 4, 5]
    assert laps[-1].lap_time_ms == 1
    assert sim.state.drivers[VER].laps_completed == 5  # set from the lap number, not incremented


def test_laps_completed_is_never_incremented_or_lowered() -> None:
    sim = Sim()
    sim.lap(VER, 3, 270_000)
    sim.lap(VER, 2, 280_000)  # an older lap arriving late
    assert sim.state.drivers[VER].laps_completed == 3
    assert sim.state.drivers[VER].last_lap_race_time_ms == 270_000


# -- tyres ----------------------------------------------------------------------------------


def test_tyre_data_comes_from_payload_and_is_never_aged_by_the_engine() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, compound="SOFT", age=3)
    sim.lap(VER, 2, 180_000, compound="SOFT", age=None)  # unknown age keeps the last known value

    ver = sim.state.drivers[VER]
    assert (ver.compound, ver.tyre_age_laps, ver.stint_number, ver.tyre_info_lap) == (
        "SOFT",
        3,
        1,
        2,
    )


def test_new_stint_replaces_tyre_info_even_with_null_compound() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, stint=1, compound="SOFT", age=1)
    sim.lap(VER, 2, 180_000, stint=2, compound=None, age=None)

    ver = sim.state.drivers[VER]
    assert ver.stint_number == 2
    assert ver.compound is None and ver.tyre_age_laps is None  # no stale SOFT carried over


def test_older_lap_tyre_info_does_not_overwrite_newer_pit_exit_info() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, stint=1, compound="SOFT", age=1)
    sim.emit(
        EventType.PIT_EXIT,
        100_000,
        VER,
        2,
        {"stint_number": 2, "compound": "HARD", "tyre_age_laps": 0, "pit_lane_duration_ms": 21_000},
    )
    sim.lap(VER, 1, 101_000, stint=1, compound="SOFT", age=1)  # late lap-1 row

    ver = sim.state.drivers[VER]
    assert (ver.stint_number, ver.compound, ver.tyre_age_laps, ver.tyre_info_lap) == (
        2,
        "HARD",
        0,
        2,
    )


# -- positions ------------------------------------------------------------------------------


def test_positions_update_keep_previous_and_sort_the_drivers() -> None:
    sim = Sim()
    sim.lap(NOR, 1, 90_000, position=1)
    sim.lap(VER, 1, 90_500, position=2)
    sim.lap(HAM, 1, 91_000, position=3)

    state = sim.state
    assert [d.abbreviation for d in state.drivers_by_position()] == ["NOR", "VER", "HAM"]
    assert state.drivers[NOR].previous_position == 3  # grid 3 -> 1
    assert state.drivers[VER].previous_position == 1
    assert state.drivers[HAM].previous_position == 2


def test_position_changed_after_lap_completed_is_not_applied_twice() -> None:
    sim = Sim()
    sim.lap(NOR, 1, 90_000, position=2)
    after_lap = sim.state.drivers[NOR].model_copy(deep=True)
    delta = sim.emit(
        EventType.POSITION_CHANGED,
        90_000,
        NOR,
        1,
        {"previous_position": 3, "new_position": 2, "granularity": "LAP_END"},
    )

    assert sim.state.drivers[NOR] == after_lap  # previous_position is not clobbered
    assert after_lap.position == 2 and after_lap.previous_position == 3
    assert delta.is_empty


def test_position_changed_alone_sets_position() -> None:
    sim = Sim()
    delta = sim.emit(
        EventType.POSITION_CHANGED, 90_000, NOR, 1, {"previous_position": 3, "new_position": 1}
    )
    assert sim.state.drivers[NOR].position == 1
    assert sim.state.drivers[NOR].previous_position == 3
    assert delta.kinds == [ChangeKind.POSITION_CHANGED]


def test_drivers_without_a_position_sort_last_with_deterministic_tie_breaks() -> None:
    sim = Sim()
    for driver in sim.state.drivers.values():
        driver.position = None
    sim.state.drivers[HAM].laps_completed = 2
    order = [d.abbreviation for d in sim.state.drivers_by_position()]
    assert order == ["HAM", "NOR", "VER"]  # laps desc, then abbreviation


# -- pit stops ------------------------------------------------------------------------------


def test_pit_entry_and_exit_update_status_counts_and_tyres() -> None:
    sim = Sim()
    sim.lap(HAM, 1, 91_000)
    assert sim.state.drivers[HAM].pit_status is PitStatus.ON_TRACK

    delta = sim.emit(EventType.PIT_ENTRY, 170_000, HAM, 2, {"stint_number": 1, "compound": "SOFT"})
    ham = sim.state.drivers[HAM]
    assert ham.pit_status is PitStatus.IN_PIT and ham.last_pit_entry_race_time_ms == 170_000
    assert delta.kinds == [ChangeKind.PIT_STATUS_CHANGED]

    # The in-lap is completed while still in the pit lane: stays IN_PIT.
    sim.lap(HAM, 2, 181_000, pit_in=True)
    assert sim.state.drivers[HAM].pit_status is PitStatus.IN_PIT

    sim.emit(
        EventType.PIT_EXIT,
        202_500,
        HAM,
        3,
        {"stint_number": 2, "compound": "HARD", "tyre_age_laps": 0, "pit_lane_duration_ms": 22_000},
    )
    ham = sim.state.drivers[HAM]
    assert ham.pit_status is PitStatus.ON_TRACK and ham.pit_stop_count == 1
    assert ham.last_pit_exit_race_time_ms == 202_500 and ham.last_pit_lane_duration_ms == 22_000
    assert (ham.stint_number, ham.compound, ham.tyre_age_laps) == (2, "HARD", 0)


def test_a_later_lap_clears_a_pit_status_whose_exit_was_never_seen() -> None:
    sim = Sim()
    sim.emit(EventType.PIT_ENTRY, 170_000, HAM, 2, {})
    sim.lap(HAM, 2, 181_000)
    assert sim.state.drivers[HAM].pit_status is PitStatus.IN_PIT
    sim.lap(HAM, 3, 271_000)
    assert sim.state.drivers[HAM].pit_status is PitStatus.ON_TRACK


def test_repeated_pit_entry_does_not_change_the_stop_count() -> None:
    sim = Sim()
    sim.emit(EventType.PIT_ENTRY, 170_000, HAM, 2, {})
    sim.emit(EventType.PIT_ENTRY, 170_000, HAM, 2, {})
    assert sim.state.drivers[HAM].pit_stop_count == 0


# -- race level ------------------------------------------------------------------------------


def test_track_status_and_fastest_lap() -> None:
    sim = Sim()
    assert sim.state.track_status is None
    delta = sim.emit(
        EventType.TRACK_STATUS_CHANGED,
        100_000,
        payload={"status": "SAFETY_CAR", "source_code": "4"},
    )
    assert sim.state.track_status is TrackStatus.SAFETY_CAR
    assert delta.kinds == [ChangeKind.TRACK_STATUS_CHANGED]
    assert delta.race == {"track_status": "SAFETY_CAR"}

    sim.lap(NOR, 4, 360_000, lap_time=88_500)
    delta = sim.emit(
        EventType.FASTEST_LAP,
        360_000,
        NOR,
        4,
        {"lap_time_ms": 88_500, "previous_holder_driver_id": None},
    )
    fastest = sim.state.fastest_lap
    assert fastest is not None
    assert (fastest.driver_id, fastest.lap_number, fastest.lap_time_ms) == (NOR, 4, 88_500)
    assert fastest.abbreviation == "NOR" and fastest.race_time_ms == 360_000
    assert sim.state.drivers[NOR].best_lap_time_ms == 88_500
    assert delta.kinds == [ChangeKind.FASTEST_LAP_CHANGED]


def test_fastest_lap_event_keeps_the_drivers_best_lap_consistent() -> None:
    sim = Sim()
    sim.emit(EventType.FASTEST_LAP, 90_000, VER, 1, {"lap_time_ms": 90_000})
    ver = sim.state.drivers[VER]
    assert (ver.best_lap_time_ms, ver.best_lap_number) == (90_000, 1)


# -- gaps -----------------------------------------------------------------------------------


def test_gaps_and_intervals_have_lap_end_semantics() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, position=1)
    sim.lap(NOR, 1, 91_500, position=2)
    sim.lap(HAM, 1, 95_000, position=3)

    ver, nor, ham = (sim.state.drivers[d] for d in (VER, NOR, HAM))
    assert (ver.gap_to_leader_ms, ver.interval_to_ahead_ms) == (0, None)
    assert (nor.gap_to_leader_ms, nor.interval_to_ahead_ms) == (1_500, 1_500)
    assert (ham.gap_to_leader_ms, ham.interval_to_ahead_ms) == (5_000, 3_500)
    assert ham.gap_basis.value == "LAP_END"
    assert ham.laps_behind_leader == 0


def test_interval_is_null_when_the_car_ahead_cannot_be_identified() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, position=1)
    sim.lap(HAM, 1, 95_000, position=3)  # nobody is known at position 2 on this lap
    sim.lap(NOR, 1, 96_000, position=None)  # no position at all

    ham, nor = sim.state.drivers[HAM], sim.state.drivers[NOR]
    assert ham.gap_to_leader_ms == 5_000 and ham.interval_to_ahead_ms is None
    assert nor.gap_to_leader_ms == 6_000 and nor.interval_to_ahead_ms is None


def test_gap_is_null_when_the_lap_left_the_retained_window() -> None:
    config = RaceStateConfig(lap_history=2)
    sim = Sim(config=config, seed=make_seed(total_laps=None))
    for n in range(1, 6):
        sim.lap(VER, n, n * 90_000, position=1)
    sim.lap(HAM, 2, 500_000, position=2)  # lap 2 is long gone from a 2-lap window

    ham = sim.state.drivers[HAM]
    assert ham.gap_to_leader_ms is None and ham.interval_to_ahead_ms is None
    assert ham.gap_basis is None
    assert ham.laps_behind_leader == 3
    assert sorted(sim.state.lap_crossings) == [4, 5]  # bounded


# -- unsupported events, completion, snapshots -------------------------------------------------


def test_sector_and_unknown_events_only_advance_the_sequence() -> None:
    sim = Sim()
    before = sim.state
    for event_type in (EventType.SECTOR_COMPLETED, "SOMETHING_NEW", EventType.BATTLE_FORMING):
        delta = sim.emit(event_type, 1_000, VER, 1, {"x": 1})
        assert delta.is_empty
    assert sim.state.last_sequence == before.last_sequence + 3
    assert sim.state.drivers == before.drivers


def test_final_event_completes_the_race_and_marks_non_finishers() -> None:
    seed = make_seed(total_laps=2, total_events=7)
    sim = Sim(seed=seed)  # sequence 0
    sim.lap(VER, 1, 90_000, position=1)  # 1
    sim.lap(NOR, 1, 91_000, position=2)  # 2
    sim.lap(VER, 2, 180_000, position=1)  # 3: chequered
    assert sim.state.phase is RacePhase.CHEQUERED
    sim.lap(NOR, 2, 181_000, position=2)  # 4
    sim.emit(EventType.TRACK_STATUS_CHANGED, 182_000, payload={"status": "GREEN"})  # 5
    assert sim.state.phase is RacePhase.CHEQUERED
    delta = sim.emit(EventType.TRACK_STATUS_CHANGED, 183_000, payload={"status": "YELLOW"})  # 6

    state = sim.state
    assert state.phase is RacePhase.COMPLETED
    assert state.drivers[VER].race_status is DriverRaceStatus.FINISHED
    assert state.drivers[NOR].race_status is DriverRaceStatus.FINISHED
    assert state.drivers[HAM].race_status is DriverRaceStatus.DID_NOT_FINISH
    assert ChangeKind.RACE_COMPLETED in delta.kinds
    assert delta.snapshot_trigger is SnapshotTrigger.FINAL


def test_periodic_snapshot_triggers_once_per_lap_bucket() -> None:
    sim = Sim(config=RaceStateConfig(snapshot_every_laps=2), seed=make_seed(total_laps=None))
    triggers = []
    for n in range(1, 6):
        triggers.append(sim.lap(VER, n, n * 90_000).snapshot_trigger)
        # a follow-up event on the same lap must not trigger again
        sim.emit(EventType.TRACK_STATUS_CHANGED, n * 90_000, payload={"status": "GREEN"})
    assert triggers == [None, SnapshotTrigger.PERIODIC, None, SnapshotTrigger.PERIODIC, None]
    assert sim.state.last_snapshot_lap == 4


def test_periodic_snapshots_can_be_disabled() -> None:
    sim = Sim(config=RaceStateConfig(snapshot_every_laps=0), seed=make_seed(total_laps=None))
    assert all(sim.lap(VER, n, n * 90_000).snapshot_trigger is None for n in range(1, 12))


# -- purity and determinism ---------------------------------------------------------------------


def test_apply_event_does_not_mutate_its_input() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, position=1)
    before = sim.state.model_dump(mode="json")
    event = sim.events[-1]
    new_state, _ = apply_event(sim.state, replace(event, sequence=99, lap_number=2), config=CONFIG)
    assert sim.state.model_dump(mode="json") == before
    assert new_state is not sim.state


def test_same_events_reduce_to_the_same_state() -> None:
    sim = Sim()
    sim.lap(VER, 1, 90_000, position=1)
    sim.lap(NOR, 1, 91_000, position=2)
    sim.emit(EventType.PIT_ENTRY, 170_000, HAM, 2, {})
    sim.lap(VER, 2, 180_000, position=1)

    again = initial_state(sim.seed, replay_id=REPLAY, run_id=RUN)
    for event in sim.events:
        again, _ = apply_event(again, event, config=CONFIG)

    assert again.logical_dump() == sim.state.logical_dump()
    assert again.model_dump_json() == sim.state.model_dump_json()
