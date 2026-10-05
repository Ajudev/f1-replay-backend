"""Unit tests for the pure timeline builder."""

from __future__ import annotations

import random
from dataclasses import replace

import pytest

from app.domain.enums import EventType, SessionType, TrackStatus
from app.timeline.builder import build_timeline
from app.timeline.errors import (
    TimelineBuildError,
    TimelineValidationError,
    UnsupportedSessionTypeError,
)
from app.timeline.events import EVENT_PRIORITY, TimelineEvent
from tests.timeline.factories import (
    EPOCH,
    drv,
    lap,
    one_driver_source,
    representative_race,
    source,
    status,
)


def of_type(events: list[TimelineEvent], event_type: EventType) -> list[TimelineEvent]:
    return [e for e in events if e.event_type == event_type]


# --- epoch / race-relative time ---------------------------------------------------------


def test_times_are_relative_to_race_start_epoch() -> None:
    src, _ = one_driver_source(2)
    built = build_timeline(src)
    assert built.race_start_session_time_ms == EPOCH
    started = built.events[0]
    assert started.event_type == EventType.RACE_STARTED
    assert started.race_time_ms == 0 and started.sequence == 0
    assert started.payload["race_start_session_time_ms"] == EPOCH
    laps = of_type(built.events, EventType.LAP_COMPLETED)
    assert [e.race_time_ms for e in laps] == [90_000, 180_000]


def test_epoch_is_earliest_lap_one_start_across_drivers() -> None:
    a, b = drv("AAA", 1), drv("BBB", 2)
    src = source(
        [a, b],
        [
            lap(a, 1, start=EPOCH + 300, end=EPOCH + 90_300),
            lap(b, 1, start=EPOCH, end=EPOCH + 90_000),
        ],
    )
    assert build_timeline(src).race_start_session_time_ms == EPOCH


def test_missing_lap_one_start_raises_build_error() -> None:
    d = drv("VER", 1)
    src = source([d], [lap(d, 1, start=None, end=90_000), lap(d, 2, start=90_000, end=180_000)])
    with pytest.raises(TimelineBuildError):
        build_timeline(src)


def test_no_laps_raises_build_error() -> None:
    with pytest.raises(TimelineBuildError):
        build_timeline(source([drv("VER", 1)], []))


@pytest.mark.parametrize(
    "session_type",
    [SessionType.QUALIFYING, SessionType.PRACTICE_1, SessionType.SPRINT_QUALIFYING],
)
def test_unsupported_session_types(session_type: SessionType) -> None:
    src, _ = one_driver_source(1)
    with pytest.raises(UnsupportedSessionTypeError):
        build_timeline(replace(src, session_type=session_type))


def test_sprint_is_supported() -> None:
    src, _ = one_driver_source(1)
    built = build_timeline(replace(src, session_type=SessionType.SPRINT))
    assert built.events[0].payload["session_type"] == "SPRINT"


def test_race_started_grid_sorted_deterministically() -> None:
    a, b, c = drv("AAA", 3), drv("BBB", None), drv("CCC", 1)
    src = source(
        [a, b, c],
        [lap(a, 1, start=EPOCH, end=EPOCH + 90_000)],
    )
    grid = build_timeline(src).events[0].payload["grid"]
    assert [g["abbreviation"] for g in grid] == ["CCC", "AAA", "BBB"]
    assert build_timeline(src).events[0].payload["driver_count"] == 3


# --- laps -------------------------------------------------------------------------------


def test_lap_completion_uses_lap_end_time_when_present() -> None:
    src, d = one_driver_source(1)
    ev = of_type(build_timeline(src).events, EventType.LAP_COMPLETED)[0]
    assert ev.driver_id == d.id and ev.lap_number == 1
    assert ev.payload["completion_time_source"] == "LAP_END_TIME"
    assert ev.payload["lap_time_ms"] == 90_000


def test_lap_completion_falls_back_to_start_plus_lap_time() -> None:
    d = drv("VER", 1)
    src = source([d], [lap(d, 1, start=EPOCH, end=None, lap_time=91_000)])
    ev = of_type(build_timeline(src).events, EventType.LAP_COMPLETED)[0]
    assert ev.race_time_ms == 91_000
    assert ev.payload["completion_time_source"] == "LAP_START_PLUS_LAP_TIME"


def test_lap_without_any_completion_time_skipped_with_warning() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000),
            lap(d, 2, start=EPOCH + 90_000, end=None, lap_time=None),
        ],
    )
    built = build_timeline(src)
    assert len(of_type(built.events, EventType.LAP_COMPLETED)) == 1
    assert any("1 lap(s) have no completion time" in w for w in built.warnings)


def test_negative_completion_time_is_validation_error() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [lap(d, 1, start=EPOCH, end=EPOCH + 90_000), lap(d, 2, start=0, end=EPOCH - 1)],
    )
    with pytest.raises(TimelineValidationError):
        build_timeline(src)


def test_lap_completion_time_going_backwards_is_validation_error() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000),
            lap(d, 2, start=EPOCH + 90_000, end=EPOCH + 180_000),
            lap(d, 3, start=EPOCH + 180_000, end=EPOCH + 150_000),
        ],
    )
    with pytest.raises(TimelineValidationError, match="completes before"):
        build_timeline(src)


def test_lap_for_unknown_driver_is_validation_error() -> None:
    d, ghost = drv("VER", 1), drv("GHO", 2)
    src = source(
        [d], [lap(d, 1, start=EPOCH, end=EPOCH + 1), lap(ghost, 1, start=EPOCH, end=EPOCH + 1)]
    )
    with pytest.raises(TimelineValidationError, match="unknown driver"):
        build_timeline(src)


def test_duplicate_driver_lap_is_validation_error() -> None:
    d = drv("VER", 1)
    src = source(
        [d], [lap(d, 1, start=EPOCH, end=EPOCH + 1), lap(d, 1, start=EPOCH, end=EPOCH + 2)]
    )
    with pytest.raises(TimelineValidationError, match="duplicate"):
        build_timeline(src)


def test_lap_event_records_track_status_in_effect() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000),
            lap(d, 2, start=EPOCH + 90_000, end=EPOCH + 180_000),
        ],
        [
            status(0, EPOCH - 10, TrackStatus.GREEN),
            status(1, EPOCH + 100_000, TrackStatus.SAFETY_CAR, "4"),
        ],
    )
    laps = of_type(build_timeline(src).events, EventType.LAP_COMPLETED)
    assert [e.payload["track_status"] for e in laps] == ["GREEN", "SAFETY_CAR"]


def test_no_track_status_yields_null_lap_track_status() -> None:
    src, _ = one_driver_source(1)
    built = build_timeline(src)
    assert of_type(built.events, EventType.LAP_COMPLETED)[0].payload["track_status"] is None
    assert of_type(built.events, EventType.TRACK_STATUS_CHANGED) == []


# --- pit events -------------------------------------------------------------------------


def test_pit_entry_exit_and_duration_pairing() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000),
            lap(d, 2, start=EPOCH + 90_000, end=EPOCH + 180_000, pit_in=EPOCH + 170_000),
            lap(d, 3, start=EPOCH + 180_000, end=EPOCH + 270_000, pit_out=EPOCH + 192_500, stint=2),
        ],
    )
    events = build_timeline(src).events
    entry = of_type(events, EventType.PIT_ENTRY)[0]
    exit_ = of_type(events, EventType.PIT_EXIT)[0]
    assert (entry.race_time_ms, entry.lap_number) == (170_000, 2)
    assert (exit_.race_time_ms, exit_.lap_number) == (192_500, 3)
    assert exit_.payload["pit_lane_duration_ms"] == 22_500
    assert entry.payload["stint_number"] == 1
    assert exit_.payload["stint_number"] == 2


def test_pit_exit_without_pairable_entry_has_null_duration() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000),
            lap(d, 2, start=EPOCH + 90_000, end=EPOCH + 180_000, pit_out=EPOCH + 100_000),
        ],
    )
    exit_ = of_type(build_timeline(src).events, EventType.PIT_EXIT)[0]
    assert exit_.payload["pit_lane_duration_ms"] is None


def test_pit_exit_before_entry_does_not_produce_negative_duration() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000, pit_in=EPOCH + 80_000),
            lap(d, 2, start=EPOCH + 90_000, end=EPOCH + 180_000, pit_out=EPOCH + 70_000),
        ],
    )
    exit_ = of_type(build_timeline(src).events, EventType.PIT_EXIT)[0]
    assert exit_.payload["pit_lane_duration_ms"] is None


def test_same_lap_pit_in_and_out_pairs() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(
                d,
                1,
                start=EPOCH,
                end=EPOCH + 90_000,
                pit_in=EPOCH + 40_000,
                pit_out=EPOCH + 62_000,
            )
        ],
    )
    exit_ = of_type(build_timeline(src).events, EventType.PIT_EXIT)[0]
    assert exit_.payload["pit_lane_duration_ms"] == 22_000


def test_pre_race_pit_exit_excluded_with_warning() -> None:
    d = drv("VER", 1)
    src = source([d], [lap(d, 1, start=EPOCH, end=EPOCH + 90_000, pit_out=EPOCH - 5_000)])
    built = build_timeline(src)
    assert of_type(built.events, EventType.PIT_EXIT) == []
    assert any("1 pit time(s) before race start" in w for w in built.warnings)
    assert all(e.race_time_ms >= 0 for e in built.events)


def test_no_pits_no_pit_events() -> None:
    src, _ = one_driver_source(2)
    events = build_timeline(src).events
    assert of_type(events, EventType.PIT_ENTRY) == []
    assert of_type(events, EventType.PIT_EXIT) == []


# --- position changes -------------------------------------------------------------------


def _pos_src(grid: int | None, positions: list[int | None]) -> tuple:
    d = drv("VER", grid)
    laps = [
        lap(d, n, start=EPOCH + (n - 1) * 90_000, end=EPOCH + n * 90_000, position=p)
        for n, p in enumerate(positions, start=1)
    ]
    return source([d], laps), d


def test_position_change_uses_grid_baseline_then_lap_baseline() -> None:
    src, _ = _pos_src(3, [2, 2, 1, 1])
    events = of_type(build_timeline(src).events, EventType.POSITION_CHANGED)
    assert [
        (e.lap_number, e.payload["previous_position"], e.payload["new_position"]) for e in events
    ] == [
        (1, 3, 2),
        (3, 2, 1),
    ]
    assert events[0].payload["previous_position_source"] == "GRID"
    assert events[1].payload["previous_position_source"] == "LAP"
    assert events[0].payload["granularity"] == "LAP_END"
    assert events[0].race_time_ms == 90_000


def test_no_position_event_when_unchanged_from_grid() -> None:
    src, _ = _pos_src(1, [1, 1])
    assert of_type(build_timeline(src).events, EventType.POSITION_CHANGED) == []


def test_no_grid_no_event_on_first_known_lap() -> None:
    src, _ = _pos_src(None, [4, 4, 3])
    events = of_type(build_timeline(src).events, EventType.POSITION_CHANGED)
    assert [(e.lap_number, e.payload["previous_position_source"]) for e in events] == [(3, "LAP")]


def test_null_positions_do_not_reset_or_emit() -> None:
    src, _ = _pos_src(2, [None, 2, None, 2, 1])
    events = of_type(build_timeline(src).events, EventType.POSITION_CHANGED)
    assert [(e.lap_number, e.payload["previous_position"]) for e in events] == [(5, 2)]


# --- track status -----------------------------------------------------------------------


def test_track_status_collapses_consecutive_duplicates() -> None:
    src, _ = one_driver_source(3)
    src = replace(
        src,
        track_statuses=[
            status(0, EPOCH, TrackStatus.GREEN),
            status(1, EPOCH + 10_000, TrackStatus.GREEN, "1"),
            status(2, EPOCH + 20_000, TrackStatus.YELLOW, "2"),
            status(3, EPOCH + 30_000, TrackStatus.YELLOW, "2"),
            status(4, EPOCH + 40_000, TrackStatus.GREEN),
        ],
    )
    events = of_type(build_timeline(src).events, EventType.TRACK_STATUS_CHANGED)
    assert [
        (e.race_time_ms, e.payload["status"], e.payload["previous_status"]) for e in events
    ] == [
        (0, "GREEN", None),
        (20_000, "YELLOW", "GREEN"),
        (40_000, "GREEN", "YELLOW"),
    ]


def test_pre_start_statuses_collapse_to_single_event_at_zero() -> None:
    src, _ = one_driver_source(2)
    src = replace(
        src,
        track_statuses=[
            status(0, 0, TrackStatus.GREEN),
            status(1, EPOCH - 500_000, TrackStatus.YELLOW, "2"),
            status(2, EPOCH - 100, TrackStatus.GREEN, "1"),
            status(3, EPOCH + 50_000, TrackStatus.SAFETY_CAR, "4"),
        ],
    )
    events = of_type(build_timeline(src).events, EventType.TRACK_STATUS_CHANGED)
    assert [(e.race_time_ms, e.payload["status"]) for e in events] == [
        (0, "GREEN"),
        (50_000, "SAFETY_CAR"),
    ]
    assert events[0].payload["source_code"] == "1"
    assert events[1].payload["previous_status"] == "GREEN"
    # initial status follows RACE_STARTED
    assert build_timeline(src).events[1].event_type == EventType.TRACK_STATUS_CHANGED


# --- fastest lap ------------------------------------------------------------------------


def test_fastest_lap_only_when_strictly_faster() -> None:
    a, b = drv("AAA", 1), drv("BBB", 2)
    src = source(
        [a, b],
        [
            lap(a, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=90_000),
            lap(b, 1, start=EPOCH, end=EPOCH + 91_000, lap_time=90_000),  # tie, later
            lap(a, 2, start=EPOCH + 90_000, end=EPOCH + 180_000, lap_time=89_000),
            lap(b, 2, start=EPOCH + 91_000, end=EPOCH + 181_000, lap_time=89_000),  # tie, later
            lap(a, 3, start=EPOCH + 180_000, end=EPOCH + 275_000, lap_time=95_000),  # slower
        ],
    )
    events = of_type(build_timeline(src).events, EventType.FASTEST_LAP)
    assert [(e.payload["lap_time_ms"], e.lap_number) for e in events] == [(90_000, 1), (89_000, 2)]
    assert events[0].payload["previous_fastest_lap_time_ms"] is None
    assert events[1].payload["previous_fastest_lap_time_ms"] == 90_000
    assert events[1].payload["previous_holder_abbreviation"] == "AAA"
    assert events[1].payload["previous_lap_number"] == 1
    assert events[1].race_time_ms == 180_000


def test_fastest_lap_ignores_deleted_and_null_lap_times() -> None:
    a = drv("AAA", 1)
    src = source(
        [a],
        [
            lap(a, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=60_000, deleted=True),
            lap(a, 2, start=EPOCH + 90_000, end=EPOCH + 180_000, lap_time=None),
            lap(a, 3, start=EPOCH + 180_000, end=EPOCH + 270_000, lap_time=90_000, deleted=None),
        ],
    )
    events = of_type(build_timeline(src).events, EventType.FASTEST_LAP)
    assert [e.lap_number for e in events] == [3]


def test_equal_time_same_timestamp_resolved_by_driver_abbreviation() -> None:
    a, b = drv("AAA", 1), drv("BBB", 2)
    src = source(
        [b, a],
        [
            lap(b, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=90_000),
            lap(a, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=90_000),
        ],
    )
    events = of_type(build_timeline(src).events, EventType.FASTEST_LAP)
    assert len(events) == 1 and events[0].driver_id == a.id


# --- ordering / priority ----------------------------------------------------------------


def test_priority_at_identical_timestamp() -> None:
    """All event kinds land on t=90_000; priority decides the order."""
    d = drv("VER", 1)
    t = 90_000
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + t, position=2, pit_in=EPOCH + t),
            lap(d, 2, start=EPOCH + t, end=EPOCH + 2 * t, pit_out=EPOCH + t, lap_time=89_000),
        ],
        [status(0, EPOCH, TrackStatus.GREEN), status(1, EPOCH + t, TrackStatus.YELLOW, "2")],
    )
    at_t = [e.event_type for e in build_timeline(src).events if e.race_time_ms == t]
    assert at_t == [
        EventType.TRACK_STATUS_CHANGED,
        EventType.PIT_ENTRY,
        EventType.LAP_COMPLETED,
        EventType.POSITION_CHANGED,
        EventType.FASTEST_LAP,
        EventType.PIT_EXIT,
    ]
    priorities = [EVENT_PRIORITY[e] for e in at_t]
    assert priorities == sorted(priorities)


def test_ties_broken_by_lap_then_driver_abbreviation() -> None:
    a, b = drv("AAA", 1), drv("BBB", 2)
    src = source(
        [b, a],
        [
            lap(b, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=None),
            lap(a, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=None),
        ],
    )
    laps = of_type(build_timeline(src).events, EventType.LAP_COMPLETED)
    assert [e.driver_id for e in laps] == [a.id, b.id]


def test_sequences_contiguous_and_times_non_decreasing() -> None:
    events = build_timeline(representative_race()).events
    assert [e.sequence for e in events] == list(range(len(events)))
    times = [e.race_time_ms for e in events]
    assert times == sorted(times)


def test_sector_completed_is_not_emitted() -> None:
    types = {e.event_type for e in build_timeline(representative_race()).events}
    assert EventType.SECTOR_COMPLETED not in types


# --- determinism ------------------------------------------------------------------------


def _fingerprint(events: list[TimelineEvent]) -> list[tuple]:
    return [
        (
            e.sequence,
            e.event_type,
            e.race_time_ms,
            e.lap_number,
            e.driver_id,
            repr(sorted(e.payload.items())),
        )
        for e in events
    ]


def test_build_is_deterministic_across_runs_and_input_order() -> None:
    src = representative_race()
    baseline = _fingerprint(build_timeline(src).events)
    assert baseline == _fingerprint(build_timeline(src).events)

    rng = random.Random(1234)
    for _ in range(10):
        laps = list(src.laps)
        drivers = list(src.drivers)
        statuses = list(src.track_statuses)
        rng.shuffle(laps)
        rng.shuffle(drivers)
        rng.shuffle(statuses)
        shuffled = replace(src, laps=laps, drivers=drivers, track_statuses=statuses)
        built = build_timeline(shuffled)
        assert _fingerprint(built.events) == baseline
        assert built.warnings == build_timeline(src).warnings


def test_track_status_processed_chronologically_not_by_sequence() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000),
            lap(d, 2, start=EPOCH + 90_000, end=EPOCH + 180_000),
        ],
        [
            status(1, EPOCH + 50_000, TrackStatus.YELLOW, "2"),
            status(2, EPOCH + 40_000, TrackStatus.SAFETY_CAR, "4"),
            status(3, EPOCH + 95_000, TrackStatus.GREEN, "1"),
        ],
    )
    events = build_timeline(src).events
    changes = of_type(events, EventType.TRACK_STATUS_CHANGED)
    assert [
        (e.race_time_ms, e.payload["status"], e.payload["previous_status"]) for e in changes
    ] == [
        (40_000, "SAFETY_CAR", None),
        (50_000, "YELLOW", "SAFETY_CAR"),
        (95_000, "GREEN", "YELLOW"),
    ]
    laps = of_type(events, EventType.LAP_COMPLETED)
    assert laps[0].payload["track_status"] == "YELLOW"


def test_completion_fallback_chain() -> None:
    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=EPOCH, end=EPOCH + 90_000, lap_time=90_000),
            lap(d, 2, start=EPOCH + 90_000, end=None, lap_time=89_000),
            lap(d, 3, start=EPOCH + 180_300, end=None, lap_time=91_000),
        ],
    )
    laps = of_type(build_timeline(src).events, EventType.LAP_COMPLETED)
    assert [(e.race_time_ms, e.payload["completion_time_source"]) for e in laps] == [
        (90_000, "LAP_END_TIME"),
        (180_300, "NEXT_LAP_START"),
        (271_300, "LAP_START_PLUS_LAP_TIME"),
    ]
