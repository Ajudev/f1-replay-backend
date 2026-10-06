"""NEW_STINT."""

from __future__ import annotations

from app.domain.enums import DetectedEventType, EventType
from tests.detection.builders import LAP_MS, Entry, Scenario, drive

NEW_STINT = DetectedEventType.NEW_STINT


def scenario() -> Scenario:
    s = Scenario(["AAA", "BBB"], detectors=["stint"])
    drive(s, [90_000, 90_000], first_lap=1)  # stint 1, MEDIUM
    return s


def test_a_pit_exit_starts_a_new_stint() -> None:
    s = scenario()

    s.pit_entry("AAA", 3 * LAP_MS, 3)
    s.pit_exit("AAA", 3 * LAP_MS + 30_000, 4, stint=2, compound="HARD", duration=21_500)

    [event] = s.detected
    assert event.event_type is NEW_STINT and event.primary_driver_abbreviation == "AAA"
    assert event.evidence == {
        "stint_number": 2,
        "compound": "HARD",
        "previous_stint_number": 1,
        "previous_compound": "MEDIUM",
        "compound_changed": True,
        "starting_lap": 4,
        "tyre_age_laps": 0,
        "pit_lane_duration_ms": 21_500,
        "pit_stop_count": 1,
        "source_event_type": "PIT_EXIT",
    }
    assert event.lap_number == 4


def test_the_same_compound_is_not_a_compound_change() -> None:
    s = scenario()

    s.pit_exit("AAA", 3 * LAP_MS, 4, stint=2, compound="MEDIUM")

    assert s.detected[0].evidence["compound_changed"] is False


def test_a_repeated_pit_exit_does_not_start_another_stint() -> None:
    s = scenario()

    s.pit_exit("AAA", 3 * LAP_MS, 4, stint=2, compound="HARD")
    s.pit_exit("AAA", 3 * LAP_MS + 1_000, 4, stint=2, compound="HARD", duration=30_000)

    assert len(s.detected) == 1


def test_a_missing_compound_stays_unknown() -> None:
    s = scenario()

    s.pit_exit("AAA", 3 * LAP_MS, 4, stint=2, compound=None, duration=None)

    [event] = s.detected
    assert event.evidence["compound"] is None
    assert event.evidence["compound_changed"] is None
    assert event.evidence["pit_lane_duration_ms"] is None


def test_a_stint_revealed_by_a_lap_is_reported_when_there_was_no_pit_exit() -> None:
    s = scenario()

    s.round(3, [Entry("AAA", 0, stint=2, compound="HARD", age=1)])

    [event] = s.detected
    assert event.evidence["source_event_type"] == "LAP_COMPLETED"
    assert event.evidence["stint_number"] == 2 and event.evidence["starting_lap"] == 3
    assert event.evidence["previous_stint_number"] == 1

    # The pit exit that arrives afterwards describes the same stint.
    s.pit_exit("AAA", 3 * LAP_MS + 10, 3, stint=2, compound="HARD")
    s.round(4, [Entry("AAA", 0, stint=2, compound="HARD", age=2)])
    assert len(s.detected) == 1


def test_the_first_stint_seen_is_not_a_new_stint() -> None:
    s = Scenario(["AAA"], detectors=["stint"])

    drive(s, [90_000, 90_000], first_lap=1)

    assert s.detected == []


def test_a_pit_exit_without_a_known_stint_number_is_still_reported_once() -> None:
    s = scenario()

    s.pit_exit("AAA", 3 * LAP_MS, 4, stint=None, compound=None)

    [event] = s.detected
    assert event.evidence["stint_number"] is None and event.evidence["pit_stop_count"] == 1


def test_stints_are_tracked_per_driver() -> None:
    s = scenario()
    drive(s, [90_000, 90_000], abbr="BBB", first_lap=1)

    s.pit_exit("AAA", 3 * LAP_MS, 4, stint=2)
    s.pit_exit("BBB", 3 * LAP_MS + 500, 4, stint=2)

    assert [e.primary_driver_abbreviation for e in s.detected] == ["AAA", "BBB"]


def test_only_pit_exits_and_laps_trigger_the_detector() -> None:
    assert scenario().engine.registry.for_event(EventType.PIT_ENTRY.value) == []
