"""OVERTAKE."""

from __future__ import annotations

from app.domain.enums import DetectedEventType, EventType, TrackStatus
from tests.detection.builders import LAP_MS, Entry, Scenario

OVERTAKE = DetectedEventType.OVERTAKE


def grid(scenario: Scenario, order: list[str], laps: int = 2) -> None:
    for lap in range(1, laps + 1):
        scenario.round(lap, [Entry(a, 400 * i) for i, a in enumerate(order)])


def test_an_adjacent_swap_on_track_is_an_overtake() -> None:
    scenario = Scenario(["AAA", "BBB", "CCC"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB", "CCC"])

    scenario.round(3, [Entry("BBB", 0), Entry("AAA", 300), Entry("CCC", 800)])

    [event] = scenario.detected
    assert event.event_type is OVERTAKE
    assert (event.primary_driver_abbreviation, event.secondary_driver_abbreviation) == (
        "BBB",
        "AAA",
    )
    assert event.lap_number == 3
    assert event.evidence == {
        "lap": 3,
        "overtaker_previous_position": 2,
        "overtaker_new_position": 1,
        "overtaken_previous_position": 1,
        "overtaken_new_position": 2,
        "classification": "ON_TRACK_LIKELY",
        "basis": "LAP_END",
        "confirmed_by": "AAA",  # the later of the two to complete the lap
    }
    assert [str(i) for i in event.source_event_ids] == [
        scenario.state_events[-2].payload["source_event_id"]
    ]


def test_the_event_is_confirmed_when_the_second_driver_crosses() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB"])

    first = scenario.round(3, [Entry("BBB", 0, pos=1)])  # only the overtaker has crossed
    assert first == []
    second = scenario.round(3, [Entry("AAA", 300, pos=2)])
    assert [e.event_type for e in second] == [OVERTAKE]
    assert second[0].source_sequence == scenario.state_events[-1].sequence


def test_a_pit_stop_swap_is_not_an_overtake() -> None:
    scenario = Scenario(["AAA", "BBB", "CCC"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB", "CCC"])

    # AAA pits at the end of lap 3 and drops behind BBB.
    scenario.round(3, [Entry("BBB", 0), Entry("AAA", 300, pit_in=True), Entry("CCC", 800)])

    assert scenario.detected == []


def test_a_swap_involving_a_pit_out_lap_of_the_previous_lap_is_not_an_overtake() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    scenario.round(1, [Entry("AAA", 0), Entry("BBB", 400)])
    scenario.round(2, [Entry("AAA", 0, pit_out=True), Entry("BBB", 400)])

    scenario.round(3, [Entry("BBB", 0), Entry("AAA", 300)])

    assert scenario.detected == []


def test_a_retirement_is_not_an_overtake() -> None:
    scenario = Scenario(["AAA", "BBB", "CCC"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB", "CCC"])

    # AAA never completes lap 3: BBB and CCC simply move up.
    scenario.round(3, [Entry("BBB", 0), Entry("CCC", 500)])

    assert scenario.detected == []


def test_a_swap_during_a_safety_car_period_is_not_an_overtake() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB"])
    scenario.track(TrackStatus.SAFETY_CAR, 3 * LAP_MS - 40_000)

    scenario.round(
        3, [Entry("BBB", 0, status="SAFETY_CAR"), Entry("AAA", 300, status="SAFETY_CAR")]
    )

    assert scenario.detected == []


def test_a_swap_in_laps_with_an_unknown_track_status_is_not_reported() -> None:
    # No TRACK_STATUS_CHANGED was ever seen and the laps carry no status either.
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"], green=False)
    for lap, order in ((1, "AB"), (2, "AB"), (3, "BA")):
        scenario.round(
            lap,
            [
                Entry({"A": "AAA", "B": "BBB"}[c], 400 * i, status="UNKNOWN")
                for i, c in enumerate(order)
            ],
        )

    assert scenario.detected == []


def test_the_lap_status_stands_in_when_the_history_does_not_reach_back() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"], green=False)
    for lap, order in ((1, "AB"), (2, "AB"), (3, "BA")):
        scenario.round(
            lap, [Entry({"A": "AAA", "B": "BBB"}[c], 400 * i) for i, c in enumerate(order)]
        )

    assert len(scenario.of_type(OVERTAKE)) == 1


def test_the_same_source_event_twice_gives_one_overtake() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB"])
    scenario.round(3, [Entry("BBB", 0), Entry("AAA", 300)])

    assert scenario.feed(scenario.state_events[-1]) == []
    assert len(scenario.of_type(OVERTAKE)) == 1


def test_multi_position_changes_that_are_not_a_single_swap_are_skipped() -> None:
    scenario = Scenario(["AAA", "BBB", "CCC"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB", "CCC"])

    # CCC jumps two places; AAA and BBB each lose one: no adjacent pair swapped.
    scenario.round(3, [Entry("CCC", 0), Entry("AAA", 300), Entry("BBB", 600)])

    assert scenario.detected == []


def test_two_simultaneous_independent_swaps_are_both_reported() -> None:
    scenario = Scenario(detectors=["overtake"])
    grid(scenario, ["AAA", "BBB", "CCC", "DDD"])

    scenario.round(
        3, [Entry("BBB", 0), Entry("AAA", 300), Entry("DDD", 20_000), Entry("CCC", 20_300)]
    )

    pairs = {
        (e.primary_driver_abbreviation, e.secondary_driver_abbreviation) for e in scenario.detected
    }
    assert pairs == {("BBB", "AAA"), ("DDD", "CCC")}
    assert len({e.detected_event_id for e in scenario.detected}) == 2


def test_lap_one_changes_are_not_reported() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])

    scenario.round(1, [Entry("BBB", 0), Entry("AAA", 300)])  # differs from the grid order

    assert scenario.detected == []


def test_a_rebuilt_state_does_not_confirm_swaps() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB"])
    scenario.round(3, [Entry("BBB", 0, pos=1)])
    # The completing event arrives as a rebuilt snapshot: intermediate events were skipped.
    found = scenario.emit(
        EventType.LAP_COMPLETED,
        3 * LAP_MS + 300,
        "AAA",
        3,
        payload={
            "lap_time_ms": 90_000,
            "position": 2,
            "stint_number": 1,
            "compound": "MEDIUM",
            "tyre_age_laps": 3,
            "is_deleted": False,
            "is_pit_in_lap": False,
            "is_pit_out_lap": False,
            "track_status": "GREEN",
        },
        rebuilt=True,
    )

    assert found == []


def test_unknown_pit_flags_do_not_count_as_on_track() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    scenario.round(1, [Entry("AAA", 0), Entry("BBB", 400)])
    scenario.round(2, [Entry("AAA", 0, pit_in=None, pit_out=None), Entry("BBB", 400)])

    scenario.round(3, [Entry("BBB", 0), Entry("AAA", 300)])  # lap N-1 of AAA is unknown

    assert scenario.detected == []


def test_unknown_pit_flags_on_the_current_lap_are_not_on_track_either() -> None:
    scenario = Scenario(["AAA", "BBB"], detectors=["overtake"])
    grid(scenario, ["AAA", "BBB"])

    scenario.round(3, [Entry("BBB", 0, pit_out=None), Entry("AAA", 300)])

    assert scenario.detected == []
