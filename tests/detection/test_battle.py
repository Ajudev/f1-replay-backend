"""BATTLE_FORMING and RAPIDLY_CLOSING."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from app.detection.config import BattleConfig, DetectionConfig
from app.domain.enums import DetectedEventType, TrackStatus
from tests.detection.builders import LAP_MS, Entry, Scenario, driver_id

BATTLE = DetectedEventType.BATTLE_FORMING
RAPID = DetectedEventType.RAPIDLY_CLOSING


def chase(gaps: Sequence[int], *, first_lap: int = 2, scenario: Scenario | None = None) -> Scenario:
    """BBB chases AAA with the given lap-end gaps, one per consecutive lap."""
    scenario = scenario or Scenario(detectors=["battle"])
    for index, gap in enumerate(gaps):
        scenario.round(first_lap + index, [Entry("AAA", 0), Entry("BBB", gap)])
    return scenario


def test_a_steadily_decreasing_gap_forms_a_battle() -> None:
    scenario = chase([3200, 2400, 1500, 800])

    [event] = scenario.detected
    assert event.event_type is BATTLE
    assert (event.primary_driver_abbreviation, event.secondary_driver_abbreviation) == (
        "BBB",
        "AAA",
    )
    assert event.lap_number == 5
    assert event.evidence["gap_ms"] == 800
    assert event.evidence["closing_rate_ms_per_lap"] == 800.0
    assert event.evidence["observed_laps"] == 4
    assert event.evidence["gap_history"] == [
        {"lap": 2, "gap_ms": 3200},
        {"lap": 3, "gap_ms": 2400},
        {"lap": 4, "gap_ms": 1500},
        {"lap": 5, "gap_ms": 800},
    ]
    assert (event.evidence["attacker_position"], event.evidence["defender_position"]) == (2, 1)
    assert event.source_sequence == scenario.state_events[-1].sequence


def test_a_static_or_growing_gap_is_not_a_battle() -> None:
    assert chase([800] * 6).detected == []
    assert chase([500, 700, 900, 1100, 1300]).detected == []


def test_not_enough_observations_gives_nothing() -> None:
    assert chase([3200, 2400, 1500]).detected == []


def test_a_gap_that_does_not_close_steadily_is_ignored() -> None:
    # Net closing, but two of the three steps went the wrong way.
    assert chase([2400, 2500, 2600, 800]).detected == []


def test_the_defender_is_the_car_directly_ahead() -> None:
    scenario = Scenario(detectors=["battle"])
    for lap, gap in zip(range(2, 6), [3200, 2400, 1500, 800], strict=True):
        scenario.round(lap, [Entry("AAA", 0), Entry("CCC", 100), Entry("BBB", 100 + gap)])

    battles = scenario.of_type(BATTLE)
    # CCC (3200 -> 800 behind AAA... but constant 100 ms) is not closing, BBB chases CCC.
    assert [(e.primary_driver_abbreviation, e.secondary_driver_abbreviation) for e in battles] == [
        ("BBB", "CCC")
    ]


def test_a_pit_lap_restarts_the_window() -> None:
    scenario = Scenario(detectors=["battle"])
    gaps = [3200, 2400, 1500, 800]
    for lap, gap in zip(range(2, 6), gaps, strict=True):
        scenario.round(lap, [Entry("AAA", 0), Entry("BBB", gap, pit_in=lap == 3)])
    assert scenario.detected == []  # only laps 4 and 5 are usable

    chase([700, 600], first_lap=6, scenario=scenario)
    # The window had to refill after the pit lap: laps 4-7, not laps 2-5.
    assert [e.lap_number for e in scenario.detected] == [7]


def test_a_defender_pit_lap_restarts_the_window() -> None:
    scenario = Scenario(detectors=["battle"])
    for lap, gap in zip(range(2, 6), [3200, 2400, 1500, 800], strict=True):
        scenario.round(lap, [Entry("AAA", 0, pit_out=lap == 4), Entry("BBB", gap)])
    assert scenario.detected == []


def test_a_neutralised_lap_restarts_the_window() -> None:
    scenario = Scenario(detectors=["battle"])
    gaps = [3200, 2400, 1500, 800]
    for lap, gap in zip(range(2, 6), gaps, strict=True):
        if lap == 4:
            scenario.track(TrackStatus.SAFETY_CAR, 4 * LAP_MS - 50_000)
            status = "SAFETY_CAR"
        else:
            status = "GREEN"
        scenario.round(lap, [Entry("AAA", 0, status=status), Entry("BBB", gap, status=status)])
        if lap == 4:
            scenario.track(TrackStatus.GREEN, 4 * LAP_MS + 5_000)
    assert scenario.detected == []


def test_a_missing_lap_is_not_consecutive() -> None:
    scenario = Scenario(detectors=["battle"])
    for lap, gap in [(2, 3200), (3, 2400), (5, 1500), (6, 800)]:
        scenario.round(lap, [Entry("AAA", 0), Entry("BBB", gap)])
    assert scenario.detected == []


def test_a_change_of_defender_restarts_the_window() -> None:
    scenario = Scenario(["AAA", "BBB", "CCC"], detectors=["battle"])
    scenario.round(2, [Entry("AAA", 0), Entry("BBB", 3200), Entry("CCC", 20_000)])
    scenario.round(3, [Entry("AAA", 0), Entry("BBB", 2400), Entry("CCC", 20_000)])
    # CCC jumps ahead of AAA's pace: BBB's car ahead is now CCC.
    scenario.round(4, [Entry("CCC", 0), Entry("BBB", 1500), Entry("AAA", 20_000)])
    scenario.round(5, [Entry("CCC", 0), Entry("BBB", 800), Entry("AAA", 20_000)])
    assert scenario.detected == []


def test_an_active_battle_is_reported_once() -> None:
    scenario = chase([3200, 2400, 1500, 800, 700, 600, 500, 600, 700])

    assert [e.event_type for e in scenario.detected] == [BATTLE]


def test_a_battle_can_form_again_after_it_ended_and_the_cooldown_passed() -> None:
    gaps = [3200, 2400, 1500, 800]  # forms on lap 5
    gaps += [2000, 2600, 3000, 3400]  # released on lap 6, opens up
    gaps += [2800, 1900, 1100, 600]  # closes again
    scenario = chase(gaps)

    battles = scenario.of_type(BATTLE)
    assert [e.lap_number for e in battles] == [5, 13]
    assert len({e.detected_event_id for e in battles}) == 2


def test_cooldown_blocks_a_second_battle_for_the_same_pair() -> None:
    gaps = [3200, 2400, 1500, 800]  # forms on lap 5
    gaps += [1600, 1500, 1000, 600]  # ends on lap 6 (>1500), closes again on lap 9
    long_cooldown = replace(DetectionConfig(), battle=replace(BattleConfig(), cooldown_laps=5))

    blocked = chase(gaps, scenario=Scenario(detectors=["battle"], config=long_cooldown))
    allowed = chase(gaps)

    assert [e.lap_number for e in blocked.of_type(BATTLE)] == [5]
    assert [e.lap_number for e in allowed.of_type(BATTLE)] == [5, 9]


def test_hysteresis_keeps_the_battle_active_between_the_thresholds() -> None:
    # 1200 is above the battle gap but below the release gap: still the same battle.
    scenario = chase([3200, 2400, 1500, 800, 1200, 1400, 900, 700, 600])

    assert len(scenario.of_type(BATTLE)) == 1


def test_rapidly_closing_gap_outside_battle_range() -> None:
    scenario = chase([6400, 4900, 3300, 1800])

    [event] = scenario.detected
    assert event.event_type is RAPID
    assert event.evidence["gap_ms"] == 1800
    assert event.evidence["closing_rate_ms_per_lap"] == 1533.3
    assert event.evidence["max_gap_ms"] == 5_000
    assert event.lap_number == 5


def test_rapidly_closing_is_reported_once_per_approach() -> None:
    scenario = chase([6400, 4900, 3300, 1800, 1700, 1650])

    assert scenario.types() == ["RAPIDLY_CLOSING"]


def test_rapidly_closing_then_battle_are_distinct_signals() -> None:
    scenario = chase([6400, 4900, 3300, 1800, 900])

    assert scenario.types() == ["RAPIDLY_CLOSING", "BATTLE_FORMING"]


def test_a_slow_approach_is_not_rapid() -> None:
    assert chase([4800, 4500, 4200, 3900]).detected == []  # 300 ms/lap, far from a battle


def test_attackers_are_tracked_independently() -> None:
    scenario = Scenario(detectors=["battle"])
    # BBB closes on AAA; DDD closes on CCC. Both pairs are separate battles.
    for lap, (g1, g2) in zip(
        range(2, 6), [(3200, 3000), (2400, 2200), (1500, 1300), (800, 700)], strict=True
    ):
        scenario.round(
            lap,
            [
                Entry("AAA", 0),
                Entry("BBB", g1),
                Entry("CCC", g1 + 30_000),
                Entry("DDD", g1 + 30_000 + g2),
            ],
        )
    pairs = {
        (e.primary_driver_abbreviation, e.secondary_driver_abbreviation)
        for e in scenario.of_type(BATTLE)
    }
    assert pairs == {("BBB", "AAA"), ("DDD", "CCC")}
    assert {e.primary_driver_id for e in scenario.detected} == {driver_id("BBB"), driver_id("DDD")}
