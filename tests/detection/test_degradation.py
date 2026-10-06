"""PACE_DEGRADATION."""

from __future__ import annotations

import pytest

from app.domain.enums import DetectedEventType, Severity, TrackStatus
from tests.detection.builders import LAP_MS, Entry, Scenario, drive, driver_id

DEGRADATION = DetectedEventType.PACE_DEGRADATION


def scenario() -> Scenario:
    return Scenario(["AAA", "BBB"], detectors=["pace_degradation"])


def test_a_sustained_slowdown_is_reported_once_with_its_evidence() -> None:
    s = scenario()

    drive(s, [90_000] * 5 + [91_000] * 5)

    [event] = s.detected
    assert event.event_type is DEGRADATION
    assert event.primary_driver_abbreviation == "AAA" and event.secondary_driver_id is None
    assert event.lap_number == 11
    assert event.severity is Severity.MEDIUM
    evidence = event.evidence
    assert evidence["baseline_median_ms"] == 90_000
    assert evidence["recent_median_ms"] == 91_000
    assert evidence["delta_ms"] == 1_000
    assert evidence["slower_recent_laps"] == 5
    assert evidence["stint_number"] == 1 and evidence["compound"] == "MEDIUM"
    assert [lap["lap"] for lap in evidence["baseline_laps"]] == [2, 3, 4, 5, 6]
    assert [lap["lap"] for lap in evidence["recent_laps"]] == [7, 8, 9, 10, 11]


def test_it_is_not_repeated_while_the_slowdown_stays_the_same() -> None:
    s = scenario()

    drive(s, [90_000] * 5 + [91_000] * 9)

    assert len(s.detected) == 1


def test_it_is_reported_again_only_when_the_delta_grew_enough() -> None:
    s = scenario()

    drive(s, [90_000] * 5 + [91_000] * 5 + [93_000] * 6)

    deltas = [e.evidence["delta_ms"] for e in s.detected]
    assert len(deltas) == 2
    assert deltas[1] >= deltas[0] + 500


def test_a_single_slow_lap_is_not_degradation() -> None:
    s = scenario()

    drive(s, [90_000] * 5 + [90_000, 90_000, 96_000, 90_000, 90_000])

    assert s.detected == []


def test_a_small_slowdown_below_the_threshold_is_ignored() -> None:
    s = scenario()

    drive(s, [90_000] * 5 + [90_300] * 5)

    assert s.detected == []


def test_stable_pace_with_jitter_gives_nothing() -> None:
    s = scenario()
    jitter = [0, 120, -80, 60, -150, 90, -40, 30, -110, 70, 20, -60, 100, -90, 40]

    drive(s, [90_000 + j for j in jitter])

    assert s.detected == []


def test_pit_laps_are_excluded_from_the_windows() -> None:
    s = scenario()
    drive(s, [90_000] * 5)
    s.round(7, [Entry("AAA", 0, lap_time=120_000, pit_in=True)])  # a slow in lap

    drive(s, [90_000] * 5, first_lap=8)

    assert s.detected == []
    laps = s.context.detectors["pace_degradation"].data["drivers"][str(driver_id("AAA"))]["laps"]  # type: ignore[union-attr]
    assert 7 not in [lap["lap_number"] for lap in laps]


@pytest.mark.parametrize("status", [TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR])
def test_neutralised_laps_and_the_restart_lap_are_excluded(status: TrackStatus) -> None:
    s = scenario()
    drive(s, [90_000] * 5)  # laps 2-6
    s.track(status, 7 * LAP_MS - 50_000)
    drive(s, [135_000] * 3, first_lap=7, status=status.value)  # laps 7-9, slow
    s.track(TrackStatus.GREEN, 9 * LAP_MS + 5_000)
    drive(s, [96_000], first_lap=10)  # restart lap, still slow
    drive(s, [90_000] * 5, first_lap=11)

    assert s.detected == []
    memory = s.context.detectors["pace_degradation"].data["drivers"][str(driver_id("AAA"))]  # type: ignore[union-attr]
    assert {lap["lap_number"] for lap in memory["laps"]}.isdisjoint({7, 8, 9, 10})


def test_a_lap_that_only_partly_overlaps_a_safety_car_is_excluded() -> None:
    s = scenario()
    drive(s, [90_000] * 5)
    # The SC came and went inside lap 7, which ended under green.
    s.track(TrackStatus.SAFETY_CAR, 7 * LAP_MS - 60_000)
    s.track(TrackStatus.GREEN, 7 * LAP_MS - 20_000)
    drive(s, [100_000], first_lap=7)

    memory = s.context.detectors["pace_degradation"].data["drivers"][str(driver_id("AAA"))]  # type: ignore[union-attr]
    assert 7 not in [lap["lap_number"] for lap in memory["laps"]]


def test_insufficient_history_gives_nothing() -> None:
    s = scenario()

    drive(s, [90_000] * 5 + [92_000] * 4)  # nine clean laps, ten are needed

    assert s.detected == []


def test_a_new_stint_starts_a_new_baseline() -> None:
    s = scenario()
    drive(s, [90_000] * 5)  # stint 1, laps 2-6
    s.round(7, [Entry("AAA", 0, lap_time=105_000, pit_in=True)])
    s.round(8, [Entry("AAA", 0, lap_time=110_000, pit_out=True, stint=2, compound="HARD")])

    # Stint 2 is five seconds slower than stint 1 from the start: no degradation within it.
    drive(s, [95_000] * 9, first_lap=9, stint=2, compound="HARD")

    assert s.detected == []
    memory = s.context.detectors["pace_degradation"].data["drivers"][str(driver_id("AAA"))]  # type: ignore[union-attr]
    assert {lap["stint_number"] for lap in memory["laps"]} == {2}


def test_degradation_within_the_new_stint_is_reported_with_its_stint() -> None:
    s = scenario()
    drive(s, [90_000] * 5)
    s.round(7, [Entry("AAA", 0, lap_time=105_000, pit_in=True)])
    s.round(8, [Entry("AAA", 0, lap_time=110_000, pit_out=True, stint=2, compound="HARD")])

    drive(s, [93_000] * 5 + [94_200] * 5, first_lap=9, stint=2, compound="HARD")

    [event] = s.detected
    assert event.evidence["stint_number"] == 2 and event.evidence["compound"] == "HARD"
    assert {lap["lap"] for lap in event.evidence["baseline_laps"]} == {9, 10, 11, 12, 13}


def test_drivers_do_not_share_history() -> None:
    s = scenario()
    # AAA degrades; BBB has the same laps in total but only half of each window.
    for lap in range(2, 12):
        aaa = 90_000 if lap < 7 else 91_000
        s.round(lap, [Entry("AAA", 0, lap_time=aaa), Entry("BBB", 5_000, lap_time=90_000)])

    assert [e.primary_driver_abbreviation for e in s.detected] == ["AAA"]


def test_a_gap_in_the_laps_restarts_the_baseline() -> None:
    s = scenario()
    drive(s, [90_000] * 5)
    drive(s, [91_500] * 5, first_lap=20)  # twelve laps missing: not one continuous window

    assert s.detected == []
