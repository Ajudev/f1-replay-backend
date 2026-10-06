"""PACE_ANOMALY."""

from __future__ import annotations

from app.domain.enums import DetectedEventType, Severity, TrackStatus
from tests.detection.builders import LAP_MS, Entry, Scenario, drive, driver_id

ANOMALY = DetectedEventType.PACE_ANOMALY
BASELINE = [90_000, 90_100, 89_900, 90_050, 89_950]  # median 90_000, MAD 50


def scenario() -> Scenario:
    return Scenario(["AAA", "BBB"], detectors=["pace_anomaly"])


def memory(s: Scenario) -> dict:
    return s.context.detectors["pace_anomaly"].data["drivers"][str(driver_id("AAA"))]  # type: ignore[union-attr]


def test_a_major_abnormal_lap_is_reported_with_evidence() -> None:
    s = scenario()

    drive(s, [*BASELINE, 96_000])

    [event] = s.detected
    assert event.event_type is ANOMALY
    assert event.lap_number == 7
    assert event.severity is Severity.MEDIUM
    evidence = event.evidence
    assert evidence["lap_time_ms"] == 96_000
    assert evidence["expected_ms"] == 90_000
    assert evidence["deviation_ms"] == 6_000
    assert evidence["mad_ms"] == 50
    assert evidence["mad_floor_ms"] == 150
    assert evidence["robust_score"] > 4
    assert [lap["lap"] for lap in evidence["baseline_laps"]] == [2, 3, 4, 5, 6]
    assert evidence["stint_number"] == 1 and evidence["compound"] == "MEDIUM"


def test_normal_variance_is_not_an_anomaly() -> None:
    s = scenario()

    drive(s, [*BASELINE, 90_400, 89_700, 90_900, 90_150])

    assert s.detected == []


def test_a_high_score_without_enough_absolute_deviation_is_ignored() -> None:
    s = scenario()
    # Perfectly consistent laps: any deviation scores high, but 700 ms is no anomaly.
    drive(s, [90_000] * 5 + [90_700])

    assert s.detected == []


def test_a_pit_lap_is_never_an_anomaly() -> None:
    s = scenario()
    drive(s, BASELINE)

    s.round(7, [Entry("AAA", 0, lap_time=118_000, pit_in=True)])
    s.round(8, [Entry("AAA", 0, lap_time=110_000, pit_out=True)])

    assert s.detected == []


def test_a_lap_under_a_safety_car_is_not_an_anomaly() -> None:
    s = scenario()
    drive(s, BASELINE)
    s.track(TrackStatus.SAFETY_CAR, 7 * LAP_MS - 60_000)

    drive(s, [140_000], first_lap=7, status="SAFETY_CAR")

    assert s.detected == []


def test_a_yellow_flag_lap_is_excluded_by_default() -> None:
    s = scenario()
    drive(s, BASELINE)
    s.track(TrackStatus.YELLOW, 7 * LAP_MS - 60_000)

    drive(s, [96_000], first_lap=7, status="YELLOW")

    assert s.detected == []


def test_a_deleted_lap_is_not_an_anomaly() -> None:
    s = scenario()
    drive(s, BASELINE)

    drive(s, [97_000], first_lap=7, deleted=True)

    assert s.detected == []


def test_without_a_full_baseline_nothing_is_reported() -> None:
    s = scenario()

    drive(s, [*BASELINE[:4], 99_000])

    assert s.detected == []


def test_an_extreme_outlier_is_reported_and_kept_out_of_the_baseline() -> None:
    s = scenario()
    drive(s, BASELINE)

    drive(s, [200_000], first_lap=7)
    [event] = s.detected
    assert event.severity is Severity.HIGH
    assert [lap["lap_number"] for lap in memory(s)["laps"]] == [2, 3, 4, 5, 6]

    drive(s, [90_100], first_lap=8)  # the next normal lap is still judged against the old baseline
    assert len(s.detected) == 1
    assert [lap["lap_number"] for lap in memory(s)["laps"]] == [3, 4, 5, 6, 8]


def test_a_run_of_slow_laps_is_reported_once_and_then_becomes_the_new_pace() -> None:
    s = scenario()
    drive(s, BASELINE)

    drive(s, [97_000, 97_100, 96_900, 97_050, 97_000, 97_000, 96_950, 97_050], first_lap=7)

    assert len(s.detected) == 1  # one event for the run, none once it is the new level
    assert all(96_900 <= lap["lap_time_ms"] <= 97_100 for lap in memory(s)["laps"])


def test_a_faster_lap_is_not_reported() -> None:
    s = scenario()

    drive(s, [*BASELINE, 84_000])

    assert s.detected == []


def test_a_new_stint_has_its_own_baseline() -> None:
    s = scenario()
    drive(s, BASELINE)
    s.round(7, [Entry("AAA", 0, lap_time=105_000, pit_in=True)])
    s.round(8, [Entry("AAA", 0, lap_time=110_000, pit_out=True, stint=2, compound="HARD")])

    drive(s, [96_000, 96_000, 96_000, 96_000, 96_000], first_lap=9, stint=2, compound="HARD")

    assert s.detected == []
