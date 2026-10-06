"""PERSONAL_BEST."""

from __future__ import annotations

from app.domain.enums import DetectedEventType, TrackStatus
from tests.detection.builders import LAP_MS, Entry, Scenario, drive

PB = DetectedEventType.PERSONAL_BEST


def scenario() -> Scenario:
    return Scenario(["AAA", "BBB"], detectors=["personal_best"])


def test_the_first_valid_lap_only_sets_the_benchmark() -> None:
    s = scenario()

    drive(s, [91_000])

    assert s.detected == []


def test_a_faster_valid_lap_is_a_personal_best() -> None:
    s = scenario()

    drive(s, [91_000, 90_500])

    [event] = s.detected
    assert event.event_type is PB
    assert event.primary_driver_abbreviation == "AAA" and event.lap_number == 3
    assert event.evidence["lap_time_ms"] == 90_500
    assert event.evidence["previous_best_ms"] == 91_000
    assert event.evidence["previous_best_lap"] == 2
    assert event.evidence["improvement_ms"] == 500
    assert event.evidence["compound"] == "MEDIUM"
    assert event.severity is None and event.confidence is None


def test_slower_or_equal_laps_are_not() -> None:
    s = scenario()

    drive(s, [91_000, 91_500, 91_000, 92_000])

    assert s.detected == []


def test_each_improvement_is_reported_against_the_previous_best() -> None:
    s = scenario()

    drive(s, [91_000, 90_500, 90_700, 90_200])

    assert [(e.lap_number, e.evidence["improvement_ms"]) for e in s.detected] == [
        (3, 500),
        (5, 300),
    ]
    assert s.detected[1].evidence["previous_best_ms"] == 90_500


def test_invalid_laps_are_ignored() -> None:
    s = scenario()
    drive(s, [91_000])

    drive(s, [85_000], first_lap=3, deleted=True)  # deleted
    drive(s, [None], first_lap=4)  # no lap time
    s.round(5, [Entry("AAA", 0, lap_time=80_000, pit_out=True)])  # out lap
    s.round(6, [Entry("AAA", 0, lap_time=80_000, pit_in=True)])  # in lap

    assert s.detected == []


def test_lap_one_and_neutralised_laps_do_not_set_the_benchmark() -> None:
    s = scenario()
    s.round(1, [Entry("AAA", 0, lap_time=100_000)])  # standing start lap
    s.track(TrackStatus.VIRTUAL_SAFETY_CAR, 2 * LAP_MS - 60_000)
    s.round(2, [Entry("AAA", 0, lap_time=120_000, status="VIRTUAL_SAFETY_CAR")])
    s.track(TrackStatus.GREEN, 2 * LAP_MS + 5_000)

    drive(s, [91_000], first_lap=3)  # first eligible lap: benchmark only

    assert s.detected == []


def test_the_same_event_twice_is_reported_once() -> None:
    s = scenario()
    drive(s, [91_000, 90_500])

    assert s.feed(s.state_events[-1]) == []
    assert len(s.detected) == 1


def test_drivers_have_their_own_benchmark() -> None:
    s = scenario()
    s.round(2, [Entry("AAA", 0, lap_time=90_000), Entry("BBB", 3_000, lap_time=92_000)])
    s.round(3, [Entry("AAA", 0, lap_time=90_500), Entry("BBB", 3_000, lap_time=91_000)])

    assert [e.primary_driver_abbreviation for e in s.detected] == ["BBB"]


def test_a_gradual_fuel_burn_improvement_is_not_noise() -> None:
    s = scenario()

    drive(s, [91_000 - 30 * i for i in range(15)])

    assert len(s.detected) <= 1
    for event in s.detected:
        assert event.evidence["improvement_ms"] >= 300


def test_small_gains_add_up_against_the_last_reported_best() -> None:
    s = scenario()

    drive(s, [91_000, 90_800, 90_600, 90_400])

    [event] = s.detected
    assert event.lap_number == 4 and event.evidence["lap_time_ms"] == 90_600
    assert event.evidence["previous_best_ms"] == 91_000  # last reported (the benchmark)
    assert event.evidence["previous_true_best_ms"] == 90_800
    assert event.evidence["improvement_ms"] == 400


def test_the_true_best_is_tracked_even_when_not_reported() -> None:
    s = scenario()

    drive(s, [91_000, 90_900, 90_800])

    assert s.detected == []
    memory = s.context.detectors["personal_best"].data["drivers"]  # type: ignore[union-attr]
    [driver] = memory.values()
    assert driver["best"]["lap_time_ms"] == 90_800
    assert driver["reported"]["lap_time_ms"] == 91_000
