"""One prepared synthetic race over every detector: expectations and determinism.

Four drivers, 26 laps. Script:

- BBB closes on AAA from lap 2 (3.2 s -> 0.8 s by lap 5), then passes AAA on lap 12.
- CCC loses 1.2 s per lap from lap 7 on (a degrading stint).
- DDD pits at the end of lap 8 and drops behind CCC (a pit-induced position change).
- Safety car during laps 15-17 (slow laps, no detections wanted there).
- AAA sets a faster lap on lap 20.
"""

from __future__ import annotations

from typing import Any

from app.domain.enums import DetectedEventType, TrackStatus
from tests.detection.builders import LAP_MS, Entry, Scenario, logical

SC_LAPS = range(15, 18)
GAPS = {2: 3200, 3: 2400, 4: 1500, 5: 800}


def entries(lap: int) -> list[Entry]:
    sc = lap in SC_LAPS
    status = "SAFETY_CAR" if sc else "GREEN"
    base = 140_000 if sc else 90_000

    def make(abbr: str, offset: int, **kw: Any) -> Entry:
        kw.setdefault("lap_time", base)
        return Entry(abbr, offset, status=status, **kw)

    gap = GAPS.get(lap, 700)
    front = [make("AAA", 0), make("BBB", gap)] if lap < 12 else [make("BBB", 0), make("AAA", 300)]
    if lap == 20:
        front[-2 if lap < 12 else 1] = make("AAA", 300, lap_time=89_000)
    ccc_time = base if sc or lap < 7 else 91_200
    ccc = make("CCC", 31_000, lap_time=ccc_time)
    if lap == 8:
        ddd = make("DDD", 40_000, lap_time=110_000, pit_in=True)
    elif lap == 9:
        ddd = make("DDD", 40_000, lap_time=110_000, pit_out=True, stint=2, compound="HARD", age=0)
    elif lap > 9:
        ddd = make("DDD", 40_000, stint=2, compound="HARD", age=lap - 9)
    else:
        ddd = make("DDD", 29_000)
    back = [ccc, ddd] if lap >= 8 else [ddd, ccc]
    return front + back


def run_race() -> Scenario:
    s = Scenario()
    for lap in range(1, 27):
        if lap == SC_LAPS.start:
            s.track(TrackStatus.SAFETY_CAR, lap * LAP_MS - 50_000)
        s.round(lap, entries(lap))
        if lap == 8:
            s.pit_exit("DDD", 8 * LAP_MS + 60_000, 9, stint=2, compound="HARD")
        if lap == SC_LAPS.stop - 1:
            s.track(TrackStatus.GREEN, lap * LAP_MS + 60_000)
    return s


def of(s: Scenario, event_type: DetectedEventType) -> list:
    return s.of_type(event_type)


def test_the_scripted_race_produces_the_expected_detections() -> None:
    s = run_race()

    battles = of(s, DetectedEventType.BATTLE_FORMING)
    assert [(e.primary_driver_abbreviation, e.lap_number) for e in battles] == [("BBB", 5)]

    overtakes = of(s, DetectedEventType.OVERTAKE)
    assert [
        (e.primary_driver_abbreviation, e.secondary_driver_abbreviation, e.lap_number)
        for e in overtakes
    ] == [("BBB", "AAA", 12)]

    degradation = of(s, DetectedEventType.PACE_DEGRADATION)
    assert degradation and {e.primary_driver_abbreviation for e in degradation} == {"CCC"}

    bests = of(s, DetectedEventType.PERSONAL_BEST)
    assert [(e.primary_driver_abbreviation, e.lap_number) for e in bests] == [("AAA", 20)]

    stints = of(s, DetectedEventType.NEW_STINT)
    assert [(e.primary_driver_abbreviation, e.evidence["stint_number"]) for e in stints] == [
        ("DDD", 2)
    ]

    assert len(s.detected) < 12  # no per-lap noise


def test_nothing_is_detected_during_the_safety_car_laps() -> None:
    s = run_race()

    during = [e for e in s.detected if e.lap_number in SC_LAPS]
    assert during == []


def test_the_pit_induced_position_change_is_not_an_overtake() -> None:
    s = run_race()

    pit_swaps = [
        e
        for e in of(s, DetectedEventType.OVERTAKE)
        if "DDD" in (e.primary_driver_abbreviation, e.secondary_driver_abbreviation)
    ]
    assert pit_swaps == []


def test_two_runs_produce_identical_logical_event_sequences() -> None:
    first, second = run_race(), run_race()

    assert logical(first.detected) == logical(second.detected)
    assert first.detected  # not vacuous
    assert {e.detected_event_id for e in first.detected} == {
        e.detected_event_id for e in second.detected
    }


def test_redelivering_the_whole_stream_changes_nothing() -> None:
    s = run_race()
    expected = logical(s.detected)

    for event in list(s.state_events):
        s.feed(event)

    assert logical(s.detected) == expected
