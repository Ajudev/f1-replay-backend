"""Registry, engine rules (idempotency, runs, bootstrap, rebuilds), contract and determinism."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import ClassVar
from uuid import UUID

import pytest
from pydantic import BaseModel

from app.detection.config import DetectionConfig
from app.detection.detectors.base import Detector
from app.detection.engine import Action, merge_delta
from app.detection.models import DetectionDraft, DetectorInput, DriverRef, StatePayload
from app.detection.registry import DetectorRegistry, build_default_registry
from app.domain.enums import DetectedEventType, EventType
from app.streaming.envelope import StreamEvent
from tests.detection.builders import (
    ALL_DETECTORS,
    BASE_TIME,
    LAP_MS,
    Entry,
    Scenario,
    driver_id,
    logical,
)
from tests.detection.test_battle import chase

RUN_B = UUID(int=2001)
GAPS = [3200, 2400, 1500, 800]


class SpyMemory(BaseModel):
    calls: int = 0


@dataclass
class Seen:
    types: list[str]
    rebuilt: list[bool]


class SpyDetector(Detector[SpyMemory]):
    name = "spy"
    version = 1
    triggers: ClassVar[frozenset[str]] = frozenset({EventType.PIT_EXIT.value})
    memory_model = SpyMemory

    def __init__(self) -> None:
        self.seen = Seen([], [])

    def evaluate(
        self, data: DetectorInput, memory: SpyMemory
    ) -> tuple[list[DetectionDraft], SpyMemory]:
        self.seen.types.append(data.source.event_type)
        self.seen.rebuilt.append(data.rebuilt)
        driver = data.driver
        assert driver is not None
        draft = DetectionDraft(
            event_type=DetectedEventType.NEW_STINT,
            primary=DriverRef.of(driver),
            evidence={"calls": memory.calls + 1},
        )
        return [draft], SpyMemory(calls=memory.calls + 1)


class BrokenDetector(Detector[SpyMemory]):
    name = "broken"
    version = 1
    triggers: ClassVar[frozenset[str]] = frozenset({EventType.LAP_COMPLETED.value})
    memory_model = SpyMemory

    def evaluate(
        self, data: DetectorInput, memory: SpyMemory
    ) -> tuple[list[DetectionDraft], SpyMemory]:
        raise RuntimeError("boom")


# -- registry ----------------------------------------------------------------------------------


def test_the_default_registry_dispatches_by_trigger() -> None:
    registry = build_default_registry()

    def names(event_type: str) -> list[str]:
        return [d.name for d in registry.for_event(event_type)]

    assert set(registry.names()) == set(ALL_DETECTORS)
    assert names("LAP_COMPLETED") == [
        "battle",
        "overtake",
        "pace_degradation",
        "pace_anomaly",
        "personal_best",
        "stint",
    ]
    assert names("POSITION_CHANGED") == ["overtake"]
    assert names("PIT_EXIT") == ["stint"]
    assert names("PIT_ENTRY") == [] and names("TRACK_STATUS_CHANGED") == []


def test_detectors_can_be_disabled_by_configuration() -> None:
    config = replace(DetectionConfig(), disabled_detectors=frozenset({"battle", "stint"}))

    assert "battle" not in build_default_registry(config).names()
    assert len(build_default_registry(config)) == len(ALL_DETECTORS) - 2


def test_a_name_can_only_be_registered_once() -> None:
    registry = DetectorRegistry()
    registry.register(SpyDetector())

    with pytest.raises(ValueError, match="already registered"):
        registry.register(SpyDetector())


def test_an_added_detector_runs_without_changing_the_engine() -> None:
    spy = SpyDetector()
    s = Scenario(["AAA"], detectors=[], extra=[spy])

    s.round(2, [Entry("AAA", 0)])  # LAP_COMPLETED: not a trigger of the spy
    assert spy.seen.types == []
    s.pit_exit("AAA", 3 * LAP_MS, 3, stint=2)

    assert spy.seen.types == ["PIT_EXIT"]
    assert [e.detector_name for e in s.detected] == ["spy"]


def test_only_detectors_with_a_matching_trigger_are_evaluated() -> None:
    spy = SpyDetector()
    s = Scenario(["AAA", "BBB"], detectors=["battle"], extra=[spy])

    chase(GAPS, scenario=s)

    assert spy.seen.types == []
    assert set(s.context.detectors) == {"battle"}  # type: ignore[union-attr]


# -- contract -----------------------------------------------------------------------------------


def test_a_detected_event_carries_the_full_contract() -> None:
    s = chase(GAPS)
    [event] = s.detected
    source_state_event = s.state_events[-1]

    assert event.schema_version == 1
    assert event.event_type is DetectedEventType.BATTLE_FORMING
    assert (event.replay_id, event.run_id) == (s.replay_id, s.run_id)
    assert event.session_id == source_state_event.session_id
    assert event.race_id is not None
    assert event.race_time_ms == source_state_event.race_time_ms
    assert event.primary_driver_id == driver_id("BBB")
    assert event.secondary_driver_id == driver_id("AAA")
    assert [str(i) for i in event.source_event_ids] == [
        source_state_event.payload["source_event_id"]
    ]
    assert event.source_sequence == source_state_event.sequence
    assert (event.detector_name, event.detector_version) == ("battle", 1)
    assert event.detected_at == BASE_TIME
    assert event.severity is None and event.confidence is None


def test_the_stream_envelope_wraps_the_detected_event() -> None:
    [event] = chase(GAPS).detected

    envelope = StreamEvent.from_fields(
        event.to_stream_event(published_at=BASE_TIME + timedelta(seconds=1)).to_fields()
    )

    assert envelope.event_id == event.detected_event_id
    assert envelope.event_type == "BATTLE_FORMING"
    assert envelope.sequence == event.source_sequence
    assert envelope.driver_abbreviation == "BBB"
    assert envelope.payload == event.model_dump(mode="json")
    assert envelope.payload["evidence"]["gap_ms"] == 800


def test_detected_event_ids_are_deterministic_and_distinct_per_run() -> None:
    first = chase(GAPS).detected[0]
    again = chase(GAPS).detected[0]
    other_run = chase(GAPS, scenario=Scenario(detectors=["battle"], run_id=RUN_B)).detected[0]

    assert first.detected_event_id == again.detected_event_id
    assert first.detected_event_id != other_run.detected_event_id


def test_two_runs_over_the_same_data_give_identical_logical_events() -> None:
    def run() -> list[dict]:
        s = Scenario(["AAA", "BBB", "CCC"])
        for lap, gap in zip(range(2, 10), [3200, 2400, 1500, 800, 700, 650, 600, 580], strict=True):
            s.round(
                lap,
                [Entry("AAA", 0, lap_time=90_000 + lap), Entry("BBB", gap), Entry("CCC", 30_000)],
            )
        return logical(s.detected)

    assert run() == run() != []


# -- idempotency, runs, recovery ----------------------------------------------------------------------


def test_a_duplicate_state_event_changes_nothing() -> None:
    s = chase(GAPS)
    before_context = s.context.model_dump_json()  # type: ignore[union-attr]
    before_events = len(s.detected)

    for event in s.state_events[-3:]:
        assert s.feed(event) == []

    assert len(s.detected) == before_events
    assert s.actions[-3:] == [Action.DUPLICATE] * 3
    assert s.context.model_dump_json() == before_context  # type: ignore[union-attr]


def test_duplicates_do_not_double_count_detector_history() -> None:
    clean = Scenario(["AAA", "BBB"], detectors=["pace_degradation"])
    noisy = Scenario(["AAA", "BBB"], detectors=["pace_degradation"])
    times = [90_000] * 5 + [91_000] * 5
    for scenario in (clean, noisy):
        for index, lap_time in enumerate(times):
            scenario.round(2 + index, [Entry("AAA", 0, lap_time=lap_time)])
    # Redeliver every event of the noisy run (as a consumer restart would).
    for event in list(noisy.state_events):
        noisy.feed(event)

    assert logical(noisy.detected) == logical(clean.detected) != []
    assert noisy.context.detectors == clean.context.detectors  # type: ignore[union-attr]


def test_a_new_run_resets_the_context_and_detector_memory() -> None:
    old = chase(GAPS)  # battle active in run A
    new = Scenario(detectors=["battle"], run_id=RUN_B)
    chase(GAPS, scenario=new)

    for event in new.state_events:
        old.feed(event)

    assert old.context.run_id == RUN_B  # type: ignore[union-attr]
    assert Action.RESET in old.actions
    # The new run's battle is reported although run A's battle was still active.
    assert [e.run_id for e in old.detected] == [old.run_id, RUN_B]


def test_stale_events_of_an_older_run_are_ignored() -> None:
    old = chase(GAPS[:2])
    new = Scenario(detectors=["battle"], run_id=RUN_B)
    chase(GAPS[:1], scenario=new)
    for event in new.state_events:
        old.feed(event)
    straggler = replace(old.state_events[-1], published_at=BASE_TIME - timedelta(seconds=5))

    assert old.feed(straggler) == []

    assert old.actions[-1] is Action.STALE
    assert old.context.run_id == RUN_B  # type: ignore[union-attr]


def test_a_context_is_bootstrapped_from_the_race_state_for_an_update() -> None:
    s = Scenario(["AAA", "BBB"], detectors=["battle"])
    chase(GAPS[:3], scenario=s)
    assert s.context is not None
    s.context = None  # the worker joins mid-run without a context
    s.round(5, [Entry("AAA", 0)])
    assert s.actions[-1] is Action.SKIP and s.context is None  # nothing to bootstrap from

    event = s.state_events[-1]
    s.feed(event, bootstrap_state=s.state)

    assert s.actions[-1] is Action.BOOTSTRAP
    assert s.context is not None and s.context.last_sequence == event.sequence
    assert s.context.state.drivers[driver_id("AAA")].laps_completed == 5
    assert s.context.detectors["battle"].data["attackers"] != {}  # detectors ran, memory fresh
    assert s.detected == []


def test_bootstrap_is_refused_when_the_race_state_cannot_vouch_for_the_event() -> None:
    s = Scenario(["AAA", "BBB"], detectors=["battle"])
    chase(GAPS[:2], scenario=s)
    event = s.state_events[-1]
    s.context = None
    other_run = s.state.model_copy(update={"run_id": RUN_B})
    behind = s.state.model_copy(update={"last_sequence": event.sequence - 1})

    for state in (None, other_run, behind):
        s.feed(event, bootstrap_state=state)
        assert s.actions[-1] is Action.SKIP
        assert s.context is None


def test_windows_refill_after_a_bootstrap() -> None:
    s = Scenario(["AAA", "BBB"], detectors=["battle"])
    chase(GAPS[:2], scenario=s)
    s.context = None  # restart without a context; the next update bootstraps from the state
    s.round(4, [Entry("AAA", 0), Entry("BBB", 1500)])
    s.feed(s.state_events[-2], bootstrap_state=s.state)
    s.feed(s.state_events[-1])
    s.round(5, [Entry("AAA", 0), Entry("BBB", 800)])

    # Only laps 4 and 5 were observed since the bootstrap: no full window, no battle.
    assert s.detected == []


def test_a_rebuilt_snapshot_replaces_the_mirror_and_flags_detectors() -> None:
    spy = SpyDetector()
    s = Scenario(["AAA", "BBB"], detectors=[], extra=[spy])
    s.round(2, [Entry("AAA", 0)])
    s.emit(EventType.TRACK_STATUS_CHANGED, 3 * LAP_MS, payload={"status": "YELLOW"})

    s.emit(
        EventType.PIT_EXIT,
        3 * LAP_MS + 10,
        "AAA",
        3,
        payload={"stint_number": 2, "compound": "HARD", "tyre_age_laps": 0},
        rebuilt=True,
    )

    assert spy.seen.rebuilt == [True]
    assert s.context.state.track_status.value == "YELLOW"  # type: ignore[union-attr]
    assert s.context.track_history.changes[-1].status.value == "YELLOW"  # type: ignore[union-attr]


def test_a_failing_detector_does_not_block_the_others_or_the_context() -> None:
    s = Scenario(["AAA", "BBB"], detectors=["personal_best"], extra=[BrokenDetector()])

    s.round(2, [Entry("AAA", 0, lap_time=91_000)])
    s.round(3, [Entry("AAA", 0, lap_time=90_000)])

    assert [e.detector_name for e in s.detected] == ["personal_best"]
    assert s.context.last_sequence == s.state_events[-1].sequence  # type: ignore[union-attr]
    assert "broken" not in s.context.detectors  # type: ignore[union-attr]


def test_memory_of_another_detector_version_is_discarded() -> None:
    s = Scenario(["AAA"], detectors=["personal_best"])
    s.round(2, [Entry("AAA", 0, lap_time=91_000)])
    s.context.detectors["personal_best"].version = 99  # type: ignore[union-attr]

    s.round(3, [Entry("AAA", 0, lap_time=90_000)])

    assert s.detected == []  # the benchmark was forgotten: lap 3 is a first lap again


def test_merge_delta_does_not_touch_its_input() -> None:
    s = chase(GAPS[:2])
    base = s.context.state  # type: ignore[union-attr]
    snapshot = base.model_dump_json()
    event = s.state_events[-1]

    merged = merge_delta(base, StatePayload.model_validate(event.payload), event)

    assert base.model_dump_json() == snapshot
    assert merged.last_sequence == event.sequence


@pytest.mark.parametrize(
    "battle",
    [
        {"gap_ms": 2_000, "release_gap_ms": 1_500},
        {"gap_ms": 1_000, "rapid_max_gap_ms": 1_000},
        {"window_laps": 1},
    ],
)
def test_invalid_battle_configuration_is_rejected(battle: dict) -> None:
    from app.detection.config import BattleConfig

    with pytest.raises(ValueError, match="Invalid detection configuration"):
        DetectionConfig(battle=BattleConfig(**battle))


def test_invalid_windows_are_rejected_and_defaults_are_valid() -> None:
    from app.detection.config import DegradationConfig

    DetectionConfig()
    with pytest.raises(ValueError, match="degradation windows"):
        DetectionConfig(degradation=DegradationConfig(recent_laps=1))
