"""``PERSONAL_BEST``: a driver's fastest valid lap of the race so far, improved.

Eligible laps are the clean ones of ``app.detection.pace`` without the restart rule:
lap time present, not deleted, not lap 1, not an in or out lap, driven in conditions
that are not neutralised. The first eligible lap only establishes the benchmark (no
event); every later eligible lap that beats the benchmark is reported with the previous
best and the improvement. Distinct from the race-wide fastest lap
(``FASTEST_LAP`` / ``state.fastest_lap``), which is a single event for the whole field.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.detection.pace import DriverPaceHistory, observe_lap
from app.domain.enums import DetectedEventType, EventType


class BestLap(BaseModel):
    lap_number: int
    lap_time_ms: int


class PersonalBestDriver(DriverPaceHistory):
    #: The true best eligible lap so far.
    best: BestLap | None = None
    #: The best lap last reported (or the benchmark); improvements are measured against it.
    reported: BestLap | None = None


class PersonalBestMemory(BaseModel):
    drivers: dict[str, PersonalBestDriver] = Field(default_factory=dict)


class PersonalBestDetector(Detector[PersonalBestMemory]):
    name = "personal_best"
    version = 1
    triggers = frozenset({EventType.LAP_COMPLETED.value})
    memory_model = PersonalBestMemory

    def evaluate(
        self, data: DetectorInput, memory: PersonalBestMemory
    ) -> tuple[list[DetectionDraft], PersonalBestMemory]:
        found = data.source_lap_record()
        if found is None:
            return [], memory
        driver, record = found
        memory = memory.model_copy(deep=True)
        history = memory.drivers.setdefault(str(driver.driver_id), PersonalBestDriver())

        if observe_lap(history, data, driver, record, check_restart=False) is not None:
            return [], memory
        assert record.lap_time_ms is not None

        previous_true = history.best
        if previous_true is not None and record.lap_time_ms >= previous_true.lap_time_ms:
            return [], memory
        history.best = BestLap(lap_number=record.lap_number, lap_time_ms=record.lap_time_ms)
        if previous_true is None:
            history.reported = history.best
            return [], memory  # benchmark only
        previous = history.reported or previous_true
        improvement = previous.lap_time_ms - record.lap_time_ms
        if improvement < data.config.personal_best.min_improvement_ms:
            return [], memory  # real but too small to report; the true best is tracked
        history.reported = history.best

        draft = DetectionDraft(
            event_type=DetectedEventType.PERSONAL_BEST,
            primary=DriverRef.of(driver),
            lap_number=record.lap_number,
            evidence={
                "lap_time_ms": record.lap_time_ms,
                "previous_best_ms": previous.lap_time_ms,
                "previous_best_lap": previous.lap_number,
                "improvement_ms": improvement,
                "previous_true_best_ms": previous_true.lap_time_ms,
                "min_improvement_ms": data.config.personal_best.min_improvement_ms,
                "compound": record.compound,
                "tyre_age_laps": record.tyre_age_laps,
                "stint_number": record.stint_number,
            },
        )
        return [draft], memory
