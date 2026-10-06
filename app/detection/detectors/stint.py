"""``NEW_STINT``: a driver starts a new set of tyres.

Reported on ``PIT_EXIT`` and, when pit data is missing, on a ``LAP_COMPLETED`` that
shows a higher stint number than the one last seen for the driver. Each
``(driver, stint)`` is reported once, however many events reveal it. The first stint a
driver is seen in is only the starting point, not a new stint. A compound the data does
not provide stays ``null``; it is never guessed.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.domain.enums import DetectedEventType, EventType
from app.race_state.models import DriverState

MAX_REMEMBERED_STINTS = 12


class StintDriver(BaseModel):
    last_stint_number: int | None = None
    last_compound: str | None = None
    #: Keys of reported stints (``stint:<n>``, or ``pit:<n>`` when the stint is unknown).
    reported: list[str] = Field(default_factory=list)


class StintMemory(BaseModel):
    drivers: dict[str, StintDriver] = Field(default_factory=dict)


class StintDetector(Detector[StintMemory]):
    name = "stint"
    version = 1
    triggers = frozenset({EventType.PIT_EXIT.value, EventType.LAP_COMPLETED.value})
    memory_model = StintMemory

    def evaluate(
        self, data: DetectorInput, memory: StintMemory
    ) -> tuple[list[DetectionDraft], StintMemory]:
        driver = data.driver
        if driver is None:
            return [], memory
        memory = memory.model_copy(deep=True)
        seen = memory.drivers.setdefault(str(driver.driver_id), StintDriver())

        if data.source.event_type == EventType.PIT_EXIT.value:
            if driver.stint_number is not None and driver.stint_number == seen.last_stint_number:
                return [], memory  # the stint is already known: not a new one
            starting_lap = driver.tyre_info_lap
            source = EventType.PIT_EXIT.value
        else:
            record = data.source_lap_record()
            if record is None or driver.stint_number is None:
                return [], memory
            if seen.last_stint_number is None or driver.stint_number < seen.last_stint_number:
                # First sighting (the starting stint) or older data: just remember.
                seen.last_stint_number = driver.stint_number
                seen.last_compound = driver.compound
                return [], memory
            if driver.stint_number == seen.last_stint_number:
                seen.last_compound = driver.compound or seen.last_compound
                return [], memory
            starting_lap = record[1].lap_number
            source = EventType.LAP_COMPLETED.value

        key = (
            f"stint:{driver.stint_number}"
            if driver.stint_number is not None
            else f"pit:{driver.pit_stop_count}"
        )
        if key in seen.reported:
            return [], memory
        previous_stint, previous_compound = seen.last_stint_number, seen.last_compound
        seen.reported.append(key)
        del seen.reported[:-MAX_REMEMBERED_STINTS]
        seen.last_stint_number = driver.stint_number
        seen.last_compound = driver.compound

        return [
            self._draft(driver, source, starting_lap, previous_stint, previous_compound)
        ], memory

    @staticmethod
    def _draft(
        driver: DriverState,
        source: str,
        starting_lap: int | None,
        previous_stint: int | None,
        previous_compound: str | None,
    ) -> DetectionDraft:
        return DetectionDraft(
            event_type=DetectedEventType.NEW_STINT,
            primary=DriverRef.of(driver),
            lap_number=starting_lap,
            key=f"stint:{driver.stint_number}:pit:{driver.pit_stop_count}",
            evidence={
                "stint_number": driver.stint_number,
                "compound": driver.compound,
                "previous_stint_number": previous_stint,
                "previous_compound": previous_compound,
                "compound_changed": (
                    None
                    if driver.compound is None or previous_compound is None
                    else driver.compound != previous_compound
                ),
                "starting_lap": starting_lap,
                "tyre_age_laps": driver.tyre_age_laps,
                "pit_lane_duration_ms": driver.last_pit_lane_duration_ms,
                "pit_stop_count": driver.pit_stop_count,
                "source_event_type": source,
            },
        )
