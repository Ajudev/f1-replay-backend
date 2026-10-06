"""``PACE_DEGRADATION``: a sustained slowdown within one stint.

On each ``LAP_COMPLETED`` that adds a clean lap, the driver's last ``recent_laps``
clean laps are compared with the ``baseline_laps`` clean laps immediately before them
(same stint only):

    delta = median(recent) - median(baseline)

A detection needs ``delta >= threshold`` *and* at least ``recent_laps - 1`` of the
recent laps slower than the baseline median, so one slow lap cannot trigger it.
It is reported once per stint and again only when the delta has grown by
``reemit_step_ms``. The event states the measured slowdown; it makes no claim about its
cause (tyre wear, fuel, traffic, pace management are indistinguishable here). A fuel
burn related speed-up is not corrected for, which makes the detector conservative.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.detection.pace import (
    CleanLap,
    DriverPaceHistory,
    is_contiguous_enough,
    median,
    observe_lap,
)
from app.domain.enums import DetectedEventType, EventType, Severity


class DegradationDriver(DriverPaceHistory):
    #: Delta of the last detection in this stint (``None`` = none yet).
    emitted_delta_ms: int | None = None

    def reset_stint(self) -> None:
        super().reset_stint()
        self.emitted_delta_ms = None


class DegradationMemory(BaseModel):
    drivers: dict[str, DegradationDriver] = Field(default_factory=dict)


def _lap_view(lap: CleanLap) -> dict[str, int]:
    return {"lap": lap.lap_number, "lap_time_ms": lap.lap_time_ms}


class PaceDegradationDetector(Detector[DegradationMemory]):
    name = "pace_degradation"
    version = 1
    triggers = frozenset({EventType.LAP_COMPLETED.value})
    memory_model = DegradationMemory

    def evaluate(
        self, data: DetectorInput, memory: DegradationMemory
    ) -> tuple[list[DetectionDraft], DegradationMemory]:
        found = data.source_lap_record()
        if found is None:
            return [], memory
        driver, record = found
        cfg = data.config.degradation
        memory = memory.model_copy(deep=True)
        history = memory.drivers.setdefault(str(driver.driver_id), DegradationDriver())

        reason = observe_lap(history, data, driver, record)
        if reason is not None:
            return [], memory
        history.add(record, keep=cfg.baseline_laps + cfg.recent_laps)

        needed = cfg.baseline_laps + cfg.recent_laps
        if not is_contiguous_enough(history.laps, needed, data.config.pace.max_excluded_laps):
            return [], memory
        window = history.laps[-needed:]
        baseline, recent = window[: cfg.baseline_laps], window[cfg.baseline_laps :]
        baseline_median = median([lap.lap_time_ms for lap in baseline])
        recent_median = median([lap.lap_time_ms for lap in recent])
        delta = round(recent_median - baseline_median)
        slower = sum(1 for lap in recent if lap.lap_time_ms > baseline_median)

        if delta < cfg.threshold_ms or slower < cfg.recent_laps - 1:
            return [], memory
        if history.emitted_delta_ms is not None and (
            delta < history.emitted_delta_ms + cfg.reemit_step_ms
        ):
            return [], memory
        history.emitted_delta_ms = delta

        severity = (
            Severity.HIGH
            if delta >= cfg.high_delta_ms
            else Severity.MEDIUM
            if delta >= cfg.medium_delta_ms
            else Severity.LOW
        )
        draft = DetectionDraft(
            event_type=DetectedEventType.PACE_DEGRADATION,
            primary=DriverRef.of(driver),
            lap_number=record.lap_number,
            severity=severity,
            evidence={
                "stint_number": record.stint_number,
                "compound": record.compound,
                "tyre_age_laps": record.tyre_age_laps,
                "baseline_median_ms": round(baseline_median),
                "recent_median_ms": round(recent_median),
                "delta_ms": delta,
                "slower_recent_laps": slower,
                "baseline_window_laps": cfg.baseline_laps,
                "recent_window_laps": cfg.recent_laps,
                "baseline_laps": [_lap_view(lap) for lap in baseline],
                "recent_laps": [_lap_view(lap) for lap in recent],
            },
        )
        return [draft], memory
