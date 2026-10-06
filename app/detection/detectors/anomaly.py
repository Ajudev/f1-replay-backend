"""``PACE_ANOMALY``: one lap far slower than the driver's recent clean laps.

For a clean lap (see ``app.detection.pace``: not a pit lap, not deleted, driven in
green conditions) with a full baseline of the last ``baseline_laps`` clean laps of the
same stint:

    expected = median(baseline)
    score    = (actual - expected) / (1.4826 * max(MAD(baseline), mad_floor))

A detection needs ``score >= min_score`` *and* ``actual - expected >= min_deviation_ms``
(the second condition stops a very consistent driver's small deviations from scoring
high). Only the slow side is reported: a fast lap is not a pace problem. Anomalous laps
do not enter the baseline, so one bad lap does not distort the next comparison. A run of
consecutive anomalous laps is reported once; when it is as long as the baseline the
driver is on a new pace level and the run becomes the baseline.

Sector based anomalies are not implemented: the race state has no sector data.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.detection.pace import CleanLap, DriverPaceHistory, mad, median, observe_lap, robust_z
from app.domain.enums import DetectedEventType, EventType, Severity


class AnomalyDriver(DriverPaceHistory):
    #: Consecutive anomalous laps (not part of ``laps``).
    streak: list[CleanLap] = Field(default_factory=list)

    def reset_stint(self) -> None:
        super().reset_stint()
        self.streak = []


class AnomalyMemory(BaseModel):
    drivers: dict[str, AnomalyDriver] = Field(default_factory=dict)


class PaceAnomalyDetector(Detector[AnomalyMemory]):
    name = "pace_anomaly"
    version = 1
    triggers = frozenset({EventType.LAP_COMPLETED.value})
    memory_model = AnomalyMemory

    def evaluate(
        self, data: DetectorInput, memory: AnomalyMemory
    ) -> tuple[list[DetectionDraft], AnomalyMemory]:
        found = data.source_lap_record()
        if found is None:
            return [], memory
        driver, record = found
        cfg = data.config.anomaly
        memory = memory.model_copy(deep=True)
        history = memory.drivers.setdefault(str(driver.driver_id), AnomalyDriver())

        reason = observe_lap(history, data, driver, record)
        if reason is not None:
            return [], memory
        assert record.lap_time_ms is not None

        baseline = history.laps[-cfg.baseline_laps :]
        if len(baseline) < cfg.baseline_laps:
            history.add(record, keep=cfg.baseline_laps)
            return [], memory

        times = [lap.lap_time_ms for lap in baseline]
        expected = median(times)
        spread = mad(times, expected)
        deviation = record.lap_time_ms - expected
        score = robust_z(record.lap_time_ms, expected, spread, mad_floor=cfg.mad_floor_ms)
        if score < cfg.min_score or deviation < cfg.min_deviation_ms:
            history.streak = []
            history.add(record, keep=cfg.baseline_laps)
            return [], memory

        lap = CleanLap(
            lap_number=record.lap_number,
            lap_time_ms=record.lap_time_ms,
            race_time_ms=record.race_time_ms,
            stint_number=record.stint_number,
            compound=record.compound,
            tyre_age_laps=record.tyre_age_laps,
        )
        first_of_run = not history.streak
        history.streak.append(lap)
        if len(history.streak) >= cfg.baseline_laps:
            # A new pace level, not an anomaly: continue from it.
            history.laps = history.streak[-cfg.baseline_laps :]
            history.streak = []
        if not first_of_run:
            return [], memory

        severity = (
            Severity.HIGH
            if deviation >= cfg.high_deviation_ms
            else Severity.MEDIUM
            if deviation >= cfg.medium_deviation_ms
            else Severity.LOW
        )
        draft = DetectionDraft(
            event_type=DetectedEventType.PACE_ANOMALY,
            primary=DriverRef.of(driver),
            lap_number=record.lap_number,
            severity=severity,
            evidence={
                "stint_number": record.stint_number,
                "compound": record.compound,
                "tyre_age_laps": record.tyre_age_laps,
                "lap_time_ms": record.lap_time_ms,
                "expected_ms": round(expected),
                "deviation_ms": round(deviation),
                "robust_score": round(score, 2),
                "mad_ms": round(spread),
                "mad_floor_ms": cfg.mad_floor_ms,
                "baseline_laps": [
                    {"lap": b.lap_number, "lap_time_ms": b.lap_time_ms} for b in baseline
                ],
            },
        )
        return [draft], memory
