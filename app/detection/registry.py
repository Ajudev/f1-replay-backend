"""Application-level detector registry (no plugin framework, no name-based branching)."""

from __future__ import annotations

from collections.abc import Iterator

from app.detection.config import DetectionConfig
from app.detection.detectors.base import Detector


class DetectorRegistry:
    """Detectors in registration order; the engine dispatches by trigger."""

    def __init__(self) -> None:
        self._detectors: dict[str, Detector] = {}

    def register(self, detector: Detector) -> None:
        if detector.name in self._detectors:
            raise ValueError(f"Detector {detector.name!r} is already registered")
        self._detectors[detector.name] = detector

    def __iter__(self) -> Iterator[Detector]:
        return iter(self._detectors.values())

    def __len__(self) -> int:
        return len(self._detectors)

    def names(self) -> list[str]:
        return list(self._detectors)

    def for_event(self, source_event_type: str) -> list[Detector]:
        """Detectors whose triggers include ``source_event_type``, in registration order."""
        return [d for d in self._detectors.values() if source_event_type in d.triggers]


def build_default_registry(config: DetectionConfig | None = None) -> DetectorRegistry:
    """The built-in detectors, minus those disabled in the configuration.

    Adding a detector is one new module plus one line below.
    """
    from app.detection.detectors.anomaly import PaceAnomalyDetector
    from app.detection.detectors.battle import BattleDetector
    from app.detection.detectors.degradation import PaceDegradationDetector
    from app.detection.detectors.overtake import OvertakeDetector
    from app.detection.detectors.personal_best import PersonalBestDetector
    from app.detection.detectors.stint import StintDetector

    config = config or DetectionConfig()
    registry = DetectorRegistry()
    for detector in (
        BattleDetector(),
        OvertakeDetector(),
        PaceDegradationDetector(),
        PaceAnomalyDetector(),
        PersonalBestDetector(),
        StintDetector(),
    ):
        if detector.name not in config.disabled_detectors:
            registry.register(detector)
    return registry
