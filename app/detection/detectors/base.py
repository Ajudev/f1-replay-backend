"""Detector contract.

A detector is a pure function of ``(input, memory)``: no I/O, no wall clock, no
mutation of the input. ``memory`` is the detector's own pydantic model, persisted by
the engine between events under the detector's name; the detector returns the new
memory. Everything a detector needs to remember (histories, cooldowns, de-duplication)
must live there, because the engine only guarantees that each state event reaches a
detector once and that memories of different replays never mix.

Adding a detector: write a module with a subclass and register it in
``app.detection.registry.build_default_registry``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pydantic import BaseModel

from app.detection.models import DetectionDraft, DetectorInput


class Detector[M: BaseModel](ABC):
    #: Unique, stable name (namespaces the memory and goes into detected event ids).
    name: ClassVar[str]
    #: Bump when the rules or the memory shape change; stored memory of another version
    #: is discarded.
    version: ClassVar[int]
    #: Raw event types (``source_event_type``) the detector is evaluated for.
    triggers: ClassVar[frozenset[str]]
    memory_model: ClassVar[type[BaseModel]]

    def new_memory(self) -> M:
        return self.memory_model()  # type: ignore[return-value]

    @abstractmethod
    def evaluate(self, data: DetectorInput, memory: M) -> tuple[list[DetectionDraft], M]:
        """Return detections for this event and the updated memory."""
