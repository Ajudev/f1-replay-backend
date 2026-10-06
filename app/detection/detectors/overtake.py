"""``OVERTAKE``: two adjacent drivers swapped positions between lap ends.

Positions are lap-end values (``LAP_COMPLETED`` / ``POSITION_CHANGED``); an overtake is
therefore only ever known to have happened *during* a lap, never where. The swap is
confirmed when the later of the two drivers completes lap N (the earlier one's new
position is already known by then): the driver now at position ``p`` was at ``p + 1`` at
the end of lap N-1 and the driver now at ``p + 1`` was at ``p``, both on lap N.

It is reported only when it is likely to be a pass on track: neither driver has a pit
in or out lap on laps N or N-1, neither is in the pit lane, both are running, and the
whole of lap N was driven in conditions that are not neutralised for both. A position
change that does not meet this (a pit cycle, a safety car period) is not an overtake.
Multi-position changes are only reported when they decompose into exactly one adjacent
swap, otherwise they are skipped, as are retirements (the retired driver never
completes the lap), and swaps where pit information for either driver on laps N or
N-1 is missing (unknown is not "on track"). Lap 1 is skipped: the grid positions it would compare against are
not lap-end data. Evidence states the lap-end basis; nothing finer is claimed.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.detection.pace import lap_window_condition
from app.detection.track_status import WindowCondition
from app.domain.enums import DetectedEventType, EventType
from app.race_state.models import DriverRaceStatus, DriverState, LapRecord, PitStatus

logger = logging.getLogger(__name__)

CLASSIFICATION_ON_TRACK_LIKELY = "ON_TRACK_LIKELY"
BASIS_LAP_END = "LAP_END"


class OvertakeMemory(BaseModel):
    #: Reported swaps as ``[lap, overtaker_id, overtaken_id]`` (bounded).
    reported: list[list[str | int]] = Field(default_factory=list)


class _Crossing:
    """A driver's lap-end positions around lap N."""

    __slots__ = ("current", "driver", "previous")

    def __init__(self, driver: DriverState, previous: LapRecord, current: LapRecord) -> None:
        self.driver = driver
        self.previous = previous
        self.current = current

    @property
    def old(self) -> int:
        assert self.previous.position is not None
        return self.previous.position

    @property
    def new(self) -> int:
        assert self.current.position is not None
        return self.current.position


def _crossing(data: DetectorInput, driver: DriverState, lap: int) -> _Crossing | None:
    current, previous = data.lap_record(driver, lap), data.lap_record(driver, lap - 1)
    if current is None or previous is None:
        return None
    if current.position is None or previous.position is None:
        return None
    return _Crossing(driver, previous, current)


def _pit_flags(crossing: _Crossing) -> list[bool | None]:
    return [
        flag
        for record in (crossing.previous, crossing.current)
        for flag in (record.is_pit_in_lap, record.is_pit_out_lap)
    ]


def _pit_related(crossing: _Crossing) -> bool:
    return any(flag is True for flag in _pit_flags(crossing)) or (
        crossing.driver.pit_status is PitStatus.IN_PIT
    )


def _pit_unknown(crossing: _Crossing) -> bool:
    """Missing pit information: the swap cannot be called an on-track pass."""
    return any(flag is None for flag in _pit_flags(crossing)) or (
        crossing.driver.pit_status is PitStatus.UNKNOWN
    )


class OvertakeDetector(Detector[OvertakeMemory]):
    name = "overtake"
    version = 1
    triggers = frozenset({EventType.LAP_COMPLETED.value, EventType.POSITION_CHANGED.value})
    memory_model = OvertakeMemory

    def evaluate(
        self, data: DetectorInput, memory: OvertakeMemory
    ) -> tuple[list[DetectionDraft], OvertakeMemory]:
        driver = data.driver
        if driver is None or data.rebuilt:
            return [], memory
        if data.source.event_type == EventType.LAP_COMPLETED.value:
            lap = data.source.lap_number
        else:
            lap = driver.laps_completed
        if lap is None or lap <= 1:
            return [], memory

        mine = _crossing(data, driver, lap)
        if mine is None or mine.new == mine.old:
            return [], memory
        if abs(mine.new - mine.old) != 1:
            self._skip(data, lap, "multi_position_change")
            return [], memory

        partners, ambiguous = self._partners(data, mine, lap)
        if ambiguous:
            self._skip(data, lap, "ambiguous_positions")
            return [], memory
        if len(partners) != 1:
            return [], memory  # the other driver has not completed the lap yet
        other = partners[0]

        winner, loser = (mine, other) if mine.new < mine.old else (other, mine)
        reason = self._unlikely_on_track(data, winner, loser, lap)
        if reason is not None:
            self._skip(data, lap, reason)
            return [], memory

        memory = memory.model_copy(deep=True)
        marker: list[str | int] = [lap, str(winner.driver.driver_id), str(loser.driver.driver_id)]
        if marker in memory.reported:
            return [], memory
        memory.reported.append(marker)
        del memory.reported[: -data.config.overtake.memory_swaps]

        draft = DetectionDraft(
            event_type=DetectedEventType.OVERTAKE,
            primary=DriverRef.of(winner.driver),
            secondary=DriverRef.of(loser.driver),
            lap_number=lap,
            key=f"lap:{lap}",
            evidence={
                "lap": lap,
                "overtaker_previous_position": winner.old,
                "overtaker_new_position": winner.new,
                "overtaken_previous_position": loser.old,
                "overtaken_new_position": loser.new,
                "classification": CLASSIFICATION_ON_TRACK_LIKELY,
                "basis": BASIS_LAP_END,
                "confirmed_by": driver.abbreviation,
            },
        )
        return [draft], memory

    @staticmethod
    def _partners(data: DetectorInput, mine: _Crossing, lap: int) -> tuple[list[_Crossing], bool]:
        """Drivers that completed lap N with exactly the mirrored position change, and
        whether a third driver claims one of the two positions (ambiguous).

        Only crossings within one lap time of this one count, so a lapped car that
        happens to have reported the same positions much later is not mistaken for it.
        """
        horizon = mine.current.lap_time_ms or 0
        found: list[_Crossing] = []
        ambiguous = False
        for other in data.current.drivers.values():
            if other.driver_id == mine.driver.driver_id:
                continue
            theirs = _crossing(data, other, lap)
            if theirs is None:
                continue
            if horizon and abs(theirs.current.race_time_ms - mine.current.race_time_ms) > horizon:
                continue
            if theirs.old == mine.new and theirs.new == mine.old:
                found.append(theirs)
            elif theirs.new in (mine.new, mine.old):
                ambiguous = True
        return found, ambiguous

    @staticmethod
    def _unlikely_on_track(
        data: DetectorInput, winner: _Crossing, loser: _Crossing, lap: int
    ) -> str | None:
        for crossing in (winner, loser):
            if _pit_related(crossing):
                return "pit_related"
            if _pit_unknown(crossing):
                return "pit_unknown"
            if crossing.driver.race_status not in (
                DriverRaceStatus.RUNNING,
                DriverRaceStatus.FINISHED,
            ):
                return "not_running"
            if lap_window_condition(data, crossing.driver, crossing.current) is not (
                WindowCondition.GREEN
            ):
                return "track_status"
        return None

    @staticmethod
    def _skip(data: DetectorInput, lap: int, reason: str) -> None:
        logger.debug(
            "Position change is not reported as overtake replay_id=%s lap=%d driver=%s reason=%s",
            data.replay_id,
            lap,
            data.source.driver_abbreviation,
            reason,
        )
