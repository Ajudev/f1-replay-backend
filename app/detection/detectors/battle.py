"""``BATTLE_FORMING`` and ``RAPIDLY_CLOSING``: one car catching the car directly ahead.

Evaluated when the *attacker* completes a lap. The defender is the driver who completed
the same lap one position ahead; the gap is the attacker's ``interval_to_ahead_ms``
(``LAP_END`` basis: the difference of the two lap-end crossing times, checked against the
defender's recorded crossing). The attacker keeps a window of one gap per consecutive
lap for the current pair. The window restarts (and an active battle ends) when the
defender changes, a lap is missing, the gap is unknown, either driver has a pit in or out
lap or is in the pit lane, or the lap was not driven in green conditions.

``BATTLE_FORMING`` needs a full window (``window_laps``), a current gap at or below
``gap_ms``, a net closing rate of at least ``min_closing_rate_ms`` per lap and a steady
decrease (at most ``max_non_closing_steps`` steps that did not close). A static
tailgater is not "forming" a battle and is never reported.

``RAPIDLY_CLOSING`` is the same trend measured further away: gap above ``gap_ms`` and up
to ``rapid_max_gap_ms`` closing by at least ``rapid_min_rate_ms`` per lap. It is the
approach; ``BATTLE_FORMING`` is arrival within ``gap_ms``. The two can follow each other
for the same approach on purpose, but ``RAPIDLY_CLOSING`` is never reported while the
pair is already in a battle.

Suppression: after ``BATTLE_FORMING`` the pair is *active* and nothing more is reported
for it until the gap exceeds ``release_gap_ms`` (hysteresis) or the pair is broken, and
then ``cooldown_laps`` laps must pass. ``RAPIDLY_CLOSING`` is reported once per approach
(re-armed when the gap leaves the zone or stops closing) and also respects the cooldown.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from app.detection.config import BattleConfig
from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.detection.pace import lap_window_condition
from app.detection.track_status import WindowCondition
from app.domain.enums import DetectedEventType, EventType
from app.race_state.models import DriverState, GapBasis, LapRecord, PitStatus

logger = logging.getLogger(__name__)


class GapObservation(BaseModel):
    lap: int
    gap_ms: int


class AttackerTrack(BaseModel):
    defender_id: str | None = None
    observations: list[GapObservation] = Field(default_factory=list)
    battle_active: bool = False
    approach_reported: bool = False
    #: Per defender: lap a battle with them ended / ``RAPIDLY_CLOSING`` was last reported.
    battle_ended: dict[str, int] = Field(default_factory=dict)
    rapid_reported: dict[str, int] = Field(default_factory=dict)


class BattleMemory(BaseModel):
    attackers: dict[str, AttackerTrack] = Field(default_factory=dict)


def _bounded(entries: dict[str, int], limit: int) -> None:
    while len(entries) > limit:
        del entries[min(entries, key=lambda k: entries[k])]


class BattleDetector(Detector[BattleMemory]):
    name = "battle"
    version = 1
    triggers = frozenset({EventType.LAP_COMPLETED.value})
    memory_model = BattleMemory

    def evaluate(
        self, data: DetectorInput, memory: BattleMemory
    ) -> tuple[list[DetectionDraft], BattleMemory]:
        found = data.source_lap_record()
        if found is None:
            return [], memory
        attacker, record = found
        cfg = data.config.battle
        memory = memory.model_copy(deep=True)
        track = memory.attackers.setdefault(str(attacker.driver_id), AttackerTrack())
        lap = record.lap_number

        defender_and_record, reason = self._defender(data, attacker, record)
        if defender_and_record is None:
            self._break(track, lap, cfg, reason)
            return [], memory
        defender, defender_record = defender_and_record
        defender_key = str(defender.driver_id)

        if track.defender_id != defender_key:
            self._break(track, lap, cfg, "defender_changed")
            track.defender_id = defender_key
        if track.observations and lap <= track.observations[-1].lap:
            return [], memory  # already observed
        if track.observations and lap != track.observations[-1].lap + 1:
            track.observations = []  # a lap is missing: not consecutive
            track.approach_reported = False
        assert attacker.interval_to_ahead_ms is not None
        track.observations.append(GapObservation(lap=lap, gap_ms=attacker.interval_to_ahead_ms))
        del track.observations[: -cfg.window_laps]

        draft = self._assess(track, cfg, attacker, defender, record, defender_record)
        return ([draft] if draft else []), memory

    # -- pair validity ---------------------------------------------------------------------

    @staticmethod
    def _defender(
        data: DetectorInput, attacker: DriverState, record: LapRecord
    ) -> tuple[tuple[DriverState, LapRecord] | None, str]:
        lap = record.lap_number
        if attacker.laps_completed != lap or record.position is None or record.position <= 1:
            return None, "no_car_ahead"
        if attacker.interval_to_ahead_ms is None or attacker.gap_basis is not GapBasis.LAP_END:
            return None, "gap_unknown"
        ahead: list[tuple[DriverState, LapRecord]] = []
        for other in data.current.drivers.values():
            other_record = data.lap_record(other, lap)
            if other_record is not None and other_record.position == record.position - 1:
                ahead.append((other, other_record))
        if len(ahead) != 1:
            return None, "defender_unclear"
        defender, defender_record = ahead[0]
        if record.race_time_ms - defender_record.race_time_ms != attacker.interval_to_ahead_ms:
            return None, "gap_inconsistent"
        for driver, driver_record in ((attacker, record), (defender, defender_record)):
            if driver_record.is_pit_in_lap is True or driver_record.is_pit_out_lap is True:
                return None, "pit_lap"
            if driver.pit_status is PitStatus.IN_PIT:
                return None, "in_pit"
            if lap_window_condition(data, driver, driver_record) is not WindowCondition.GREEN:
                return None, "track_status"
        return (defender, defender_record), ""

    @staticmethod
    def _break(track: AttackerTrack, lap: int, cfg: BattleConfig, reason: str) -> None:
        """The pair stopped being comparable: restart the window, end an active battle."""
        if track.battle_active and track.defender_id is not None:
            track.battle_ended[track.defender_id] = lap
            _bounded(track.battle_ended, cfg.memory_pairs)
            logger.debug("Battle ended lap=%d reason=%s", lap, reason)
        track.battle_active = False
        track.approach_reported = False
        track.observations = []
        track.defender_id = None

    # -- trend ----------------------------------------------------------------------------

    def _assess(
        self,
        track: AttackerTrack,
        cfg: BattleConfig,
        attacker: DriverState,
        defender: DriverState,
        record: LapRecord,
        defender_record: LapRecord,
    ) -> DetectionDraft | None:
        assert track.defender_id is not None
        lap = record.lap_number
        gaps = [o.gap_ms for o in track.observations]
        gap_now = gaps[-1]

        if track.battle_active and gap_now > cfg.release_gap_ms:
            track.battle_active = False
            track.battle_ended[track.defender_id] = lap
            _bounded(track.battle_ended, cfg.memory_pairs)
        if gap_now > cfg.rapid_max_gap_ms:
            track.approach_reported = False
        if len(gaps) < cfg.window_laps:
            return None

        steps = len(gaps) - 1
        rate = (gaps[0] - gaps[-1]) / steps
        non_closing = sum(
            1 for earlier, later in zip(gaps, gaps[1:], strict=False) if later >= earlier
        )
        steady = rate > 0 and non_closing <= cfg.max_non_closing_steps
        if rate <= 0:
            track.approach_reported = False
        if track.battle_active or not steady:
            return None

        ended = track.battle_ended.get(track.defender_id)
        if ended is not None and lap - ended < cfg.cooldown_laps:
            return None

        if gap_now <= cfg.gap_ms and rate >= cfg.min_closing_rate_ms:
            track.battle_active = True
            track.approach_reported = True
            track.rapid_reported[track.defender_id] = lap
            _bounded(track.rapid_reported, cfg.memory_pairs)
            return self._draft(
                DetectedEventType.BATTLE_FORMING,
                track,
                cfg,
                attacker,
                defender,
                record,
                defender_record,
                rate,
                {"threshold_ms": cfg.gap_ms},
            )

        if cfg.gap_ms < gap_now <= cfg.rapid_max_gap_ms and rate >= cfg.rapid_min_rate_ms:
            last = track.rapid_reported.get(track.defender_id)
            if track.approach_reported or (last is not None and lap - last < cfg.cooldown_laps):
                return None
            track.approach_reported = True
            track.rapid_reported[track.defender_id] = lap
            _bounded(track.rapid_reported, cfg.memory_pairs)
            return self._draft(
                DetectedEventType.RAPIDLY_CLOSING,
                track,
                cfg,
                attacker,
                defender,
                record,
                defender_record,
                rate,
                {"max_gap_ms": cfg.rapid_max_gap_ms, "min_closing_rate_ms": cfg.rapid_min_rate_ms},
            )
        return None

    @staticmethod
    def _draft(
        event_type: DetectedEventType,
        track: AttackerTrack,
        cfg: BattleConfig,
        attacker: DriverState,
        defender: DriverState,
        record: LapRecord,
        defender_record: LapRecord,
        rate: float,
        extra: dict[str, Any],
    ) -> DetectionDraft:
        return DetectionDraft(
            event_type=event_type,
            primary=DriverRef.of(attacker),
            secondary=DriverRef.of(defender),
            lap_number=record.lap_number,
            key=f"lap:{record.lap_number}",
            evidence={
                "gap_ms": track.observations[-1].gap_ms,
                "gap_history": [{"lap": o.lap, "gap_ms": o.gap_ms} for o in track.observations],
                "closing_rate_ms_per_lap": round(rate, 1),
                "observed_laps": len(track.observations),
                "window_laps": cfg.window_laps,
                "attacker_position": record.position,
                "defender_position": defender_record.position,
                "basis": GapBasis.LAP_END.value,
                **extra,
            },
        )
