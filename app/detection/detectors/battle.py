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
then ``cooldown_laps`` laps must pass.

``BATTLE_ENDED`` closes every active battle exactly once, with a ``reason``:
``GAP_RELEASED`` (gap above ``release_gap_ms``), ``OVERTAKE`` (the attacker is now ahead
of the defender), ``PIT``, ``TRACK_STATUS`` (lap not green), ``PAIR_BROKEN`` (another car
in between, gap unknown or inconsistent), ``CONTACT_LOST`` (the defender completed two
more laps than the attacker's last observed lap, e.g. the attacker stopped) or
``RACE_FINISHED`` (the attacker took the flag). ``RAPIDLY_CLOSING`` is reported once per approach
(re-armed when the gap leaves the zone or stops closing) and also respects the cooldown.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from app.detection.config import BattleConfig
from app.detection.detectors.base import Detector
from app.detection.models import DetectionDraft, DetectorInput, DriverRef
from app.detection.pace import lap_window_condition
from app.detection.track_status import WindowCondition
from app.domain.enums import DetectedEventType, EventType
from app.race_state.models import DriverRaceStatus, DriverState, GapBasis, LapRecord, PitStatus

logger = logging.getLogger(__name__)


class GapObservation(BaseModel):
    lap: int
    gap_ms: int


class AttackerTrack(BaseModel):
    defender_id: str | None = None
    observations: list[GapObservation] = Field(default_factory=list)
    battle_active: bool = False
    #: Lap the active battle was reported (``BATTLE_FORMING``).
    battle_started_lap: int | None = None
    approach_reported: bool = False
    #: Per defender: lap a battle with them ended / ``RAPIDLY_CLOSING`` was last reported.
    battle_ended: dict[str, int] = Field(default_factory=dict)
    rapid_reported: dict[str, int] = Field(default_factory=dict)


class BattleMemory(BaseModel):
    attackers: dict[str, AttackerTrack] = Field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EndedBattle:
    defender_id: str
    started_lap: int | None
    gap_ms: int | None


#: ``_defender`` / ``_break`` reasons → public ``BATTLE_ENDED`` reasons.
_END_REASONS = {"pit_lap": "PIT", "in_pit": "PIT", "track_status": "TRACK_STATUS"}


def _bounded(entries: dict[str, int], limit: int) -> None:
    while len(entries) > limit:
        del entries[min(entries, key=lambda k: entries[k])]


class BattleDetector(Detector[BattleMemory]):
    name = "battle"
    version = 2
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
        lap = record.lap_number
        drafts = self._lost_contact(data, memory, attacker, lap, cfg)
        track = memory.attackers.setdefault(str(attacker.driver_id), AttackerTrack())
        drafts.extend(self._evaluate_pair(data, track, attacker, record, cfg))
        if attacker.race_status is DriverRaceStatus.FINISHED and track.battle_active:
            drafts.append(
                self._ended(data, attacker, self._break(track, lap, cfg), lap, "RACE_FINISHED")
            )
        return drafts, memory

    def _evaluate_pair(
        self,
        data: DetectorInput,
        track: AttackerTrack,
        attacker: DriverState,
        record: LapRecord,
        cfg: BattleConfig,
    ) -> list[DetectionDraft]:
        lap = record.lap_number
        drafts: list[DetectionDraft] = []
        defender_and_record, reason = self._defender(data, attacker, record)
        if defender_and_record is None:
            ended = self._break(track, lap, cfg)
            if ended is not None:
                drafts.append(
                    self._ended(
                        data,
                        attacker,
                        ended,
                        lap,
                        self._end_reason(data, attacker, ended, reason, lap),
                    )
                )
            return drafts
        defender, defender_record = defender_and_record
        defender_key = str(defender.driver_id)

        if track.defender_id != defender_key:
            ended = self._break(track, lap, cfg)
            if ended is not None:
                drafts.append(
                    self._ended(
                        data,
                        attacker,
                        ended,
                        lap,
                        self._end_reason(data, attacker, ended, "defender_changed", lap),
                    )
                )
            track.defender_id = defender_key
        if track.observations and lap <= track.observations[-1].lap:
            return drafts  # already observed
        if track.observations and lap != track.observations[-1].lap + 1:
            track.observations = []  # a lap is missing: not consecutive
            track.approach_reported = False
        assert attacker.interval_to_ahead_ms is not None
        track.observations.append(GapObservation(lap=lap, gap_ms=attacker.interval_to_ahead_ms))
        del track.observations[: -cfg.window_laps]

        if track.battle_active and track.observations[-1].gap_ms > cfg.release_gap_ms:
            ended = self._end(track, lap, cfg)
            drafts.append(self._ended(data, attacker, ended, lap, "GAP_RELEASED"))
        draft = self._assess(track, cfg, attacker, defender, record, defender_record)
        if draft is not None:
            drafts.append(draft)
        return drafts

    def _lost_contact(
        self,
        data: DetectorInput,
        memory: BattleMemory,
        defender: DriverState,
        lap: int,
        cfg: BattleConfig,
    ) -> list[DetectionDraft]:
        """End battles whose attacker fell out of contact with this (defending) driver."""
        drafts: list[DetectionDraft] = []
        defender_key = str(defender.driver_id)
        for attacker_key in sorted(memory.attackers):
            track = memory.attackers[attacker_key]
            if not (track.battle_active and track.defender_id == defender_key):
                continue
            last = track.observations[-1].lap if track.observations else None
            attacker = data.current.drivers.get(UUID(attacker_key))
            if attacker is None or (last is not None and lap < last + 2):
                continue
            drafts.append(
                self._ended(data, attacker, self._break(track, lap, cfg), lap, "CONTACT_LOST")
            )
        return drafts

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

    @classmethod
    def _break(cls, track: AttackerTrack, lap: int, cfg: BattleConfig) -> EndedBattle | None:
        """The pair stopped being comparable: restart the window, end an active battle."""
        ended = cls._end(track, lap, cfg) if track.battle_active else None
        track.approach_reported = False
        track.observations = []
        track.defender_id = None
        return ended

    @staticmethod
    def _end(track: AttackerTrack, lap: int, cfg: BattleConfig) -> EndedBattle:
        """End the active battle (starts the pair's cooldown)."""
        assert track.defender_id is not None
        ended = EndedBattle(
            defender_id=track.defender_id,
            started_lap=track.battle_started_lap,
            gap_ms=track.observations[-1].gap_ms if track.observations else None,
        )
        track.battle_ended[track.defender_id] = lap
        _bounded(track.battle_ended, cfg.memory_pairs)
        track.battle_active = False
        track.battle_started_lap = None
        return ended

    @staticmethod
    def _end_reason(
        data: DetectorInput, attacker: DriverState, ended: EndedBattle, reason: str, lap: int
    ) -> str:
        """``OVERTAKE`` when the attacker crossed the line of ``lap`` ahead of the defender
        (a defender stopping on track before the line reads the same; lap-end data cannot
        tell them apart)."""
        if reason in _END_REASONS:
            return _END_REASONS[reason]
        defender = data.current.drivers.get(UUID(ended.defender_id))
        record = data.lap_record(attacker, lap)
        if defender is None or record is None:
            return "PAIR_BROKEN"
        defender_record = data.lap_record(defender, lap)
        if defender_record is None:
            ahead = defender.laps_completed < lap
        else:
            ahead = (
                record.position is not None
                and defender_record.position is not None
                and record.position < defender_record.position
            )
        return "OVERTAKE" if ahead else "PAIR_BROKEN"

    @staticmethod
    def _ended(
        data: DetectorInput,
        attacker: DriverState,
        ended: EndedBattle | None,
        lap: int,
        reason: str,
    ) -> DetectionDraft:
        assert ended is not None
        defender = data.current.drivers.get(UUID(ended.defender_id))
        logger.debug("Battle ended lap=%d reason=%s", lap, reason)
        return DetectionDraft(
            event_type=DetectedEventType.BATTLE_ENDED,
            primary=DriverRef.of(attacker),
            secondary=DriverRef.of(defender) if defender is not None else None,
            lap_number=lap,
            key=f"lap:{lap}",
            evidence={
                "reason": reason,
                "gap_ms": ended.gap_ms,
                "started_lap": ended.started_lap,
                "ended_lap": lap,
                "attacker_position": attacker.position,
                "defender_position": defender.position if defender is not None else None,
            },
        )

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
            track.battle_started_lap = lap
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
