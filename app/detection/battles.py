"""Battles currently in progress, read from the battle detector's persisted memory.

No second copy is kept: the detection context in Redis is the source, so the list is
exactly what the detector will close with ``BATTLE_ENDED``. Deterministic: the memory
is a function of the replay's events, and the list is ordered by attacker position.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from pydantic import ValidationError

from app.detection.detectors.battle import BattleDetector, BattleMemory
from app.detection.models import DetectionContext
from app.detection.store import DetectionContextStore


@dataclass(frozen=True, slots=True)
class ActiveBattle:
    attacker_id: UUID
    attacker_abbreviation: str
    defender_id: UUID
    defender_abbreviation: str
    started_lap: int | None
    last_gap_ms: int | None
    last_observed_lap: int | None


def active_battles(context: DetectionContext) -> list[ActiveBattle]:
    stored = context.detectors.get(BattleDetector.name)
    if stored is None or stored.version != BattleDetector.version:
        return []
    try:
        memory = BattleMemory.model_validate(stored.data)
    except ValidationError:
        return []
    drivers = context.state.drivers
    battles: list[ActiveBattle] = []
    for attacker_key, track in memory.attackers.items():
        if not track.battle_active or track.defender_id is None:
            continue
        attacker = drivers.get(UUID(attacker_key))
        defender = drivers.get(UUID(track.defender_id))
        if attacker is None or defender is None:
            continue
        last = track.observations[-1] if track.observations else None
        battles.append(
            ActiveBattle(
                attacker_id=attacker.driver_id,
                attacker_abbreviation=attacker.abbreviation,
                defender_id=defender.driver_id,
                defender_abbreviation=defender.abbreviation,
                started_lap=track.battle_started_lap,
                last_gap_ms=last.gap_ms if last else None,
                last_observed_lap=last.lap if last else None,
            )
        )
    battles.sort(key=lambda b: (drivers[b.attacker_id].position or 99, b.attacker_abbreviation))
    return battles


class ActiveBattleService:
    def __init__(self, store: DetectionContextStore) -> None:
        self._store = store

    async def list(
        self, replay_id: UUID, run_id: UUID | None
    ) -> tuple[UUID | None, list[ActiveBattle]]:
        """The run and its active battles. ``run_id`` is the replay's current run; when it
        is ``None`` (the replay has not run in this process, e.g. after a restart) the
        stored run is used, like the race state endpoints do. Another run gives nothing.

        Raises ``RedisError`` when Redis is unavailable.
        """
        context = await self._store.get(replay_id)
        if context is None or (run_id is not None and context.run_id != run_id):
            return run_id, []
        return context.run_id, active_battles(context)
