"""Synthetic seeds and event sequences for reducer-level tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid5

from app.domain.enums import EventType
from app.race_state.config import RaceStateConfig
from app.race_state.models import RaceSeed, RaceState, SeedDriver, StateDelta
from app.race_state.reducer import ReducerEvent, apply_event, initial_state

REPLAY = UUID(int=100)
RUN = UUID(int=200)
SESSION = UUID(int=300)
VER, HAM, NOR = UUID(int=1), UUID(int=2), UUID(int=3)
ABBR = {VER: "VER", HAM: "HAM", NOR: "NOR"}
CONFIG = RaceStateConfig(lap_history=10, snapshot_every_laps=10)


def make_seed(total_laps: int | None = 5, total_events: int = 1000) -> RaceSeed:
    return RaceSeed(
        session_id=SESSION,
        race_id=UUID(int=400),
        season=2024,
        round=3,
        session_type="RACE",
        total_laps=total_laps,
        total_events=total_events,
        drivers=(
            SeedDriver(VER, "VER", 1, "Max Verstappen", "Red Bull", 1),
            SeedDriver(HAM, "HAM", 44, "Lewis Hamilton", "Mercedes", 2),
            SeedDriver(NOR, "NOR", 4, "Lando Norris", "McLaren", 3),
        ),
    )


def lap_payload(
    position: int | None = 1,
    lap_time: int | None = 90_000,
    *,
    stint: int | None = 1,
    compound: str | None = "SOFT",
    age: int | None = 1,
    deleted: bool | None = False,
    pit_in: bool = False,
    pit_out: bool = False,
) -> dict[str, Any]:
    return {
        "lap_time_ms": lap_time,
        "position": position,
        "compound": compound,
        "tyre_age_laps": age,
        "stint_number": stint,
        "is_deleted": deleted,
        "is_accurate": True,
        "is_pit_in_lap": pit_in,
        "is_pit_out_lap": pit_out,
        "track_status": "GREEN",
        "completion_time_source": "LAP_END_TIME",
    }


class Sim:
    """Applies events one after another, keeping the sequence contiguous."""

    def __init__(
        self,
        *,
        seed: RaceSeed | None = None,
        config: RaceStateConfig = CONFIG,
        started: bool = True,
    ) -> None:
        self.config = config
        self.seed = seed or make_seed()
        self.state: RaceState = initial_state(self.seed, replay_id=REPLAY, run_id=RUN)
        self.sequence = 0
        self.events: list[ReducerEvent] = []
        if started:
            self.start()

    def start(self) -> StateDelta:
        grid = [
            {
                "driver_id": str(d.driver_id),
                "abbreviation": d.abbreviation,
                "grid_position": d.grid_position,
            }
            for d in self.seed.drivers
        ]
        return self.emit(EventType.RACE_STARTED, 0, payload={"grid": grid, "season": 2024})

    def emit(
        self,
        event_type: EventType | str,
        race_time_ms: int,
        driver: UUID | None = None,
        lap: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> StateDelta:
        event = ReducerEvent(
            event_id=uuid5(RUN, str(self.sequence)),
            event_type=str(event_type),
            sequence=self.sequence,
            race_time_ms=race_time_ms,
            lap_number=lap,
            driver_id=driver,
            driver_abbreviation=ABBR.get(driver) if driver else None,
            payload=payload or {},
        )
        self.events.append(event)
        self.sequence += 1
        self.state, delta = apply_event(self.state, event, config=self.config)
        return delta

    def lap(self, driver: UUID, lap: int, time_ms: int, **kwargs: Any) -> StateDelta:
        return self.emit(EventType.LAP_COMPLETED, time_ms, driver, lap, lap_payload(**kwargs))
