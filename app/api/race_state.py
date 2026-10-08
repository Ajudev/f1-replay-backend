"""Race state HTTP routes (thin; logic lives in the race state service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Path

from app.api.dependencies import DRIVER_PATTERN, RaceStateServiceDep, get_race_state_service
from app.api.exceptions import error_responses
from app.schemas.race_state import DriverStateResponse, RaceStateResponse

__all__ = ["router", "get_race_state_service"]

router = APIRouter(prefix="/replays/{replay_id}", tags=["Race State"])

DriverPath = Annotated[
    str, Path(pattern=DRIVER_PATTERN, description="Driver abbreviation (case-insensitive) or UUID")
]


@router.get(
    "/state",
    response_model=RaceStateResponse,
    summary="Get the current race state",
    responses=error_responses(404, 409, 503),
)
async def get_race_state(replay_id: UUID, service: RaceStateServiceDep) -> RaceStateResponse:
    """Authoritative race snapshot with drivers ordered by position.

    409 ``REPLAY_NOT_STARTED`` before the replay starts; 404 ``RACE_STATE_UNAVAILABLE``
    when nothing has been processed yet. The final state stays available after completion.
    """
    view = await service.get_state(replay_id)
    return RaceStateResponse.build(view.state, view.source, view.replay_status)


@router.get(
    "/drivers/{driver}",
    response_model=DriverStateResponse,
    summary="Get one driver's current state",
    responses=error_responses(404, 409, 503),
)
async def get_driver_state(
    replay_id: UUID, driver: DriverPath, service: RaceStateServiceDep
) -> DriverStateResponse:
    """Position, gaps, laps, tyres, pit status and recent laps of one driver."""
    view, driver_state = await service.get_driver(replay_id, driver)
    state = view.state
    return DriverStateResponse(
        replay_id=state.replay_id,
        replay_status=view.replay_status,
        source=view.source,
        phase=state.phase,
        current_race_time_ms=state.current_race_time_ms,
        current_lap=state.current_lap,
        last_sequence=state.last_sequence,
        driver=driver_state,
    )
