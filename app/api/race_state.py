"""Race state HTTP routes (thin; logic lives in the race state service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from app.race_state.service import RaceStateService
from app.schemas.race_state import DriverStateResponse, RaceStateResponse

router = APIRouter(prefix="/replays/{replay_id}/state", tags=["race-state"])


def get_race_state_service(request: Request) -> RaceStateService:
    return request.app.state.race_state_service


Service = Annotated[RaceStateService, Depends(get_race_state_service)]


@router.get("", response_model=RaceStateResponse)
async def get_race_state(replay_id: UUID, service: Service) -> RaceStateResponse:
    """Current race state with drivers ordered by position."""
    view = await service.get_state(replay_id)
    return RaceStateResponse.build(view.state, view.source, view.replay_status)


@router.get("/drivers/{driver}", response_model=DriverStateResponse)
async def get_driver_state(replay_id: UUID, driver: str, service: Service) -> DriverStateResponse:
    """One driver's state, by abbreviation (case-insensitive) or driver UUID."""
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
