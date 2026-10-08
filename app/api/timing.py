"""Replay timing / chart data HTTP routes (thin; logic lives in the timing service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from app.api.dependencies import DRIVER_PATTERN, LapRangeDep, TimingServiceDep
from app.api.exceptions import error_responses
from app.api.race_state import DriverPath
from app.schemas.timing import DriverTimingResponse, ReplayTimingResponse

router = APIRouter(prefix="/replays/{replay_id}", tags=["Timing"])


@router.get(
    "/timing",
    response_model=ReplayTimingResponse,
    summary="Lap-by-lap timing series",
    responses=error_responses(404, 503),
)
async def get_replay_timing(
    replay_id: UUID,
    service: TimingServiceDep,
    laps: LapRangeDep,
    driver: Annotated[
        list[Annotated[str, Query(pattern=DRIVER_PATTERN)]] | None,
        Query(max_length=30, description="Repeatable abbreviation or UUID; default: all drivers"),
    ] = None,
) -> ReplayTimingResponse:
    """Per-driver lap points (lap time, position, gap to leader, tyre, pit flags, sectors)
    for laps the replay has already released."""
    result = await service.series(
        replay_id, drivers=driver, lap_from=laps.lap_from, lap_to=laps.lap_to
    )
    return ReplayTimingResponse(
        replay_id=result.replay_id,
        session_id=result.session_id,
        upto_sequence=result.upto_sequence,
        lap_from=laps.lap_from,
        lap_to=laps.lap_to,
        drivers=result.series,
    )


@router.get(
    "/drivers/{driver}/timing",
    response_model=DriverTimingResponse,
    summary="One driver's lap-by-lap timing series",
    responses=error_responses(404, 503),
)
async def get_driver_timing(
    replay_id: UUID, driver: DriverPath, service: TimingServiceDep, laps: LapRangeDep
) -> DriverTimingResponse:
    result = await service.series(
        replay_id, drivers=[driver], lap_from=laps.lap_from, lap_to=laps.lap_to
    )
    return DriverTimingResponse(
        replay_id=result.replay_id,
        session_id=result.session_id,
        upto_sequence=result.upto_sequence,
        lap_from=laps.lap_from,
        lap_to=laps.lap_to,
        driver=result.series[0],
    )
