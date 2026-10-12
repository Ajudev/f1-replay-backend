"""Circuit map HTTP routes: track data import, circuit layout, telemetry, battles (thin)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, status

from app.api.dependencies import DbSession, ReplayServiceDep
from app.api.exceptions import error_responses
from app.api.race_state import DriverPath
from app.core.config import get_settings
from app.detection.battles import ActiveBattleService
from app.ingestion.adapter import FastF1SessionLoader, TrackDataLoader
from app.ingestion.track_service import TrackDataImportService
from app.schemas.track import (
    ActiveBattleOut,
    ActiveBattlesResponse,
    CircuitLayoutResponse,
    LapTelemetryResponse,
    TrackDataImportResponse,
    TrackDataStatusResponse,
)
from app.services.track import CircuitService, TelemetryService

router = APIRouter(tags=["Track"])
import_router = APIRouter(tags=["Data Management"])


def get_track_data_loader() -> TrackDataLoader:
    """Default FastF1 loader; override in tests."""
    return FastF1SessionLoader(cache_dir=get_settings().fastf1_cache_dir)


def get_telemetry_service(request: Request) -> TelemetryService:
    return TelemetryService(request.app.state.replay_service, request.app.state.session_factory)


def get_battle_service(request: Request) -> ActiveBattleService:
    return request.app.state.battle_service


TrackDataLoaderDep = Annotated[TrackDataLoader, Depends(get_track_data_loader)]
TelemetryServiceDep = Annotated[TelemetryService, Depends(get_telemetry_service)]
BattleServiceDep = Annotated[ActiveBattleService, Depends(get_battle_service)]


@import_router.post(
    "/sessions/{session_id}/track-data",
    response_model=TrackDataImportResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Import a session's car positions, telemetry and circuit layout",
    responses=error_responses(404, 422, 500, 502),
)
async def import_track_data(
    session_id: UUID,
    request: Request,
    session: DbSession,
    loader: TrackDataLoaderDep,
) -> TrackDataImportResponse:
    """Downloads telemetry (slow, large). Requires the session's timeline; re-running
    replaces the previous import. Operator endpoint."""
    service = TrackDataImportService(
        loader, session, sample_interval_ms=get_settings().track_position_sample_ms
    )
    result = await service.import_track_data(session_id)
    cache = getattr(request.app.state, "position_cache", None)
    if cache is not None:
        cache.invalidate(session_id)
    return TrackDataImportResponse(
        status=result.status,  # type: ignore[arg-type]
        session_id=result.session_id,
        driver_count=result.driver_count,
        position_sample_count=result.position_sample_count,
        telemetry_sample_count=result.telemetry_sample_count,
        outline_points=result.outline_points,
        warnings=result.warnings,
    )


@import_router.get(
    "/sessions/{session_id}/track-data/status",
    response_model=TrackDataStatusResponse,
    summary="Whether a session's track data has been imported",
    responses=error_responses(404),
)
async def get_track_data_status(session_id: UUID, session: DbSession) -> TrackDataStatusResponse:
    layout = await CircuitService(session).find_layout(session_id)
    return TrackDataStatusResponse.build(session_id, layout)


@router.get(
    "/sessions/{session_id}/circuit",
    response_model=CircuitLayoutResponse,
    summary="Circuit layout of a session",
    responses=error_responses(404),
)
async def get_session_circuit(session_id: UUID, session: DbSession) -> CircuitLayoutResponse:
    """404 ``TRACK_DATA_NOT_IMPORTED`` until the session's track data is imported."""
    return CircuitLayoutResponse.build(await CircuitService(session).layout(session_id))


@router.get(
    "/replays/{replay_id}/circuit",
    response_model=CircuitLayoutResponse,
    summary="Circuit layout of a replay's session",
    responses=error_responses(404, 503),
)
async def get_replay_circuit(
    replay_id: UUID, session: DbSession, replays: ReplayServiceDep
) -> CircuitLayoutResponse:
    view = await replays.get(replay_id)
    return CircuitLayoutResponse.build(await CircuitService(session).layout(view.state.session_id))


@router.get(
    "/replays/{replay_id}/drivers/{driver}/telemetry",
    response_model=LapTelemetryResponse,
    summary="One driver's telemetry trace for a lap",
    responses=error_responses(404, 409, 503),
)
async def get_lap_telemetry(
    replay_id: UUID,
    driver: DriverPath,
    service: TelemetryServiceDep,
    lap: Annotated[int, Query(ge=1, le=200, description="Lap number")],
) -> LapTelemetryResponse:
    """Samples of the lap up to the replay clock (``complete`` is false while the lap is
    being driven). 409 ``LAP_NOT_REACHED`` for a lap the replay has not reached."""
    return LapTelemetryResponse.build(await service.lap_trace(replay_id, driver, lap))


@router.get(
    "/replays/{replay_id}/battles",
    response_model=ActiveBattlesResponse,
    summary="Battles in progress",
    responses=error_responses(404, 503),
)
async def get_active_battles(
    replay_id: UUID, replays: ReplayServiceDep, battles: BattleServiceDep
) -> ActiveBattlesResponse:
    """Attacker/defender pairs reported by ``BATTLE_FORMING`` and not yet ended by
    ``BATTLE_ENDED``, for the replay's current run."""
    await replays.get(replay_id)  # 404 for an unknown replay
    run_id, items = await battles.list(replay_id, replays.current_run_id(replay_id))
    return ActiveBattlesResponse(
        replay_id=replay_id, run_id=run_id, battles=[ActiveBattleOut.build(b) for b in items]
    )
