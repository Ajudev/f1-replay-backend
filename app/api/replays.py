"""Replay control HTTP routes (thin; lifecycle logic lives in the replay service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, status

from app.replay.service import ReplayService, ReplayView
from app.schemas.replays import ReplayCreateRequest, ReplayResponse, ReplaySpeedRequest

router = APIRouter(prefix="/replays", tags=["replays"])


def get_replay_service(request: Request) -> ReplayService:
    return request.app.state.replay_service


Service = Annotated[ReplayService, Depends(get_replay_service)]


def _response(view: ReplayView) -> ReplayResponse:
    state = view.state
    return ReplayResponse(
        id=state.replay_id,
        session_id=state.session_id,
        race_id=view.race_id,
        status=state.status,
        status_reason=state.status_reason,
        playback_speed=float(state.playback_speed),
        current_race_time_ms=state.current_race_time_ms,
        current_sequence=state.current_sequence,
        emitted_event_count=state.emitted_event_count,
        total_events=state.total_events,
        current_lap=state.current_lap,
        total_laps=state.total_laps,
        created_at=view.created_at,
        started_at=state.started_at,
        paused_at=state.paused_at,
        ended_at=state.ended_at,
    )


@router.post("", response_model=ReplayResponse, status_code=status.HTTP_201_CREATED)
async def create_replay(body: ReplayCreateRequest, service: Service) -> ReplayResponse:
    """Create a replay (CREATED) for a session whose timeline has been generated."""
    return _response(await service.create(body.session_id, body.playback_speed))


@router.get("/{replay_id}", response_model=ReplayResponse)
async def get_replay(replay_id: UUID, service: Service) -> ReplayResponse:
    return _response(await service.get(replay_id))


@router.post("/{replay_id}/start", response_model=ReplayResponse)
async def start_replay(replay_id: UUID, service: Service) -> ReplayResponse:
    return _response(await service.start(replay_id))


@router.post("/{replay_id}/pause", response_model=ReplayResponse)
async def pause_replay(replay_id: UUID, service: Service) -> ReplayResponse:
    return _response(await service.pause(replay_id))


@router.post("/{replay_id}/resume", response_model=ReplayResponse)
async def resume_replay(replay_id: UUID, service: Service) -> ReplayResponse:
    return _response(await service.resume(replay_id))


@router.post("/{replay_id}/stop", response_model=ReplayResponse)
async def stop_replay(replay_id: UUID, service: Service) -> ReplayResponse:
    return _response(await service.stop(replay_id))


@router.post("/{replay_id}/restart", response_model=ReplayResponse)
async def restart_replay(replay_id: UUID, service: Service) -> ReplayResponse:
    return _response(await service.restart(replay_id))


@router.put("/{replay_id}/speed", response_model=ReplayResponse)
async def change_replay_speed(
    replay_id: UUID, body: ReplaySpeedRequest, service: Service
) -> ReplayResponse:
    return _response(await service.change_speed(replay_id, body.playback_speed))
