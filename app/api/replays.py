"""Replay lifecycle HTTP routes (thin; the replay service owns the state machine)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, status

from app.api.dependencies import ReplayServiceDep, get_replay_service
from app.api.exceptions import error_responses
from app.schemas.replays import ReplayCreateRequest, ReplayResponse, ReplaySpeedRequest

__all__ = ["router", "get_replay_service"]

router = APIRouter(prefix="/replays", tags=["Replays"])

_COMMAND_ERRORS = error_responses(404, 409, 503)


@router.post(
    "",
    response_model=ReplayResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a replay",
    responses=error_responses(404, 409, 422, 503),
)
async def create_replay(body: ReplayCreateRequest, service: ReplayServiceDep) -> ReplayResponse:
    """Create a replay (``CREATED``) for a session whose timeline has been generated."""
    return ReplayResponse.from_view(await service.create(body.session_id, body.playback_speed))


@router.get(
    "/{replay_id}",
    response_model=ReplayResponse,
    summary="Get replay status",
    responses=error_responses(404, 503),
)
async def get_replay(replay_id: UUID, service: ReplayServiceDep) -> ReplayResponse:
    return ReplayResponse.from_view(await service.get(replay_id))


@router.post(
    "/{replay_id}/start",
    response_model=ReplayResponse,
    summary="Start a created replay",
    responses=_COMMAND_ERRORS,
)
async def start_replay(replay_id: UUID, service: ReplayServiceDep) -> ReplayResponse:
    """``CREATED`` → ``RUNNING``; any other status is 409 ``INVALID_REPLAY_TRANSITION``."""
    return ReplayResponse.from_view(await service.start(replay_id))


@router.post(
    "/{replay_id}/pause",
    response_model=ReplayResponse,
    summary="Pause a running replay",
    responses=_COMMAND_ERRORS,
)
async def pause_replay(replay_id: UUID, service: ReplayServiceDep) -> ReplayResponse:
    """``RUNNING`` → ``PAUSED``."""
    return ReplayResponse.from_view(await service.pause(replay_id))


@router.post(
    "/{replay_id}/resume",
    response_model=ReplayResponse,
    summary="Resume a paused replay",
    responses=_COMMAND_ERRORS,
)
async def resume_replay(replay_id: UUID, service: ReplayServiceDep) -> ReplayResponse:
    """``PAUSED`` → ``RUNNING``."""
    return ReplayResponse.from_view(await service.resume(replay_id))


@router.post(
    "/{replay_id}/stop",
    response_model=ReplayResponse,
    summary="Stop a replay",
    responses=_COMMAND_ERRORS,
)
async def stop_replay(replay_id: UUID, service: ReplayServiceDep) -> ReplayResponse:
    """``RUNNING``/``PAUSED`` → ``STOPPED`` (terminal; use restart to run again)."""
    return ReplayResponse.from_view(await service.stop(replay_id))


@router.post(
    "/{replay_id}/restart",
    response_model=ReplayResponse,
    summary="Restart a replay from the beginning",
    responses=_COMMAND_ERRORS,
)
async def restart_replay(replay_id: UUID, service: ReplayServiceDep) -> ReplayResponse:
    """Any started status → ``RUNNING`` from race time 0 under a new run; keeps the speed."""
    return ReplayResponse.from_view(await service.restart(replay_id))


@router.patch(
    "/{replay_id}/speed",
    response_model=ReplayResponse,
    summary="Change playback speed",
    responses=error_responses(404, 422, 503),
)
async def change_replay_speed(
    replay_id: UUID, body: ReplaySpeedRequest, service: ReplayServiceDep
) -> ReplayResponse:
    """Allowed in any status; the virtual race position is preserved."""
    return ReplayResponse.from_view(await service.change_speed(replay_id, body.playback_speed))
