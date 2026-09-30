"""Race import and query HTTP routes."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.session import get_db
from app.ingestion.adapter import FastF1SessionLoader, SessionLoader
from app.ingestion.service import ImportService
from app.schemas.races import (
    LapPage,
    RaceDetail,
    RaceImportRequest,
    RaceImportResponse,
    RaceSummary,
    SessionDetail,
    StintOut,
    TrackStatusOut,
)
from app.services import race_queries

router = APIRouter(tags=["races"])

DbSession = Annotated[AsyncSession, Depends(get_db)]


def get_session_loader() -> SessionLoader:
    """Default FastF1 loader; override in tests."""
    settings = get_settings()
    return FastF1SessionLoader(cache_dir=settings.fastf1_cache_dir)


SessionLoaderDep = Annotated[SessionLoader, Depends(get_session_loader)]


@router.post(
    "/races/import",
    response_model=RaceImportResponse,
    status_code=status.HTTP_201_CREATED,
)
async def import_race(
    body: RaceImportRequest,
    response: Response,
    session: DbSession,
    loader: SessionLoaderDep,
) -> RaceImportResponse:
    """Import a historical session from FastF1 into PostgreSQL."""
    event: int | str = body.round if body.round is not None else str(body.event_name)
    service = ImportService(loader=loader, session=session)
    result = await service.import_session(
        body.season,
        event,
        body.session_type,
        replace=body.replace,
    )
    if result.status == "already_imported":
        response.status_code = status.HTTP_200_OK
    else:
        response.status_code = status.HTTP_201_CREATED

    return RaceImportResponse(
        status=result.status,  # type: ignore[arg-type]
        race_id=result.race_id,
        session_id=result.session_id,
        season=result.season,
        round=result.round,
        event_name=result.event_name,
        session_type=result.session_type,
        driver_count=result.driver_count,
        lap_count=result.lap_count,
        sector_count=result.sector_count,
        stint_count=result.stint_count,
        track_status_count=result.track_status_count,
        warnings=result.warnings,
        skipped_count=result.skipped_count,
    )


@router.get("/races", response_model=list[RaceSummary])
async def list_races(session: DbSession) -> list[RaceSummary]:
    return await race_queries.RaceQueryService(session).list_races()


@router.get("/races/{race_id}", response_model=RaceDetail)
async def get_race(race_id: UUID, session: DbSession) -> RaceDetail:
    return await race_queries.RaceQueryService(session).get_race(race_id)


@router.get("/sessions/{session_id}", response_model=SessionDetail)
async def get_session(session_id: UUID, session: DbSession) -> SessionDetail:
    return await race_queries.RaceQueryService(session).get_session(session_id)


@router.get("/sessions/{session_id}/laps", response_model=LapPage)
async def list_laps(
    session_id: UUID,
    session: DbSession,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    driver: Annotated[str | None, Query(description="Driver abbreviation filter")] = None,
) -> LapPage:
    return await race_queries.RaceQueryService(session).list_laps(
        session_id,
        limit=limit,
        offset=offset,
        driver=driver,
    )


@router.get("/sessions/{session_id}/stints", response_model=list[StintOut])
async def list_stints(session_id: UUID, session: DbSession) -> list[StintOut]:
    return await race_queries.RaceQueryService(session).list_stints(session_id)


@router.get("/sessions/{session_id}/track-status", response_model=list[TrackStatusOut])
async def list_track_status(session_id: UUID, session: DbSession) -> list[TrackStatusOut]:
    return await race_queries.RaceQueryService(session).list_track_status(session_id)
