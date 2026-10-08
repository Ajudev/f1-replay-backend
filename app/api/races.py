"""Historical race, session and import HTTP routes (thin; logic lives in services)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status

from app.api.dependencies import DbSession, DriverParam, LapRangeDep, OffsetParam
from app.api.exceptions import error_responses
from app.core.config import get_settings
from app.domain.enums import SessionType
from app.ingestion.adapter import FastF1SessionLoader, SessionLoader
from app.ingestion.service import ImportService
from app.schemas.races import (
    DriverSummary,
    LapPage,
    RaceDetail,
    RaceImportRequest,
    RaceImportResponse,
    RaceSummary,
    SeasonSummary,
    SessionDetail,
    StintOut,
    TrackStatusOut,
)
from app.services.race_queries import RaceQueryService

router = APIRouter(tags=["Races"])
import_router = APIRouter(tags=["Data Management"])

LapLimit = Annotated[int, Query(ge=1, le=500, description="Page size")]
SessionTypeParam = Annotated[
    SessionType, Query(description="Which session of the event (default: the race)")
]


def get_session_loader() -> SessionLoader:
    """Default FastF1 loader; override in tests."""
    settings = get_settings()
    return FastF1SessionLoader(cache_dir=settings.fastf1_cache_dir)


SessionLoaderDep = Annotated[SessionLoader, Depends(get_session_loader)]


@import_router.post(
    "/races/import",
    response_model=RaceImportResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Import a historical session",
    responses=error_responses(404, 422, 500, 502),
)
async def import_race(
    body: RaceImportRequest,
    response: Response,
    session: DbSession,
    loader: SessionLoaderDep,
) -> RaceImportResponse:
    """Import a historical session into the database (201 imported/replaced, 200 already
    imported). Operator endpoint: it downloads historical data and can be slow."""
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


@router.get("/seasons", response_model=list[SeasonSummary], summary="List imported seasons")
async def list_seasons(session: DbSession) -> list[SeasonSummary]:
    """Seasons with at least one imported race, newest first."""
    return await RaceQueryService(session).list_seasons()


@router.get("/races", response_model=list[RaceSummary], summary="List imported races")
async def list_races(
    session: DbSession,
    season: Annotated[int | None, Query(ge=1950, le=2100)] = None,
    round: Annotated[int | None, Query(ge=1, le=30, description="Championship round")] = None,
    event: Annotated[
        str | None,
        Query(min_length=1, max_length=100, description="Case-insensitive name/location match"),
    ] = None,
    session_type: Annotated[
        SessionType | None, Query(description="Only races with this session imported")
    ] = None,
) -> list[RaceSummary]:
    """Imported races (newest season first, then by round) with their sessions."""
    return await RaceQueryService(session).list_races(
        season=season, round_number=round, event=event, session_type=session_type
    )


@router.get(
    "/races/{race_id}",
    response_model=RaceDetail,
    summary="Get race metadata",
    responses=error_responses(404),
)
async def get_race(race_id: UUID, session: DbSession) -> RaceDetail:
    return await RaceQueryService(session).get_race(race_id)


@router.get(
    "/races/{race_id}/drivers",
    response_model=list[DriverSummary],
    summary="List a race's drivers",
    responses=error_responses(404),
)
async def list_race_drivers(
    race_id: UUID, session: DbSession, session_type: SessionTypeParam = SessionType.RACE
) -> list[DriverSummary]:
    """Participants of one session of the event, ordered by abbreviation."""
    queries = RaceQueryService(session)
    return await queries.list_drivers(await queries.race_session_id(race_id, session_type))


@router.get(
    "/races/{race_id}/laps",
    response_model=LapPage,
    summary="List a race's historical laps",
    responses=error_responses(404),
)
async def list_race_laps(
    race_id: UUID,
    session: DbSession,
    laps: LapRangeDep,
    driver: DriverParam = None,
    session_type: SessionTypeParam = SessionType.RACE,
    limit: LapLimit = 100,
    offset: OffsetParam = 0,
) -> LapPage:
    """Complete historical laps (not limited by any replay), ordered by lap then driver."""
    queries = RaceQueryService(session)
    return await queries.list_laps(
        await queries.race_session_id(race_id, session_type),
        limit=limit,
        offset=offset,
        driver=driver,
        lap_from=laps.lap_from,
        lap_to=laps.lap_to,
    )


@router.get(
    "/sessions/{session_id}",
    response_model=SessionDetail,
    summary="Get session metadata",
    responses=error_responses(404),
)
async def get_session(session_id: UUID, session: DbSession) -> SessionDetail:
    return await RaceQueryService(session).get_session(session_id)


@router.get(
    "/sessions/{session_id}/laps",
    response_model=LapPage,
    summary="List a session's historical laps",
    responses=error_responses(404),
)
async def list_laps(
    session_id: UUID,
    session: DbSession,
    laps: LapRangeDep,
    driver: DriverParam = None,
    limit: LapLimit = 100,
    offset: OffsetParam = 0,
) -> LapPage:
    return await RaceQueryService(session).list_laps(
        session_id,
        limit=limit,
        offset=offset,
        driver=driver,
        lap_from=laps.lap_from,
        lap_to=laps.lap_to,
    )


@router.get(
    "/sessions/{session_id}/stints",
    response_model=list[StintOut],
    summary="List a session's tyre stints",
    responses=error_responses(404),
)
async def list_stints(
    session_id: UUID, session: DbSession, driver: DriverParam = None
) -> list[StintOut]:
    return await RaceQueryService(session).list_stints(session_id, driver=driver)


@router.get(
    "/sessions/{session_id}/track-status",
    response_model=list[TrackStatusOut],
    summary="List a session's track status periods",
    responses=error_responses(404),
)
async def list_track_status(session_id: UUID, session: DbSession) -> list[TrackStatusOut]:
    return await RaceQueryService(session).list_track_status(session_id)
