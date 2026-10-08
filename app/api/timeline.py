"""Historical race timeline HTTP routes (thin; logic lives in the service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.domain.enums import EventType
from app.schemas.timeline import (
    TimelineEventOut,
    TimelineEventPage,
    TimelineGenerateRequest,
    TimelineSummaryResponse,
)
from app.timeline.service import TimelineService, TimelineSummary

router = APIRouter(tags=["Data Management"])

DbSession = Annotated[AsyncSession, Depends(get_db)]


def _summary_response(summary: TimelineSummary) -> TimelineSummaryResponse:
    return TimelineSummaryResponse(
        status=summary.status,
        session_id=summary.session_id,
        schema_version=summary.schema_version,
        is_current_schema_version=summary.is_current_schema_version,
        generated_at=summary.generated_at,
        race_start_session_time_ms=summary.race_start_session_time_ms,
        event_count=summary.event_count,
        counts_by_type=summary.counts_by_type,
        warnings=summary.warnings,
    )


@router.post(
    "/sessions/{session_id}/timeline",
    response_model=TimelineSummaryResponse,
    status_code=status.HTTP_201_CREATED,
)
async def generate_timeline(
    session_id: UUID,
    response: Response,
    session: DbSession,
    body: TimelineGenerateRequest | None = None,
) -> TimelineSummaryResponse:
    """Build and persist the historical timeline for an imported session."""
    request = body or TimelineGenerateRequest()
    summary = await TimelineService(session).generate(session_id, regenerate=request.regenerate)
    if summary.status == "already_generated":
        response.status_code = status.HTTP_200_OK
    return _summary_response(summary)


@router.get("/sessions/{session_id}/timeline/summary", response_model=TimelineSummaryResponse)
async def get_timeline_summary(session_id: UUID, session: DbSession) -> TimelineSummaryResponse:
    return _summary_response(await TimelineService(session).get_summary(session_id))


@router.get("/sessions/{session_id}/timeline", response_model=TimelineEventPage)
async def list_timeline_events(
    session_id: UUID,
    session: DbSession,
    driver: Annotated[str | None, Query(description="Driver abbreviation filter")] = None,
    event_type: Annotated[list[EventType] | None, Query(description="Repeatable")] = None,
    lap_from: Annotated[int | None, Query(ge=1)] = None,
    lap_to: Annotated[int | None, Query(ge=1)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> TimelineEventPage:
    page = await TimelineService(session).get_events(
        session_id,
        driver=driver,
        event_types=event_type,
        lap_from=lap_from,
        lap_to=lap_to,
        limit=limit,
        offset=offset,
    )
    return TimelineEventPage(
        items=[
            TimelineEventOut(
                id=item.id,
                sequence=item.sequence,
                event_type=item.event_type,
                race_time_ms=item.race_time_ms,
                lap_number=item.lap_number,
                driver_id=item.driver_id,
                driver_abbreviation=item.driver_abbreviation,
                payload=item.payload,
            )
            for item in page.items
        ],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )
