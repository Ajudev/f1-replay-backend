"""Detected event HTTP routes (thin; logic lives in the detection service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from app.api.dependencies import DbSession, DriverParam, LapRangeDep, LimitParam, OffsetParam
from app.api.exceptions import error_responses
from app.detection.repository import DetectedEventQuery
from app.detection.service import DetectedEventService
from app.domain.enums import DetectedEventType
from app.schemas.detection import DetectedEventOut, DetectedEventPageOut

router = APIRouter(prefix="/replays/{replay_id}/events", tags=["Events"])


@router.get(
    "",
    response_model=DetectedEventPageOut,
    summary="List detected events",
    responses=error_responses(404, 503),
)
async def list_detected_events(
    replay_id: UUID,
    session: DbSession,
    laps: LapRangeDep,
    event_type: Annotated[
        list[DetectedEventType] | None, Query(description="Repeatable; any of the given types")
    ] = None,
    driver: DriverParam = None,
    run_id: Annotated[UUID | None, Query(description="Default: the latest run")] = None,
    limit: LimitParam = 100,
    offset: OffsetParam = 0,
) -> DetectedEventPageOut:
    """Detections of a replay ordered by source sequence, event type and id.

    ``driver`` matches either the primary or the secondary driver.
    """
    page = await DetectedEventService(session).list_events(
        DetectedEventQuery(
            replay_id=replay_id,
            run_id=run_id,
            event_types=tuple(event_type or ()),
            driver=driver,
            lap_from=laps.lap_from,
            lap_to=laps.lap_to,
            limit=limit,
            offset=offset,
        )
    )
    return DetectedEventPageOut(
        replay_id=page.replay_id,
        run_id=page.run_id,
        items=[DetectedEventOut.from_row(row) for row in page.items],
        total=page.total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get(
    "/{event_id}",
    response_model=DetectedEventOut,
    summary="Get one detected event",
    responses=error_responses(404, 503),
)
async def get_detected_event(
    replay_id: UUID, event_id: UUID, session: DbSession
) -> DetectedEventOut:
    return DetectedEventOut.from_row(
        await DetectedEventService(session).get_event(replay_id, event_id)
    )
