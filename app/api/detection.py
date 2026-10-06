"""Detected event HTTP routes (thin; logic lives in the detection service)."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.detection.repository import DetectedEventQuery
from app.detection.service import DetectedEventService
from app.domain.enums import DetectedEventType
from app.schemas.detection import DetectedEventOut, DetectedEventPageOut

router = APIRouter(prefix="/replays/{replay_id}/detected-events", tags=["detection"])

DbSession = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=DetectedEventPageOut)
async def list_detected_events(
    replay_id: UUID,
    session: DbSession,
    event_type: Annotated[list[DetectedEventType] | None, Query()] = None,
    driver: Annotated[str | None, Query(description="Abbreviation or driver UUID")] = None,
    lap_from: Annotated[int | None, Query(ge=1)] = None,
    lap_to: Annotated[int | None, Query(ge=1)] = None,
    run_id: Annotated[UUID | None, Query(description="Default: the latest run")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DetectedEventPageOut:
    """Detections of a replay ordered by source sequence, event type and id."""
    page = await DetectedEventService(session).list_events(
        DetectedEventQuery(
            replay_id=replay_id,
            run_id=run_id,
            event_types=tuple(event_type or ()),
            driver=driver,
            lap_from=lap_from,
            lap_to=lap_to,
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
