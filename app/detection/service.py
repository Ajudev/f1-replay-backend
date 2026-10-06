"""Read side of the detected events (the API's application service)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.detection.repository import DetectedEventQuery, DetectedEventRepository
from app.models import DetectedEvent as DetectedEventRow
from app.replay.errors import ReplayNotFoundError, ReplayPersistenceError

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DetectedEventPage:
    replay_id: UUID
    #: Run the items belong to (``None`` when nothing has been detected yet).
    run_id: UUID | None
    items: list[DetectedEventRow]
    total: int
    limit: int
    offset: int


class DetectedEventService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_events(self, query: DetectedEventQuery) -> DetectedEventPage:
        """Detections of a replay, by default only those of its latest run.

        ``ReplayNotFoundError`` for an unknown replay, ``ReplayPersistenceError`` when
        the database fails.
        """
        repo = DetectedEventRepository(self._session)
        try:
            if not await repo.replay_exists(query.replay_id):
                raise ReplayNotFoundError(query.replay_id)
            run_id = query.run_id or await repo.latest_run_id(query.replay_id)
            if run_id is None:
                return DetectedEventPage(query.replay_id, None, [], 0, query.limit, query.offset)
            scoped = DetectedEventQuery(
                replay_id=query.replay_id,
                run_id=run_id,
                event_types=query.event_types,
                driver=query.driver,
                lap_from=query.lap_from,
                lap_to=query.lap_to,
                limit=query.limit,
                offset=query.offset,
            )
            rows, total = await repo.list(scoped)
        except SQLAlchemyError as exc:
            logger.exception("Detected events read failed replay_id=%s", query.replay_id)
            raise ReplayPersistenceError("Detected event storage unavailable") from exc
        return DetectedEventPage(query.replay_id, run_id, rows, total, query.limit, query.offset)
