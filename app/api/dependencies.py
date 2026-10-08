"""Shared FastAPI dependencies: application services and common query parameters.

Services are application-scoped singletons on ``app.state`` (built in the lifespan);
tests replace them through ``app.dependency_overrides``. This is also the single
place where authentication would be added for REST and WebSocket routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Query, Request
from fastapi.exceptions import RequestValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.race_state.service import RaceStateService
from app.replay.service import ReplayService
from app.services.timing import ReplayTimingService

DbSession = Annotated[AsyncSession, Depends(get_db)]

#: Driver abbreviation (``NOR``, case-insensitive) or driver UUID.
DRIVER_PATTERN = r"^[A-Za-z0-9-]{1,36}$"
DriverParam = Annotated[
    str | None,
    Query(pattern=DRIVER_PATTERN, description="Driver abbreviation (case-insensitive) or UUID"),
]
LimitParam = Annotated[int, Query(ge=1, le=1000, description="Page size")]
OffsetParam = Annotated[int, Query(ge=0, description="Items to skip")]


def get_replay_service(request: Request) -> ReplayService:
    return request.app.state.replay_service


def get_race_state_service(request: Request) -> RaceStateService:
    return request.app.state.race_state_service


def get_timing_service(request: Request) -> ReplayTimingService:
    return request.app.state.timing_service


ReplayServiceDep = Annotated[ReplayService, Depends(get_replay_service)]
RaceStateServiceDep = Annotated[RaceStateService, Depends(get_race_state_service)]
TimingServiceDep = Annotated[ReplayTimingService, Depends(get_timing_service)]


class LapRange:
    """``lap_from`` / ``lap_to`` query parameters (inclusive; ``lap_from <= lap_to``)."""

    def __init__(
        self,
        lap_from: Annotated[int | None, Query(ge=1, description="First lap (inclusive)")] = None,
        lap_to: Annotated[int | None, Query(ge=1, description="Last lap (inclusive)")] = None,
    ) -> None:
        if lap_from is not None and lap_to is not None and lap_from > lap_to:
            raise RequestValidationError(
                [
                    {
                        "loc": ("query", "lap_to"),
                        "msg": "lap_to must be greater than or equal to lap_from",
                        "type": "value_error",
                    }
                ]
            )
        self.lap_from = lap_from
        self.lap_to = lap_to


LapRangeDep = Annotated[LapRange, Depends()]
