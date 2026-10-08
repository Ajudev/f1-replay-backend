"""HTTP error contract: every error response is ``{"code", "message", "details"}``.

``code`` is a stable, machine-readable identifier the frontend can branch on;
``message`` is client-safe text; ``details`` is an optional object (``null`` when
there is nothing to add). Domain exceptions are mapped by type in ``ERROR_MAP``
(first match in MRO order wins); FastAPI validation errors, ``HTTPException`` and
unexpected exceptions are normalized to the same shape. Stack traces and database
errors are never returned.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.detection.errors import DetectedEventNotFoundError
from app.ingestion.errors import (
    EventNotFoundError,
    NormalizationError,
    PersistenceError,
    SessionLoadError,
    SessionNotFoundError,
)
from app.race_state.errors import (
    DriverNotInStateError,
    RaceStateStoreUnavailableError,
    RaceStateUnavailableError,
    ReplayNotStartedError,
)
from app.replay.errors import (
    InvalidPlaybackSpeedError,
    InvalidReplayTransitionError,
    ReplayNotFoundError,
    ReplayPersistenceError,
    ReplayTimelineUnavailableError,
)
from app.services.race_queries import (
    DriverNotFoundError,
    RaceNotFoundError,
    RaceSessionTypeNotFoundError,
)
from app.services.race_queries import SessionNotFoundError as QuerySessionNotFoundError
from app.timeline.errors import (
    TimelineBuildError,
    TimelineConflictError,
    TimelineNotGeneratedError,
    TimelineValidationError,
    UnsupportedSessionTypeError,
)

logger = logging.getLogger(__name__)


class ErrorResponse(BaseModel):
    """Body of every non-2xx response."""

    code: str = Field(
        description="Stable machine-readable error code", examples=["REPLAY_NOT_FOUND"]
    )
    message: str = Field(description="Human-readable, client-safe description")
    details: dict[str, Any] | None = Field(default=None, description="Optional structured context")


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    status: int
    code: str
    details: Callable[[Any], dict[str, Any] | None] | None = None


ERROR_MAP: dict[type[Exception], ErrorSpec] = {
    # races / sessions (PostgreSQL queries)
    RaceNotFoundError: ErrorSpec(404, "RACE_NOT_FOUND"),
    QuerySessionNotFoundError: ErrorSpec(404, "SESSION_NOT_FOUND"),
    RaceSessionTypeNotFoundError: ErrorSpec(404, "SESSION_NOT_FOUND"),
    DriverNotFoundError: ErrorSpec(404, "DRIVER_NOT_FOUND"),
    # import (FastF1 ingestion)
    EventNotFoundError: ErrorSpec(404, "HISTORICAL_EVENT_NOT_FOUND"),
    SessionNotFoundError: ErrorSpec(404, "HISTORICAL_SESSION_NOT_FOUND"),
    NormalizationError: ErrorSpec(422, "HISTORICAL_DATA_INVALID"),
    SessionLoadError: ErrorSpec(502, "HISTORICAL_DATA_UNAVAILABLE"),
    PersistenceError: ErrorSpec(500, "IMPORT_PERSISTENCE_FAILED"),
    # timeline
    TimelineNotGeneratedError: ErrorSpec(404, "TIMELINE_NOT_GENERATED"),
    TimelineConflictError: ErrorSpec(409, "TIMELINE_CONFLICT"),
    UnsupportedSessionTypeError: ErrorSpec(422, "UNSUPPORTED_SESSION_TYPE"),
    TimelineBuildError: ErrorSpec(422, "TIMELINE_BUILD_FAILED"),
    TimelineValidationError: ErrorSpec(
        422, "TIMELINE_INVALID", lambda exc: {"problems": exc.problems}
    ),
    # replays
    ReplayNotFoundError: ErrorSpec(404, "REPLAY_NOT_FOUND"),
    InvalidReplayTransitionError: ErrorSpec(
        409,
        "INVALID_REPLAY_TRANSITION",
        lambda exc: {"command": exc.command, "current_status": exc.current_status.value},
    ),
    InvalidPlaybackSpeedError: ErrorSpec(422, "INVALID_PLAYBACK_SPEED"),
    ReplayTimelineUnavailableError: ErrorSpec(409, "REPLAY_TIMELINE_UNAVAILABLE"),
    ReplayPersistenceError: ErrorSpec(503, "DATABASE_UNAVAILABLE"),
    # race state
    ReplayNotStartedError: ErrorSpec(409, "REPLAY_NOT_STARTED"),
    RaceStateUnavailableError: ErrorSpec(404, "RACE_STATE_UNAVAILABLE"),
    DriverNotInStateError: ErrorSpec(404, "DRIVER_NOT_FOUND"),
    RaceStateStoreUnavailableError: ErrorSpec(503, "RACE_STATE_STORE_UNAVAILABLE"),
    # detected events
    DetectedEventNotFoundError: ErrorSpec(404, "DETECTED_EVENT_NOT_FOUND"),
}

#: Infrastructure failures that escaped a service without being mapped.
_DEPENDENCY_ERRORS: tuple[tuple[type[Exception], str, str], ...] = (
    (SQLAlchemyError, "DATABASE_UNAVAILABLE", "Database unavailable"),
    (RedisError, "REDIS_UNAVAILABLE", "Redis unavailable"),
)


def error_body(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return ErrorResponse(code=code, message=message, details=details).model_dump(mode="json")


def resolve_error(exc: Exception) -> tuple[int, dict[str, Any]] | None:
    """Status and body for a mapped domain exception (``None`` if unmapped).

    Shared with the WebSocket gateway so both transports report the same codes.
    """
    for cls in type(exc).__mro__:
        spec = ERROR_MAP.get(cls)  # type: ignore[arg-type]
        if spec is not None:
            message = getattr(exc, "message", None) or str(exc)
            details = spec.details(exc) if spec.details else None
            return spec.status, error_body(spec.code, message, details)
    return None


def error_responses(*statuses: int) -> dict[int | str, dict[str, Any]]:
    """OpenAPI ``responses=`` fragment documenting the error body for ``statuses``."""
    return {status: {"model": ErrorResponse} for status in statuses}


def register_exception_handlers(app: FastAPI) -> None:
    """Attach domain exception → HTTP mappings and normalize framework errors."""

    async def domain_handler(_request: Request, exc: Exception) -> JSONResponse:
        resolved = resolve_error(exc)
        assert resolved is not None
        status, body = resolved
        return JSONResponse(status_code=status, content=body)

    for exc_type in ERROR_MAP:
        app.add_exception_handler(exc_type, domain_handler)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"loc": list(err.get("loc", ())), "msg": err.get("msg"), "type": err.get("type")}
            for err in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content=error_body("VALIDATION_ERROR", "Request validation failed", {"errors": errors}),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(exc.status_code, "HTTP_ERROR")
        return JSONResponse(
            status_code=exc.status_code,
            content=error_body(code, str(exc.detail)),
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(Exception)
    async def unexpected_handler(request: Request, exc: Exception) -> JSONResponse:
        for exc_type, code, message in _DEPENDENCY_ERRORS:
            if isinstance(exc, exc_type):
                logger.error("Dependency failure path=%s", request.url.path, exc_info=exc)
                return JSONResponse(status_code=503, content=error_body(code, message))
        logger.exception("Unhandled error path=%s", request.url.path)
        return JSONResponse(
            status_code=500, content=error_body("INTERNAL_ERROR", "Internal server error")
        )
