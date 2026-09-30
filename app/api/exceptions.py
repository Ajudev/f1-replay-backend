"""HTTP exception handlers for ingestion and query errors."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.ingestion.errors import (
    EventNotFoundError,
    NormalizationError,
    PersistenceError,
    SessionLoadError,
    SessionNotFoundError,
)
from app.services.race_queries import RaceNotFoundError
from app.services.race_queries import SessionNotFoundError as QuerySessionNotFoundError


def register_exception_handlers(app: FastAPI) -> None:
    """Attach domain exception → HTTP status mappings."""

    @app.exception_handler(EventNotFoundError)
    async def event_not_found_handler(
        _request: Request,
        exc: EventNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": exc.message})

    @app.exception_handler(SessionNotFoundError)
    async def session_not_found_handler(
        _request: Request,
        exc: SessionNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": exc.message})

    @app.exception_handler(QuerySessionNotFoundError)
    async def query_session_not_found_handler(
        _request: Request,
        exc: QuerySessionNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": exc.message})

    @app.exception_handler(RaceNotFoundError)
    async def race_not_found_handler(
        _request: Request,
        exc: RaceNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": exc.message})

    @app.exception_handler(NormalizationError)
    async def normalization_error_handler(
        _request: Request,
        exc: NormalizationError,
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": exc.message})

    @app.exception_handler(SessionLoadError)
    async def session_load_error_handler(
        _request: Request,
        exc: SessionLoadError,
    ) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": exc.message})

    @app.exception_handler(PersistenceError)
    async def persistence_error_handler(
        _request: Request,
        exc: PersistenceError,
    ) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": exc.message})
