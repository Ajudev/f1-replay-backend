"""FastAPI application factory and lifespan."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.health import router as health_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import create_engine, create_session_factory
from app.infrastructure.redis import RedisClient

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application startup and shutdown lifecycle."""
    settings: Settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    redis = RedisClient(settings.redis_url)

    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.redis = redis

    logger.info(
        "Application starting name=%s env=%s",
        settings.app_name,
        settings.app_env,
    )
    try:
        yield
    finally:
        # Close whatever client is currently on app.state (tests may replace it).
        await app.state.redis.aclose()
        await app.state.engine.dispose()
        logger.info(
            "Application shutdown name=%s env=%s",
            settings.app_name,
            settings.app_env,
        )


def create_app() -> FastAPI:
    """Build and return the FastAPI application."""
    application = FastAPI(
        title="F1 Replay API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(health_router)
    return application


app = create_app()
