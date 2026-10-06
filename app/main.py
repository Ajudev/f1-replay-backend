"""FastAPI application factory and lifespan."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError

from app.api.exceptions import register_exception_handlers
from app.api.health import router as health_router
from app.api.race_state import router as race_state_router
from app.api.races import router as races_router
from app.api.replays import router as replays_router
from app.api.timeline import router as timeline_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import create_engine, create_session_factory
from app.infrastructure.redis import RedisClient
from app.race_state.config import RaceStateConfig
from app.race_state.repository import RaceStateStore
from app.race_state.service import RaceStateService
from app.replay.clock import AsyncioReplayTimer
from app.replay.errors import ReplayPersistenceError
from app.replay.service import ReplayService
from app.streaming.config import StreamConfig
from app.streaming.publisher import RedisStreamPublisher

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application startup and shutdown lifecycle."""
    settings: Settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    redis = RedisClient(settings.redis_url)
    # Replay events go to Redis Streams over the app's shared connection pool. The
    # client connects lazily, so startup succeeds with Redis down; a publish failure
    # fails the replay explicitly. Consumers run as separate processes (CLI).
    stream_config = StreamConfig.from_settings(settings)
    replay_service = ReplayService(
        session_factory,
        sink=RedisStreamPublisher(redis, stream_config),
        timer=AsyncioReplayTimer(),
    )
    # Read side only: the race state processor runs as its own worker process.
    race_state_service = RaceStateService(
        RaceStateStore(
            redis, stream_config=stream_config, config=RaceStateConfig.from_settings(settings)
        ),
        session_factory,
        replay_service,
    )

    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.redis = redis
    app.state.replay_service = replay_service
    app.state.race_state_service = race_state_service

    logger.info(
        "Application starting name=%s env=%s",
        settings.app_name,
        settings.app_env,
    )
    await _recover_interrupted_replays(replay_service)
    try:
        yield
    finally:
        await app.state.replay_service.shutdown()
        # Close whatever client is currently on app.state (tests may replace it).
        await app.state.redis.aclose()
        await app.state.engine.dispose()
        logger.info(
            "Application shutdown name=%s env=%s",
            settings.app_name,
            settings.app_env,
        )


async def _recover_interrupted_replays(service: ReplayService) -> None:
    """Replays cannot survive a restart; mark persisted active ones STOPPED.

    Best effort: if the database is unreachable at startup, the service still
    repairs each such replay the next time it is read or commanded.
    """
    try:
        async with asyncio.timeout(5):
            await service.recover_interrupted()
    except (ReplayPersistenceError, SQLAlchemyError, OSError, TimeoutError):
        logger.warning("Interrupted replay recovery skipped: database unavailable", exc_info=True)


def create_app() -> FastAPI:
    """Build and return the FastAPI application."""
    application = FastAPI(
        title="F1 Replay API",
        version="0.1.0",
        lifespan=lifespan,
    )
    register_exception_handlers(application)
    application.include_router(health_router)
    application.include_router(races_router)
    application.include_router(timeline_router)
    application.include_router(replays_router)
    application.include_router(race_state_router)
    return application


app = create_app()
