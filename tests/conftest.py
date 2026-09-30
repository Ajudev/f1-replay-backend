"""Shared pytest fixtures.

Environment variables are set before application imports so settings load correctly.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator, Iterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Must run before importing app modules that call get_settings().
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://test:test@localhost:5432/test",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/1")
os.environ.setdefault("APP_NAME", "f1-replay-test")
os.environ.setdefault("LOG_LEVEL", "WARNING")

import app.models  # noqa: E402, F401
from app.core.config import get_settings  # noqa: E402
from app.db.base import Base  # noqa: E402
from app.main import create_app  # noqa: E402

get_settings.cache_clear()


@pytest.fixture(autouse=True)
def clear_settings_cache() -> Iterator[None]:
    """Drop cached Settings before/after each test so env overrides do not leak."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def sqlite_engine() -> AsyncGenerator[AsyncEngine, None]:
    """In-memory SQLite engine with foreign keys enabled."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    @event.listens_for(engine.sync_engine, "connect")
    def _enable_foreign_keys(dbapi_connection: object, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def db_session(sqlite_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Yield a fresh session; rollback after each test."""
    session_factory = async_sessionmaker(
        sqlite_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.rollback()


@pytest.fixture
def session_factory(
    sqlite_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(sqlite_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """HTTPX async client against create_app() without starting real infra."""
    application = create_app()
    transport = ASGITransport(app=application)
    async with (
        AsyncClient(transport=transport, base_url="http://test") as ac,
        application.router.lifespan_context(application),
    ):
        yield ac
