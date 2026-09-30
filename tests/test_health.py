"""Health and readiness endpoint tests."""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.infrastructure.redis import RedisClient, get_redis
from app.main import create_app


@pytest.fixture
async def health_client() -> AsyncGenerator[AsyncClient, None]:
    """Client without relying on real lifespan DB/Redis for overrides."""
    application = create_app()
    transport = ASGITransport(app=application)
    async with (
        AsyncClient(transport=transport, base_url="http://test") as ac,
        application.router.lifespan_context(application),
    ):
        yield ac


async def test_health_returns_ok_without_external_services(
    health_client: AsyncClient,
) -> None:
    response = await health_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_returns_200_when_dependencies_succeed(
    health_client: AsyncClient,
) -> None:
    app = health_client._transport.app  # type: ignore[attr-defined]

    async def fake_db() -> AsyncGenerator[AsyncSession, None]:
        session = AsyncMock(spec=AsyncSession)
        session.execute = AsyncMock(return_value=None)
        yield session

    redis = AsyncMock(spec=RedisClient)
    redis.ping = AsyncMock(return_value=True)

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_redis] = lambda: redis
    try:
        response = await health_client.get("/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {"postgres": "ok", "redis": "ok"},
    }


async def test_ready_returns_503_when_postgres_fails(
    health_client: AsyncClient,
) -> None:
    app = health_client._transport.app  # type: ignore[attr-defined]

    async def fake_db() -> AsyncGenerator[AsyncSession, None]:
        session = AsyncMock(spec=AsyncSession)
        session.execute = AsyncMock(side_effect=RuntimeError("db down"))
        yield session

    redis = AsyncMock(spec=RedisClient)
    redis.ping = AsyncMock(return_value=True)

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_redis] = lambda: redis
    try:
        response = await health_client.get("/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["postgres"] == "error"
    assert body["checks"]["redis"] == "ok"
    assert "db down" not in response.text


async def test_ready_returns_503_when_redis_fails(
    health_client: AsyncClient,
) -> None:
    app = health_client._transport.app  # type: ignore[attr-defined]

    async def fake_db() -> AsyncGenerator[AsyncSession, None]:
        session = AsyncMock(spec=AsyncSession)
        session.execute = AsyncMock(return_value=None)
        yield session

    redis = AsyncMock(spec=RedisClient)
    redis.ping = AsyncMock(return_value=False)

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_redis] = lambda: redis
    try:
        response = await health_client.get("/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["postgres"] == "ok"
    assert body["checks"]["redis"] == "error"
