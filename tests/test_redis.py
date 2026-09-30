"""Redis client wrapper tests."""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from app.infrastructure.redis import RedisClient
from app.main import create_app


@pytest.fixture
async def fake_redis_client() -> AsyncGenerator[RedisClient, None]:
    server = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = RedisClient.from_client(server)
    yield client
    await client.aclose()


async def test_ping_returns_true(fake_redis_client: RedisClient) -> None:
    assert await fake_redis_client.ping() is True


async def test_close_does_not_raise_and_subsequent_ping_is_false(
    fake_redis_client: RedisClient,
) -> None:
    await fake_redis_client.close()
    await fake_redis_client.close()  # idempotent
    assert fake_redis_client.is_closed is True
    assert await fake_redis_client.ping() is False


async def test_lifespan_closes_redis_client() -> None:
    """Lifespan should close the redis client attached to app.state."""
    application = create_app()
    closed = {"value": False}

    fake = MagicMock(spec=RedisClient)
    fake.aclose = AsyncMock(side_effect=lambda: closed.__setitem__("value", True))
    fake.ping = AsyncMock(return_value=True)

    transport = ASGITransport(app=application)
    async with (
        AsyncClient(transport=transport, base_url="http://test"),
        application.router.lifespan_context(application),
    ):
        # Replace the real client created during startup.
        await application.state.redis.aclose()
        application.state.redis = fake
    # After lifespan exits, aclose should have been called on the current client.
    assert closed["value"] is True
    fake.aclose.assert_awaited()
