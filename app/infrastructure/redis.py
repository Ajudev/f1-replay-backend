"""Async Redis client wrapper and FastAPI dependency."""

from typing import Protocol

from fastapi import Request
from redis.asyncio import Redis


class RedisClientProtocol(Protocol):
    """Minimal protocol for Redis health checks and lifecycle."""

    async def ping(self) -> bool: ...

    async def aclose(self) -> None: ...


class RedisClient:
    """Thin wrapper around ``redis.asyncio.Redis``."""

    def __init__(self, url: str) -> None:
        self._client: Redis | None = Redis.from_url(url, decode_responses=True)
        self._closed = False

    @classmethod
    def from_client(cls, client: Redis) -> "RedisClient":
        """Wrap an existing redis client (useful for tests with fakeredis)."""
        instance = cls.__new__(cls)
        instance._client = client
        instance._closed = False
        return instance

    async def ping(self) -> bool:
        """Return True if Redis responds to PING."""
        if self._client is None or self._closed:
            return False
        try:
            result = await self._client.ping()
            return bool(result)
        except Exception:
            return False

    async def close(self) -> None:
        """Close the underlying client. Safe to call multiple times."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying client. Safe to call multiple times."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def is_closed(self) -> bool:
        """Whether the client has been closed."""
        return self._closed


def get_redis(request: Request) -> RedisClient:
    """Return the application-scoped Redis client from ``app.state``."""
    client: RedisClient = request.app.state.redis
    return client
