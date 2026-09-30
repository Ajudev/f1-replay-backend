"""Health and readiness check service."""

import logging
from collections.abc import Awaitable, Callable
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.redis import RedisClient
from app.schemas.health import HealthResponse, ReadinessChecks, ReadinessResponse

logger = logging.getLogger(__name__)

CheckResult = Literal["ok", "error"]


async def check_postgres(session: AsyncSession) -> CheckResult:
    """Run ``SELECT 1`` and return ok/error without leaking connection details."""
    try:
        await session.execute(text("SELECT 1"))
        return "ok"
    except Exception:
        logger.warning("Postgres readiness check failed", exc_info=False)
        return "error"


async def check_redis(redis: RedisClient) -> CheckResult:
    """Ping Redis and return ok/error without leaking connection details."""
    try:
        if await redis.ping():
            return "ok"
        return "error"
    except Exception:
        logger.warning("Redis readiness check failed", exc_info=False)
        return "error"


def liveness() -> HealthResponse:
    """Return a liveness response (no external dependencies)."""
    return HealthResponse(status="ok")


async def readiness(
    *,
    postgres_check: Callable[[], Awaitable[CheckResult]],
    redis_check: Callable[[], Awaitable[CheckResult]],
) -> tuple[ReadinessResponse, int]:
    """Evaluate readiness and return response plus HTTP status code."""
    postgres_status = await postgres_check()
    redis_status = await redis_check()
    checks = ReadinessChecks(postgres=postgres_status, redis=redis_status)
    if postgres_status == "ok" and redis_status == "ok":
        return ReadinessResponse(status="ready", checks=checks), 200
    return ReadinessResponse(status="not_ready", checks=checks), 503
