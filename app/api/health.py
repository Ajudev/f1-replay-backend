"""Health and readiness HTTP routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.infrastructure.redis import RedisClient, get_redis
from app.schemas.health import HealthResponse, ReadinessResponse
from app.services import health as health_service

router = APIRouter(tags=["health"])

DbSession = Annotated[AsyncSession, Depends(get_db)]
RedisDep = Annotated[RedisClient, Depends(get_redis)]


@router.get("/health")
async def health() -> HealthResponse:
    """Liveness probe — does not touch Postgres or Redis."""
    return health_service.liveness()


@router.get("/ready")
async def ready(
    response: Response,
    session: DbSession,
    redis: RedisDep,
) -> ReadinessResponse:
    """Readiness probe — checks Postgres and Redis."""

    async def postgres_check() -> health_service.CheckResult:
        return await health_service.check_postgres(session)

    async def redis_check() -> health_service.CheckResult:
        return await health_service.check_redis(redis)

    result, status_code = await health_service.readiness(
        postgres_check=postgres_check,
        redis_check=redis_check,
    )
    response.status_code = status_code
    return result
