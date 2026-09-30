"""Health and readiness response schemas."""

from typing import Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Liveness probe response."""

    status: Literal["ok"] = "ok"


class ReadinessChecks(BaseModel):
    """Individual dependency check results."""

    postgres: Literal["ok", "error"]
    redis: Literal["ok", "error"]


class ReadinessResponse(BaseModel):
    """Readiness probe response."""

    status: Literal["ready", "not_ready"]
    checks: ReadinessChecks = Field(description="Dependency check results")
