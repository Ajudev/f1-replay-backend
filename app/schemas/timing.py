"""Response schemas for replay timing / chart data series.

Library-neutral: one point per completed lap with plain metric values. The frontend
derives chart-specific shapes (e.g. gap between two drivers = difference of their
``gap_to_leader_ms`` on the same lap).
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field

from app.schemas.races import SectorOut


class TimingPoint(BaseModel):
    lap_number: int
    race_time_ms: int = Field(description="Race time at which the driver completed the lap")
    lap_time_ms: int | None
    position: int | None = Field(description="Lap-end classification position")
    gap_to_leader_ms: int | None = Field(
        description="Time behind the first driver to complete this lap number (lap-end basis); "
        "null when unknown"
    )
    compound: str | None
    tyre_age_laps: int | None
    stint_number: int | None
    is_pit_in_lap: bool | None
    is_pit_out_lap: bool | None
    is_deleted: bool | None
    track_status: str | None = Field(description="Track status when the lap was completed")
    sectors: list[SectorOut] = Field(description="Empty when no sector times exist")


class DriverTimingSeries(BaseModel):
    driver_id: UUID
    abbreviation: str
    points: list[TimingPoint] = Field(description="Ordered by lap number")


class ReplayTimingResponse(BaseModel):
    replay_id: UUID
    session_id: UUID
    upto_sequence: int | None = Field(
        description="Only laps released by the replay up to this timeline sequence are included"
    )
    lap_from: int | None
    lap_to: int | None
    drivers: list[DriverTimingSeries] = Field(description="Ordered by abbreviation")


class DriverTimingResponse(BaseModel):
    replay_id: UUID
    session_id: UUID
    upto_sequence: int | None
    lap_from: int | None
    lap_to: int | None
    driver: DriverTimingSeries
