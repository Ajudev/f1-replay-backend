"""Schemas for track data: import, circuit layout, telemetry traces and active battles."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from app.detection.battles import ActiveBattle
from app.domain.enums import DrsState
from app.models import CircuitLayout
from app.services.track import LapTelemetry


class TrackDataImportResponse(BaseModel):
    status: Literal["imported", "replaced"]
    session_id: UUID
    driver_count: int = Field(description="Drivers with at least one stored sample")
    position_sample_count: int
    telemetry_sample_count: int
    outline_points: int
    warnings: list[str] = Field(description="Data-quality issues found during import")


class TrackDataStatusResponse(BaseModel):
    session_id: UUID
    status: Literal["NOT_IMPORTED", "IMPORTED"]
    imported_at: datetime | None = None
    sample_interval_ms: int | None = None
    position_sample_count: int = 0
    telemetry_sample_count: int = 0
    warnings: list[str] = Field(default_factory=list)

    @classmethod
    def build(cls, session_id: UUID, layout: CircuitLayout | None) -> TrackDataStatusResponse:
        if layout is None:
            return cls(session_id=session_id, status="NOT_IMPORTED")
        return cls(
            session_id=session_id,
            status="IMPORTED",
            imported_at=layout.updated_at,
            sample_interval_ms=layout.sample_interval_ms,
            position_sample_count=layout.position_sample_count,
            telemetry_sample_count=layout.telemetry_sample_count,
            warnings=layout.warnings,
        )


class Bounds(BaseModel):
    min_x: int
    max_x: int
    min_y: int
    max_y: int


class CornerOut(BaseModel):
    number: int
    letter: str
    x: int
    y: int
    angle_deg: float | None


class CircuitLayoutResponse(BaseModel):
    """Track outline and corners in raw FastF1 units (1/10 m), unrotated."""

    session_id: UUID
    circuit_name: str | None
    rotation_deg: float | None = Field(description="Rotate by this to match official maps")
    bounds: Bounds = Field(description="Extent of the outline and every stored car position")
    outline: list[tuple[int, int]] = Field(description="[x, y] points in driving order")
    corners: list[CornerOut] | None = Field(description="null when unavailable")
    sample_interval_ms: int
    warnings: list[str]

    @classmethod
    def build(cls, layout: CircuitLayout) -> CircuitLayoutResponse:
        return cls(
            session_id=layout.session_id,
            circuit_name=layout.circuit_name,
            rotation_deg=layout.rotation_deg,
            bounds=Bounds(
                min_x=layout.min_x, max_x=layout.max_x, min_y=layout.min_y, max_y=layout.max_y
            ),
            outline=[(x, y) for x, y in layout.outline],
            corners=[CornerOut(**c) for c in layout.corners]
            if layout.corners is not None
            else None,
            sample_interval_ms=layout.sample_interval_ms,
            warnings=layout.warnings,
        )


class TelemetrySampleOut(BaseModel):
    race_time_ms: int
    x: int | None
    y: int | None
    off_track: bool | None
    speed_kph: int | None
    rpm: int | None
    gear: int | None
    throttle_pct: int | None = Field(description="0-100 (raw values outside are kept)")
    brake: bool | None
    drs: DrsState | None


class LapTelemetryResponse(BaseModel):
    replay_id: UUID
    session_id: UUID
    driver_id: UUID
    abbreviation: str
    lap_number: int
    upto_race_time_ms: int = Field(description="Replay clock the trace is limited to")
    complete: bool = Field(description="False while the lap is still being driven")
    samples: list[TelemetrySampleOut]
    warnings: list[str]

    @classmethod
    def build(cls, trace: LapTelemetry) -> LapTelemetryResponse:
        return cls(
            replay_id=trace.replay_id,
            session_id=trace.session_id,
            driver_id=trace.driver_id,
            abbreviation=trace.abbreviation,
            lap_number=trace.lap_number,
            upto_race_time_ms=trace.upto_race_time_ms,
            complete=trace.complete,
            samples=[
                TelemetrySampleOut.model_validate(s, from_attributes=True) for s in trace.samples
            ],
            warnings=trace.warnings,
        )


class ActiveBattleOut(BaseModel):
    attacker_id: UUID
    attacker_abbreviation: str
    defender_id: UUID
    defender_abbreviation: str
    started_lap: int | None
    last_gap_ms: int | None = Field(description="Gap at the attacker's last observed lap end")
    last_observed_lap: int | None

    @classmethod
    def build(cls, battle: ActiveBattle) -> ActiveBattleOut:
        return cls.model_validate(battle, from_attributes=True)


class ActiveBattlesResponse(BaseModel):
    replay_id: UUID
    run_id: UUID | None
    battles: list[ActiveBattleOut]
