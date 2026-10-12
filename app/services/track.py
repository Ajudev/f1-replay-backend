"""Circuit layout and per-lap telemetry reads (PostgreSQL only, never FastF1).

Telemetry traces never reveal the replay's future: only samples with
``race_time_ms <= replay.current_race_time_ms`` are returned, so a lap in progress is
truncated at the replay clock and a lap that has not started yet is refused.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import DrsState
from app.models import CircuitLayout, Driver, DriverTrackSample, RaceSession
from app.replay.errors import ReplayPersistenceError
from app.replay.service import ReplayService
from app.services.race_queries import DriverNotFoundError, SessionNotFoundError, driver_condition


class TrackDataNotImportedError(Exception):
    def __init__(self, session_id: UUID) -> None:
        self.message = (
            f"Track data has not been imported for session {session_id}; "
            f"call POST /api/v1/sessions/{session_id}/track-data first"
        )
        super().__init__(self.message)


class LapNotReachedError(Exception):
    def __init__(self, driver: str, lap_number: int) -> None:
        self.message = f"The replay has not reached lap {lap_number} of {driver} yet"
        super().__init__(self.message)


class CircuitService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def layout(self, session_id: UUID) -> CircuitLayout:
        """``SessionNotFoundError`` / ``TrackDataNotImportedError`` when missing."""
        layout = await self.find_layout(session_id)
        if layout is None:
            raise TrackDataNotImportedError(session_id)
        return layout

    async def find_layout(self, session_id: UUID) -> CircuitLayout | None:
        if await self._session.get(RaceSession, session_id) is None:
            raise SessionNotFoundError(session_id)
        return await self._session.scalar(
            select(CircuitLayout).where(CircuitLayout.session_id == session_id)
        )


@dataclass(frozen=True, slots=True)
class TelemetryPoint:
    race_time_ms: int
    x: int | None
    y: int | None
    off_track: bool | None
    speed_kph: int | None
    rpm: int | None
    gear: int | None
    throttle_pct: int | None
    brake: bool | None
    drs: DrsState | None


@dataclass(frozen=True, slots=True)
class LapTelemetry:
    replay_id: UUID
    session_id: UUID
    driver_id: UUID
    abbreviation: str
    lap_number: int
    upto_race_time_ms: int
    complete: bool
    samples: list[TelemetryPoint]
    warnings: list[str]


class TelemetryService:
    def __init__(
        self, replay_service: ReplayService, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        self._replays = replay_service
        self._session_factory = session_factory

    async def lap_trace(self, replay_id: UUID, driver: str, lap_number: int) -> LapTelemetry:
        view = await self._replays.get(replay_id)
        session_id = view.state.session_id
        now = view.state.current_race_time_ms
        try:
            async with self._session_factory() as db:
                found = (
                    await db.execute(
                        select(Driver.id, Driver.abbreviation).where(
                            Driver.session_id == session_id, driver_condition(driver)
                        )
                    )
                ).first()
                if found is None:
                    raise DriverNotFoundError(session_id, driver)
                driver_id, abbreviation = found
                if await CircuitService(db).find_layout(session_id) is None:
                    raise TrackDataNotImportedError(session_id)
                rows = (
                    await db.execute(
                        select(
                            DriverTrackSample.race_time_ms,
                            DriverTrackSample.x,
                            DriverTrackSample.y,
                            DriverTrackSample.off_track,
                            DriverTrackSample.speed_kph,
                            DriverTrackSample.rpm,
                            DriverTrackSample.gear,
                            DriverTrackSample.throttle_pct,
                            DriverTrackSample.brake,
                            DriverTrackSample.drs_code,
                        )
                        .where(
                            DriverTrackSample.session_id == session_id,
                            DriverTrackSample.driver_id == driver_id,
                            DriverTrackSample.lap_number == lap_number,
                        )
                        .order_by(DriverTrackSample.race_time_ms)
                    )
                ).all()
        except SQLAlchemyError as exc:
            raise ReplayPersistenceError("Track data storage unavailable") from exc

        warnings: list[str] = []
        if not rows:
            warnings.append(f"No telemetry samples are stored for lap {lap_number}")
        visible = [row for row in rows if row.race_time_ms <= now]
        if rows and not visible:
            raise LapNotReachedError(abbreviation, lap_number)
        return LapTelemetry(
            replay_id=replay_id,
            session_id=session_id,
            driver_id=driver_id,
            abbreviation=abbreviation,
            lap_number=lap_number,
            upto_race_time_ms=now,
            complete=bool(rows) and len(visible) == len(rows),
            samples=[
                TelemetryPoint(*row[:-1], drs=DrsState.from_code(row.drs_code)) for row in visible
            ],
            warnings=warnings,
        )
