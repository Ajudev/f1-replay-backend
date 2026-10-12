"""PostgreSQL access for track data imports (circuit layout and track samples)."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import delete, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import SessionType
from app.ingestion.errors import PersistenceError
from app.ingestion.records import NormalizedTrackData, TrackDriverInfo, TrackLapWindow
from app.models import (
    CircuitLayout,
    Driver,
    DriverTrackSample,
    Lap,
    Race,
    RaceSession,
    SessionTimeline,
)
from app.services.race_queries import SessionNotFoundError
from app.timeline.errors import TimelineNotGeneratedError

#: Rows per INSERT round trip (a race has ~250k samples).
INSERT_CHUNK = 5_000


@dataclass(frozen=True, slots=True)
class TrackImportContext:
    session_id: UUID
    season: int
    round: int
    session_type: SessionType
    circuit_name: str | None
    #: Race start in session time, as stored with the timeline the replay uses.
    epoch_ms: int
    drivers: list[TrackDriverInfo]


class TrackDataRepository:
    async def load_context(self, db: AsyncSession, session_id: UUID) -> TrackImportContext:
        """Session identity, timeline epoch and stored laps.

        ``SessionNotFoundError`` for an unknown session, ``TimelineNotGeneratedError``
        when the session has no timeline (no epoch to align samples to).
        """
        head = (
            await db.execute(
                select(
                    RaceSession.session_type,
                    Race.season,
                    Race.round,
                    Race.circuit_name,
                    Race.location,
                )
                .join(Race, Race.id == RaceSession.race_id)
                .where(RaceSession.id == session_id)
            )
        ).first()
        if head is None:
            raise SessionNotFoundError(session_id)
        epoch = await db.scalar(
            select(SessionTimeline.race_start_session_time_ms).where(
                SessionTimeline.session_id == session_id
            )
        )
        if epoch is None:
            raise TimelineNotGeneratedError(
                f"Session {session_id} has no timeline; generate it before importing track data"
            )
        session_type, season, round_number, circuit_name, location = head

        rows = (
            await db.execute(
                select(
                    Driver.abbreviation,
                    Lap.lap_number,
                    Lap.lap_start_time_ms,
                    Lap.lap_end_time_ms,
                    Lap.lap_time_ms,
                    Lap.is_deleted,
                    Lap.is_accurate,
                    Lap.is_pit_in_lap,
                    Lap.is_pit_out_lap,
                )
                .join(Lap, Lap.driver_id == Driver.id, isouter=True)
                .where(Driver.session_id == session_id)
                .order_by(Driver.abbreviation, Lap.lap_number)
            )
        ).all()
        laps: dict[str, list[TrackLapWindow]] = {}
        for abbr, number, start, end, lap_time, deleted, accurate, pit_in, pit_out in rows:
            windows = laps.setdefault(abbr, [])
            if number is None:
                continue  # driver without laps
            if end is None and start is not None and lap_time is not None:
                end = start + lap_time
            if end is None:
                continue
            windows.append(
                TrackLapWindow(
                    lap_number=number,
                    end_ms=end,
                    lap_time_ms=lap_time,
                    start_ms=start,
                    usable_for_outline=deleted is not True
                    and accurate is not False
                    and not pit_in
                    and not pit_out,
                )
            )
        return TrackImportContext(
            session_id=session_id,
            season=season,
            round=round_number,
            session_type=session_type,
            circuit_name=circuit_name or location,
            epoch_ms=epoch,
            drivers=[TrackDriverInfo(abbr, windows) for abbr, windows in laps.items()],
        )

    async def replace(
        self, db: AsyncSession, context: TrackImportContext, data: NormalizedTrackData
    ) -> bool:
        """Replace the session's track data in the caller's transaction; True if it
        replaced an earlier import."""
        session_id = context.session_id
        try:
            replaced = (
                await db.execute(
                    delete(CircuitLayout).where(CircuitLayout.session_id == session_id)
                )
            ).rowcount > 0  # type: ignore[attr-defined]
            await db.execute(
                delete(DriverTrackSample).where(DriverTrackSample.session_id == session_id)
            )
            min_x, max_x, min_y, max_y = data.bounds
            db.add(
                CircuitLayout(
                    session_id=session_id,
                    circuit_name=context.circuit_name,
                    outline=[[x, y] for x, y in data.outline],
                    corners=data.corners,
                    rotation_deg=data.rotation_deg,
                    min_x=min_x,
                    max_x=max_x,
                    min_y=min_y,
                    max_y=max_y,
                    sample_interval_ms=data.sample_interval_ms,
                    position_sample_count=data.position_sample_count,
                    telemetry_sample_count=data.telemetry_sample_count,
                    warnings=data.warnings,
                )
            )
            driver_ids = dict(
                (
                    await db.execute(
                        select(Driver.abbreviation, Driver.id).where(
                            Driver.session_id == session_id
                        )
                    )
                ).all()
            )
            for abbr, color in data.team_colors.items():
                if abbr in driver_ids:
                    await db.execute(
                        update(Driver).where(Driver.id == driver_ids[abbr]).values(team_color=color)
                    )
            for start in range(0, len(data.samples), INSERT_CHUNK):
                chunk = data.samples[start : start + INSERT_CHUNK]
                await db.execute(
                    insert(DriverTrackSample),
                    [
                        {
                            "session_id": session_id,
                            "driver_id": driver_ids[s.driver_abbreviation],
                            "race_time_ms": s.race_time_ms,
                            "lap_number": s.lap_number,
                            "x": s.x,
                            "y": s.y,
                            "off_track": s.off_track,
                            "speed_kph": s.speed_kph,
                            "rpm": s.rpm,
                            "gear": s.gear,
                            "throttle_pct": s.throttle_pct,
                            "brake": s.brake,
                            "drs_code": s.drs_code,
                        }
                        for s in chunk
                    ],
                )
            await db.flush()
        except IntegrityError as exc:
            raise PersistenceError(
                "Failed to persist track data due to a data integrity conflict"
            ) from exc
        return replaced

    async def layout(self, db: AsyncSession, session_id: UUID) -> CircuitLayout | None:
        return await db.scalar(select(CircuitLayout).where(CircuitLayout.session_id == session_id))
