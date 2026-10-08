"""Replay timing series: per-driver lap history limited to what a replay has released.

Points come from the persisted timeline's ``LAP_COMPLETED`` events with
``sequence <= replay.current_sequence``, so a client watching a replay never sees laps
from its future, and the series agree with the race state built from the same events.
Sector times are joined from the ``laps``/``sectors`` tables for those laps only.

``gap_to_leader_ms`` uses the race state's definition (the driver's crossing time minus
the earliest crossing of that lap), so a timing point and the state's lap-end gap agree.
Unlike the hot state, which only keeps a bounded window of lap crossings, timing series
are derived from the persisted timeline and are available for every released lap.

Fixed number of queries per request (driver roster, lap events, leader crossings,
sectors); requested drivers are resolved against the roster in memory. Sector rows are
only attached to points of released laps.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import ColumnElement, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import EventType
from app.models import Driver, Lap, RaceEvent, Sector
from app.replay.errors import ReplayPersistenceError
from app.replay.service import ReplayService
from app.schemas.races import SectorOut
from app.schemas.timing import DriverTimingSeries, TimingPoint
from app.services.race_queries import DriverNotFoundError


@dataclass(frozen=True, slots=True)
class TimingResult:
    replay_id: UUID
    session_id: UUID
    upto_sequence: int | None
    series: list[DriverTimingSeries]


class ReplayTimingService:
    def __init__(
        self, replay_service: ReplayService, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        self._replays = replay_service
        self._session_factory = session_factory

    async def series(
        self,
        replay_id: UUID,
        *,
        drivers: list[str] | None = None,
        lap_from: int | None = None,
        lap_to: int | None = None,
    ) -> TimingResult:
        """Series for ``drivers`` (abbreviation or UUID; all when empty).

        ``ReplayNotFoundError`` / ``DriverNotFoundError`` for unknown ids,
        ``ReplayPersistenceError`` when the database fails.
        """
        view = await self._replays.get(replay_id)
        session_id = view.state.session_id
        upto = view.state.current_sequence
        try:
            async with self._session_factory() as db:
                series = await _TimingQueries(db, session_id).series(
                    upto, drivers or [], lap_from, lap_to
                )
        except SQLAlchemyError as exc:
            raise ReplayPersistenceError("Timing storage unavailable") from exc
        return TimingResult(replay_id, session_id, upto, series)


def _resolve(session_id: UUID, roster: Any, driver: str) -> UUID:
    """Driver id for an abbreviation (case-insensitive) or UUID; 404 semantics if unknown."""
    wanted = driver.strip()
    try:
        by_id = UUID(wanted)
    except ValueError:
        by_id = None
    for driver_id, abbreviation in roster:
        if driver_id == by_id or (by_id is None and abbreviation == wanted.upper()):
            return driver_id
    raise DriverNotFoundError(session_id, driver)


class _TimingQueries:
    def __init__(self, db: AsyncSession, session_id: UUID) -> None:
        self._db = db
        self._session_id = session_id

    async def series(
        self, upto: int | None, wanted: list[str], lap_from: int | None, lap_to: int | None
    ) -> list[DriverTimingSeries]:
        # One query for the session's drivers; requested ones are resolved in memory.
        roster = (
            await self._db.execute(
                select(Driver.id, Driver.abbreviation)
                .where(Driver.session_id == self._session_id)
                .order_by(Driver.abbreviation)
            )
        ).all()
        driver_ids = [_resolve(self._session_id, roster, d) for d in wanted]
        drivers = [row for row in roster if not driver_ids or row[0] in driver_ids]
        if upto is None:
            return [DriverTimingSeries(driver_id=i, abbreviation=a, points=[]) for i, a in drivers]

        laps = self._lap_conditions(upto, lap_from, lap_to)
        events = await self._db.scalars(
            select(RaceEvent)
            .where(*laps, *([RaceEvent.driver_id.in_(driver_ids)] if driver_ids else []))
            .order_by(RaceEvent.lap_number, RaceEvent.sequence)
        )
        leader_rows = await self._db.execute(
            select(RaceEvent.lap_number, func.min(RaceEvent.race_time_ms))
            .where(*laps)
            .group_by(RaceEvent.lap_number)
        )
        leader_ms: dict[int, int] = {lap: ms for lap, ms in leader_rows.all() if ms is not None}
        sectors = await self._sectors(driver_ids, lap_from, lap_to)

        points: dict[UUID, list[TimingPoint]] = defaultdict(list)
        for event in events:
            if event.driver_id is None or event.lap_number is None:
                continue
            p: dict[str, Any] = event.payload or {}
            race_ms = event.race_time_ms or 0
            leader = leader_ms.get(event.lap_number)
            points[event.driver_id].append(
                TimingPoint(
                    lap_number=event.lap_number,
                    race_time_ms=race_ms,
                    lap_time_ms=p.get("lap_time_ms"),
                    position=p.get("position"),
                    gap_to_leader_ms=race_ms - leader if leader is not None else None,
                    compound=p.get("compound"),
                    tyre_age_laps=p.get("tyre_age_laps"),
                    stint_number=p.get("stint_number"),
                    is_pit_in_lap=p.get("is_pit_in_lap"),
                    is_pit_out_lap=p.get("is_pit_out_lap"),
                    is_deleted=p.get("is_deleted"),
                    track_status=p.get("track_status"),
                    sectors=sectors.get((event.driver_id, event.lap_number), []),
                )
            )
        return [
            DriverTimingSeries(driver_id=i, abbreviation=a, points=points.get(i, []))
            for i, a in drivers
        ]

    def _lap_conditions(
        self, upto: int, lap_from: int | None, lap_to: int | None
    ) -> list[ColumnElement[bool]]:
        conditions: list[ColumnElement[bool]] = [
            RaceEvent.session_id == self._session_id,
            RaceEvent.event_type == EventType.LAP_COMPLETED,
            RaceEvent.sequence <= upto,
        ]
        if lap_from is not None:
            conditions.append(RaceEvent.lap_number >= lap_from)
        if lap_to is not None:
            conditions.append(RaceEvent.lap_number <= lap_to)
        return conditions

    async def _sectors(
        self, driver_ids: list[UUID], lap_from: int | None, lap_to: int | None
    ) -> dict[tuple[UUID, int], list[SectorOut]]:
        conditions: list[ColumnElement[bool]] = [Lap.session_id == self._session_id]
        if driver_ids:
            conditions.append(Lap.driver_id.in_(driver_ids))
        if lap_from is not None:
            conditions.append(Lap.lap_number >= lap_from)
        if lap_to is not None:
            conditions.append(Lap.lap_number <= lap_to)
        rows = await self._db.execute(
            select(Lap.driver_id, Lap.lap_number, Sector.sector_number, Sector.sector_time_ms)
            .join(Sector, Sector.lap_id == Lap.id)
            .where(*conditions)
            .order_by(Lap.driver_id, Lap.lap_number, Sector.sector_number)
        )
        result: dict[tuple[UUID, int], list[SectorOut]] = defaultdict(list)
        for driver_id, lap_number, number, time_ms in rows.all():
            result[(driver_id, lap_number)].append(
                SectorOut(sector_number=number, sector_time_ms=time_ms)
            )
        return result
