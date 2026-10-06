"""PostgreSQL race state snapshots: recovery points and a fallback for reads."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import RaceStateSnapshot
from app.race_state.models import RACE_STATE_SCHEMA_VERSION, RaceState, SnapshotTrigger

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StoredSnapshot:
    run_id: UUID
    sequence: int
    trigger: SnapshotTrigger
    created_at: datetime
    state: RaceState


class SnapshotSink(Protocol):
    """Where the processor persists snapshots (replaceable in tests)."""

    async def save(self, state: RaceState, trigger: SnapshotTrigger) -> None: ...


class RaceStateSnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(
        self, state: RaceState, trigger: SnapshotTrigger, *, created_at: datetime | None = None
    ) -> bool:
        """Insert a snapshot (no commit). ``(replay, run, sequence)`` is unique, so a
        repeated write is silently ignored; returns whether a row was inserted."""
        values = {
            "id": uuid4(),
            "replay_id": state.replay_id,
            "run_id": state.run_id,
            "session_id": state.session_id,
            "sequence": state.last_sequence,
            "race_time_ms": state.current_race_time_ms,
            "current_lap": state.current_lap,
            "trigger": trigger.value,
            "state_schema_version": state.schema_version,
            "payload": state.model_dump(mode="json"),
            "created_at": created_at or datetime.now(UTC),
        }
        dialect = self._session.get_bind().dialect.name
        insert = pg_insert if dialect == "postgresql" else sqlite_insert
        statement = (
            insert(RaceStateSnapshot)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["replay_id", "run_id", "sequence"])
        )
        result = await self._session.execute(statement)
        return bool(result.rowcount)  # type: ignore[attr-defined]

    async def latest(self, replay_id: UUID) -> StoredSnapshot | None:
        """Most recently written snapshot of the replay (readable schema version only)."""
        row = await self._session.scalar(
            select(RaceStateSnapshot)
            .where(
                RaceStateSnapshot.replay_id == replay_id,
                RaceStateSnapshot.state_schema_version == RACE_STATE_SCHEMA_VERSION,
            )
            .order_by(RaceStateSnapshot.created_at.desc(), RaceStateSnapshot.sequence.desc())
            .limit(1)
        )
        if row is None:
            return None
        try:
            state = RaceState.model_validate(row.payload)
        except ValidationError:
            logger.error("Unreadable race state snapshot replay_id=%s id=%s", replay_id, row.id)
            return None
        return StoredSnapshot(
            run_id=row.run_id,
            sequence=row.sequence,
            trigger=SnapshotTrigger(row.trigger),
            created_at=row.created_at,
            state=state,
        )


class DatabaseSnapshotSink:
    """Writes each snapshot in its own short transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(self, state: RaceState, trigger: SnapshotTrigger) -> None:
        async with self._session_factory() as db:
            inserted = await RaceStateSnapshotRepository(db).save(state, trigger)
            await db.commit()
        logger.info(
            "Race state snapshot %s replay_id=%s run_id=%s sequence=%d lap=%s trigger=%s",
            "written" if inserted else "already existed",
            state.replay_id,
            state.run_id,
            state.last_sequence,
            state.current_lap,
            trigger.value,
        )
