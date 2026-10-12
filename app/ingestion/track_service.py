"""Track data import: load FastF1 positions/telemetry → align to race time → replace."""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.adapter import TrackDataLoader
from app.ingestion.errors import TrackDataUnavailableError
from app.ingestion.records import TrackDataImportResult
from app.ingestion.track_normalizer import normalize_track_data
from app.ingestion.track_repository import TrackDataRepository

logger = logging.getLogger(__name__)


class TrackDataImportService:
    """Import a session's car positions, telemetry and circuit layout (idempotent)."""

    def __init__(
        self,
        loader: TrackDataLoader,
        session: AsyncSession,
        *,
        sample_interval_ms: int,
        repository: TrackDataRepository | None = None,
    ) -> None:
        self._loader = loader
        self._session = session
        self._sample_interval_ms = sample_interval_ms
        self._repository = repository or TrackDataRepository()

    async def import_track_data(self, session_id: UUID) -> TrackDataImportResult:
        context = await self._repository.load_context(self._session, session_id)
        logger.info(
            "Track data import started session_id=%s season=%s round=%s session_type=%s",
            session_id,
            context.season,
            context.round,
            context.session_type.value,
        )
        # FastF1 telemetry loading and resampling are slow and CPU-bound: off the loop.
        extracted = await asyncio.to_thread(
            self._loader.load_track_data, context.season, context.round, context.session_type
        )
        normalized = await asyncio.to_thread(
            normalize_track_data,
            extracted,
            drivers=context.drivers,
            epoch_ms=context.epoch_ms,
            sample_interval_ms=self._sample_interval_ms,
        )
        if normalized.position_sample_count == 0:
            raise TrackDataUnavailableError(
                f"No car position data is available for session {session_id}; "
                "the historical source has no telemetry for it"
            )
        replaced = await self._repository.replace(self._session, context, normalized)
        await self._session.commit()
        logger.info(
            "Track data import completed session_id=%s replaced=%s samples=%d positions=%d "
            "telemetry=%d outline_points=%d warnings=%d",
            session_id,
            replaced,
            len(normalized.samples),
            normalized.position_sample_count,
            normalized.telemetry_sample_count,
            len(normalized.outline),
            len(normalized.warnings),
        )
        return TrackDataImportResult(
            status="replaced" if replaced else "imported",
            session_id=session_id,
            driver_count=len({s.driver_abbreviation for s in normalized.samples}),
            position_sample_count=normalized.position_sample_count,
            telemetry_sample_count=normalized.telemetry_sample_count,
            outline_points=len(normalized.outline),
            warnings=normalized.warnings,
        )
