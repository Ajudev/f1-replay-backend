"""Import orchestration: load → normalize → persist → commit."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import SessionType
from app.ingestion.adapter import SessionLoader
from app.ingestion.normalizer import normalize
from app.ingestion.records import ExistingSessionInfo, ImportResult, PersistedImport
from app.ingestion.repository import ImportRepository

logger = logging.getLogger(__name__)


class ImportService:
    """Coordinate FastF1 load, normalization, and database persistence."""

    def __init__(
        self,
        loader: SessionLoader,
        session: AsyncSession,
        repository: ImportRepository | None = None,
    ) -> None:
        self._loader = loader
        self._session = session
        self._repository = repository or ImportRepository()

    async def import_session(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
        *,
        replace: bool = False,
    ) -> ImportResult:
        logger.info(
            "Import started season=%s event=%s session_type=%s replace=%s",
            season,
            event,
            session_type.value,
            replace,
        )
        logger.info(
            "Session selected season=%s event=%s session_type=%s",
            season,
            event,
            session_type.value,
        )

        if not replace and isinstance(event, int):
            existing = await self._repository.find_existing_by_round(
                self._session,
                season,
                event,
                session_type,
            )
            if existing is not None:
                logger.info(
                    "Already imported (skipped load) race_id=%s session_id=%s",
                    existing.race_id,
                    existing.session_id,
                )
                return self._already_imported_result(existing)

        try:
            extracted = await asyncio.to_thread(
                self._loader.load,
                season,
                event,
                session_type,
            )
        except Exception:
            logger.exception(
                "Import load failed season=%s event=%s session_type=%s",
                season,
                event,
                session_type.value,
            )
            raise

        logger.info(
            "Load completed season=%s round=%s event_name=%s drivers=%s laps=%s",
            extracted.season,
            extracted.round,
            extracted.event_name,
            len(extracted.drivers),
            len(extracted.laps),
        )

        try:
            normalized = normalize(extracted)
        except Exception:
            logger.exception(
                "Normalization failed season=%s event=%s session_type=%s",
                season,
                event,
                session_type.value,
            )
            raise

        logger.info(
            "Normalization completed drivers=%s laps=%s sectors=%s stints=%s "
            "track_status=%s skipped=%s warnings=%s",
            len(normalized.drivers),
            len(normalized.laps),
            sum(len(lap.sectors) for lap in normalized.laps),
            len(normalized.stints),
            len(normalized.track_statuses),
            normalized.skipped_count,
            len(normalized.warnings),
        )

        try:
            persisted = await self._repository.persist(
                self._session,
                normalized,
                replace=replace,
            )
        except Exception:
            logger.exception(
                "Persistence failed season=%s event=%s session_type=%s",
                season,
                event,
                session_type.value,
            )
            raise

        if persisted.already_present:
            assert persisted.race_id is not None
            assert persisted.session_id is not None
            assert persisted.season is not None
            assert persisted.round is not None
            assert persisted.event_name is not None
            assert persisted.session_type is not None
            logger.info(
                "Already imported race_id=%s session_id=%s",
                persisted.race_id,
                persisted.session_id,
            )
            return self._already_imported_from_persisted(persisted)

        await self._session.commit()

        status = "replaced" if persisted.replaced else "imported"
        assert persisted.race_id is not None
        assert persisted.session_id is not None
        logger.info(
            "Persistence completed status=%s race_id=%s session_id=%s "
            "drivers=%s laps=%s sectors=%s stints=%s track_status=%s warnings=%s",
            status,
            persisted.race_id,
            persisted.session_id,
            persisted.driver_count,
            persisted.lap_count,
            persisted.sector_count,
            persisted.stint_count,
            persisted.track_status_count,
            len(normalized.warnings),
        )

        return ImportResult(
            status=status,
            race_id=persisted.race_id,
            session_id=persisted.session_id,
            season=normalized.season,
            round=normalized.round,
            event_name=normalized.event_name,
            session_type=normalized.session_type,
            driver_count=persisted.driver_count,
            lap_count=persisted.lap_count,
            sector_count=persisted.sector_count,
            stint_count=persisted.stint_count,
            track_status_count=persisted.track_status_count,
            warnings=normalized.warnings,
            skipped_count=normalized.skipped_count,
        )

    @staticmethod
    def _already_imported_result(existing: ExistingSessionInfo) -> ImportResult:
        return ImportResult(
            status="already_imported",
            race_id=existing.race_id,
            session_id=existing.session_id,
            season=existing.season,
            round=existing.round,
            event_name=existing.event_name,
            session_type=existing.session_type,
            driver_count=existing.driver_count,
            lap_count=existing.lap_count,
            sector_count=existing.sector_count,
            stint_count=existing.stint_count,
            track_status_count=existing.track_status_count,
            warnings=[],
            skipped_count=0,
        )

    @staticmethod
    def _already_imported_from_persisted(persisted: PersistedImport) -> ImportResult:
        assert persisted.race_id is not None
        assert persisted.session_id is not None
        assert persisted.season is not None
        assert persisted.round is not None
        assert persisted.event_name is not None
        assert persisted.session_type is not None
        return ImportResult(
            status="already_imported",
            race_id=persisted.race_id,
            session_id=persisted.session_id,
            season=persisted.season,
            round=persisted.round,
            event_name=persisted.event_name,
            session_type=persisted.session_type,
            driver_count=persisted.driver_count,
            lap_count=persisted.lap_count,
            sector_count=persisted.sector_count,
            stint_count=persisted.stint_count,
            track_status_count=persisted.track_status_count,
            warnings=[],
            skipped_count=0,
        )
