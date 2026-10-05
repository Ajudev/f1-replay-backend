"""HTTP API tests for race import and query endpoints."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.races import get_session_loader
from app.db.session import get_db
from app.domain.enums import SessionType
from app.ingestion.errors import SessionLoadError
from app.ingestion.records import (
    ExtractedDriver,
    ExtractedLap,
    ExtractedSession,
    ExtractedTrackStatus,
)
from app.main import create_app


class FakeLoader:
    def __init__(self, extracted: ExtractedSession | None = None) -> None:
        self.extracted = extracted or _sample()
        self.fail_with: Exception | None = None
        self.calls = 0

    def load(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> ExtractedSession:
        self.calls += 1
        if self.fail_with is not None:
            raise self.fail_with
        return self.extracted


def _sample() -> ExtractedSession:
    return ExtractedSession(
        season=2024,
        round=1,
        event_name="Bahrain Grand Prix",
        official_event_name="Official Bahrain",
        country="Bahrain",
        location="Sakhir",
        event_date=date(2024, 3, 2),
        session_type=SessionType.RACE,
        session_name="Race",
        session_start=None,
        drivers=[
            ExtractedDriver(
                driver_number=4,
                abbreviation="NOR",
                first_name="Lando",
                last_name="Norris",
                full_name="Lando Norris",
                team_name="McLaren",
                grid_position=5,
                finish_position=3,
                result_status="Finished",
            )
        ],
        laps=[
            ExtractedLap(
                driver_abbreviation="NOR",
                driver_number=4,
                lap_number=1,
                lap_time_ms=90_000,
                position=4,
                compound="SOFT",
                tyre_age_laps=1,
                stint_number=1,
                pit_in_time_ms=None,
                pit_out_time_ms=None,
                sector1_time_ms=30_000,
                sector2_time_ms=30_000,
                sector3_time_ms=30_000,
                lap_start_time_ms=0,
                lap_end_time_ms=90_000,
                is_deleted=False,
                is_accurate=True,
                team_name="McLaren",
            )
        ],
        track_statuses=[
            ExtractedTrackStatus(race_time_ms=0, source_code="1", message="AllClear"),
        ],
    )


@pytest.fixture
async def api_client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[tuple[AsyncClient, FakeLoader], None]:
    application = create_app()
    loader = FakeLoader()

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    application.dependency_overrides[get_db] = override_db
    application.dependency_overrides[get_session_loader] = lambda: loader

    transport = ASGITransport(app=application)
    async with (
        AsyncClient(transport=transport, base_url="http://test") as client,
        application.router.lifespan_context(application),
    ):
        yield client, loader

    application.dependency_overrides.clear()


async def test_import_returns_201_and_counts(
    api_client: tuple[AsyncClient, FakeLoader],
) -> None:
    client, _ = api_client
    response = await client.post(
        "/races/import",
        json={"season": 2024, "round": 1, "session_type": "RACE"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "imported"
    assert body["driver_count"] == 1
    assert body["lap_count"] == 1
    assert body["sector_count"] == 3
    assert body["stint_count"] == 1
    assert body["track_status_count"] == 1


async def test_second_import_returns_200_already_imported(
    api_client: tuple[AsyncClient, FakeLoader],
) -> None:
    client, loader = api_client
    first = await client.post(
        "/races/import",
        json={"season": 2024, "round": 1},
    )
    second = await client.post(
        "/races/import",
        json={"season": 2024, "round": 1},
    )
    assert first.status_code == 201
    assert second.status_code == 200
    body = second.json()
    assert body["status"] == "already_imported"
    assert loader.calls == 1
    assert body["driver_count"] == first.json()["driver_count"] == 1
    assert body["lap_count"] == first.json()["lap_count"] == 1
    assert body["sector_count"] == first.json()["sector_count"] == 3
    assert body["stint_count"] == 1
    assert body["track_status_count"] == 1
    assert body["warnings"] == []
    assert body["skipped_count"] == 0


async def test_validation_422_when_both_round_and_event_name(
    api_client: tuple[AsyncClient, FakeLoader],
) -> None:
    client, _ = api_client
    response = await client.post(
        "/races/import",
        json={"season": 2024, "round": 1, "event_name": "Bahrain"},
    )
    assert response.status_code == 422


async def test_404_unknown_race(api_client: tuple[AsyncClient, FakeLoader]) -> None:
    client, _ = api_client
    response = await client.get(f"/races/{uuid4()}")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


async def test_get_session_returns_drivers_and_counts(
    api_client: tuple[AsyncClient, FakeLoader],
) -> None:
    client, _ = api_client
    imported = await client.post(
        "/races/import",
        json={"season": 2024, "round": 1},
    )
    session_id = imported.json()["session_id"]
    response = await client.get(f"/sessions/{session_id}")
    assert response.status_code == 200
    body = response.json()
    assert len(body["drivers"]) == 1
    assert body["drivers"][0]["abbreviation"] == "NOR"
    assert body["drivers"][0]["grid_position"] == 5
    assert body["drivers"][0]["finish_position"] == 3
    assert body["counts"]["laps"] == 1
    assert body["counts"]["sectors"] == 3
    assert body["counts"]["stints"] == 1
    assert body["counts"]["track_status_periods"] == 1


async def test_session_load_error_returns_502_without_raw_exception(
    api_client: tuple[AsyncClient, FakeLoader],
) -> None:
    client, loader = api_client
    loader.fail_with = SessionLoadError("Failed to load session RACE for 2024 event 1")
    response = await client.post(
        "/races/import",
        json={"season": 2024, "round": 1},
    )
    assert response.status_code == 502
    detail = response.json()["detail"]
    assert detail == "Failed to load session RACE for 2024 event 1"
    assert "Traceback" not in response.text
    assert "fastf1" not in response.text.lower()
