"""HTTP tests for the timeline endpoints."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.session import get_db
from app.main import create_app
from tests.timeline.factories import representative_race
from tests.timeline.seed import seed_source


@pytest.fixture
async def api(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[tuple[AsyncClient, str], None]:
    application = create_app()
    src = representative_race()
    await seed_source(session_factory, src)

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    application.dependency_overrides[get_db] = override_db
    transport = ASGITransport(app=application)
    async with (
        AsyncClient(transport=transport, base_url="http://test") as client,
        application.router.lifespan_context(application),
    ):
        yield client, str(src.session_id)
    application.dependency_overrides.clear()


async def test_post_generates_then_reports_already_generated(
    api: tuple[AsyncClient, str],
) -> None:
    client, sid = api
    first = await client.post(f"/api/v1/sessions/{sid}/timeline", json={"regenerate": False})
    assert first.status_code == 201
    body = first.json()
    assert body["status"] == "generated"
    assert body["event_count"] == sum(body["counts_by_type"].values())
    assert body["counts_by_type"]["RACE_STARTED"] == 1

    second = await client.post(f"/api/v1/sessions/{sid}/timeline")
    assert second.status_code == 200
    assert second.json()["status"] == "already_generated"

    third = await client.post(f"/api/v1/sessions/{sid}/timeline", json={"regenerate": True})
    assert third.status_code == 201
    assert third.json()["status"] == "regenerated"


async def test_get_events_paginated_ordered_with_filters(api: tuple[AsyncClient, str]) -> None:
    client, sid = api
    await client.post(f"/api/v1/sessions/{sid}/timeline")

    page = await client.get(f"/api/v1/sessions/{sid}/timeline", params={"limit": 4})
    assert page.status_code == 200
    body = page.json()
    assert body["limit"] == 4 and body["offset"] == 0
    assert [e["sequence"] for e in body["items"]] == [0, 1, 2, 3]
    assert body["items"][0]["event_type"] == "RACE_STARTED"
    assert set(body["items"][0]) == {
        "id",
        "sequence",
        "event_type",
        "race_time_ms",
        "lap_number",
        "driver_id",
        "driver_abbreviation",
        "payload",
    }

    filtered = await client.get(
        f"/api/v1/sessions/{sid}/timeline",
        params=[("event_type", "PIT_ENTRY"), ("event_type", "PIT_EXIT"), ("driver", "HAM")],
    )
    items = filtered.json()["items"]
    assert [e["event_type"] for e in items] == ["PIT_ENTRY", "PIT_EXIT"]

    laps = await client.get(
        f"/api/v1/sessions/{sid}/timeline",
        params={"event_type": "LAP_COMPLETED", "lap_from": 5, "lap_to": 5},
    )
    assert laps.json()["total"] == 3


async def test_summary_endpoint(api: tuple[AsyncClient, str]) -> None:
    client, sid = api
    await client.post(f"/api/v1/sessions/{sid}/timeline")
    response = await client.get(f"/api/v1/sessions/{sid}/timeline/summary")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "available"
    assert body["schema_version"] == 1
    assert body["is_current_schema_version"] is True
    assert body["race_start_session_time_ms"] == 3_600_000
    assert isinstance(body["warnings"], list)


async def test_not_generated_returns_actionable_404(api: tuple[AsyncClient, str]) -> None:
    client, sid = api
    for path in (f"/api/v1/sessions/{sid}/timeline", f"/api/v1/sessions/{sid}/timeline/summary"):
        response = await client.get(path)
        assert response.status_code == 404
        assert "POST" in response.json()["message"]


async def test_unknown_session_returns_404(api: tuple[AsyncClient, str]) -> None:
    client, _ = api
    missing = uuid4()
    assert (await client.post(f"/api/v1/sessions/{missing}/timeline")).status_code == 404
    assert (await client.get(f"/api/v1/sessions/{missing}/timeline")).status_code == 404
    assert (await client.get(f"/api/v1/sessions/{missing}/timeline/summary")).status_code == 404


async def test_query_validation(api: tuple[AsyncClient, str]) -> None:
    client, sid = api
    assert (
        await client.get(f"/api/v1/sessions/{sid}/timeline", params={"limit": 0})
    ).status_code == 422
    assert (
        await client.get(f"/api/v1/sessions/{sid}/timeline", params={"limit": 1001})
    ).status_code == 422
    assert (
        await client.get(f"/api/v1/sessions/{sid}/timeline", params={"event_type": "NOPE"})
    ).status_code == 422


async def test_validation_failure_returns_422_with_problems(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    from dataclasses import replace

    from tests.timeline.factories import drv, lap, source

    d = drv("VER", 1)
    src = source(
        [d],
        [
            lap(d, 1, start=3_600_000, end=3_690_000),
            lap(d, 2, start=3_690_000, end=3_780_000),
            lap(d, 3, start=3_780_000, end=3_700_000),
        ],
    )
    src = replace(src)
    await seed_source(session_factory, src)
    application = create_app()

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    application.dependency_overrides[get_db] = override_db
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(f"/api/v1/sessions/{src.session_id}/timeline")
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "TIMELINE_INVALID"
    assert "failed validation" in body["message"]
    assert body["details"]["problems"]
