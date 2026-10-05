"""HTTP tests for replay control endpoints."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.replays import get_replay_service
from app.main import create_app
from tests.replay.conftest import ReplayEnv
from tests.timeline.factories import representative_race
from tests.timeline.seed import seed_source

RESPONSE_FIELDS = {
    "id",
    "session_id",
    "race_id",
    "status",
    "status_reason",
    "playback_speed",
    "current_race_time_ms",
    "current_sequence",
    "emitted_event_count",
    "total_events",
    "current_lap",
    "total_laps",
    "created_at",
    "started_at",
    "paused_at",
    "ended_at",
}


@pytest.fixture
async def api(replay_env: ReplayEnv) -> AsyncIterator[AsyncClient]:
    application = create_app()
    application.dependency_overrides[get_replay_service] = lambda: replay_env.service
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    application.dependency_overrides.clear()


async def create(api: AsyncClient, env: ReplayEnv, speed: float = 1) -> str:
    response = await api.post(
        "/replays", json={"session_id": str(env.race.session_id), "playback_speed": speed}
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def test_lifecycle_endpoints(api: AsyncClient, replay_env: ReplayEnv) -> None:
    replay_id = await create(api, replay_env, 5)
    body = (await api.get(f"/replays/{replay_id}")).json()
    assert set(body) == RESPONSE_FIELDS
    assert body["status"] == "CREATED" and body["playback_speed"] == 5.0
    assert body["race_id"] == str(replay_env.race.race_id)

    started = await api.post(f"/replays/{replay_id}/start")
    assert started.status_code == 200
    assert started.json()["status"] == "RUNNING"
    assert started.json()["total_events"] == replay_env.event_count

    await replay_env.timer.advance(20)  # 100 s of race time at 5x
    paused = (await api.post(f"/replays/{replay_id}/pause")).json()
    assert paused["status"] == "PAUSED" and paused["current_race_time_ms"] == 100_000
    assert paused["current_lap"] == 2 and paused["total_laps"] == 5

    speed = await api.put(f"/replays/{replay_id}/speed", json={"playback_speed": 20})
    assert speed.status_code == 200
    assert speed.json()["playback_speed"] == 20.0
    assert speed.json()["current_race_time_ms"] == 100_000

    assert (await api.post(f"/replays/{replay_id}/resume")).json()["status"] == "RUNNING"
    stopped = (await api.post(f"/replays/{replay_id}/stop")).json()
    assert stopped["status"] == "STOPPED" and stopped["ended_at"] is not None

    restarted = (await api.post(f"/replays/{replay_id}/restart")).json()
    assert restarted["status"] == "RUNNING"
    assert restarted["current_race_time_ms"] == 0 and restarted["playback_speed"] == 20.0

    await replay_env.timer.advance(1_000)
    final = (await api.get(f"/replays/{replay_id}")).json()
    assert final["status"] == "COMPLETED"
    assert final["emitted_event_count"] == replay_env.event_count


async def test_invalid_transitions_and_repeats_return_409(
    api: AsyncClient, replay_env: ReplayEnv
) -> None:
    replay_id = await create(api, replay_env)
    response = await api.post(f"/replays/{replay_id}/pause")
    assert response.status_code == 409
    assert response.json()["current_status"] == "CREATED"

    assert (await api.post(f"/replays/{replay_id}/start")).status_code == 200
    repeated = await api.post(f"/replays/{replay_id}/start")
    assert repeated.status_code == 409 and repeated.json()["current_status"] == "RUNNING"

    assert (await api.post(f"/replays/{replay_id}/pause")).status_code == 200
    again = await api.post(f"/replays/{replay_id}/pause")
    assert again.status_code == 409 and again.json()["current_status"] == "PAUSED"

    assert (await api.post(f"/replays/{replay_id}/stop")).status_code == 200
    for _ in range(2):
        stop = await api.post(f"/replays/{replay_id}/stop")
        assert stop.status_code == 409 and stop.json()["current_status"] == "STOPPED"
    assert (await api.post(f"/replays/{replay_id}/resume")).status_code == 409
    assert (await api.get(f"/replays/{replay_id}")).json()["status"] == "STOPPED"


@pytest.mark.parametrize("speed", [0, -1, 3, 0.5, 1000, "fast"])
async def test_invalid_speed_rejected(
    api: AsyncClient, replay_env: ReplayEnv, speed: object
) -> None:
    replay_id = await create(api, replay_env)
    response = await api.put(f"/replays/{replay_id}/speed", json={"playback_speed": speed})
    assert response.status_code == 422
    create_response = await api.post(
        "/replays", json={"session_id": str(replay_env.race.session_id), "playback_speed": speed}
    )
    assert create_response.status_code == 422


async def test_not_found_and_missing_timeline(api: AsyncClient, replay_env: ReplayEnv) -> None:
    missing = uuid4()
    assert (await api.get(f"/replays/{missing}")).status_code == 404
    for action in ("start", "pause", "resume", "stop", "restart"):
        assert (await api.post(f"/replays/{missing}/{action}")).status_code == 404
    assert (
        await api.put(f"/replays/{missing}/speed", json={"playback_speed": 2})
    ).status_code == 404
    assert (await api.post("/replays", json={"session_id": str(uuid4())})).status_code == 404
    assert (await api.get("/replays/not-a-uuid")).status_code == 422


async def test_create_without_timeline_returns_409(
    api: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    src = replace(representative_race(), season=2021, round=7)
    await seed_source(session_factory, src)
    response = await api.post("/replays", json={"session_id": str(src.session_id)})
    assert response.status_code == 409
    assert "POST" in response.json()["detail"]
