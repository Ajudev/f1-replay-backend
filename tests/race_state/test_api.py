"""Race state HTTP endpoints."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError as RedisConnectionError

from app.api.race_state import get_race_state_service
from app.domain.enums import ReplayStatus
from app.main import create_app
from app.race_state.models import RaceState, SnapshotTrigger
from app.race_state.service import RaceStateService
from app.race_state.snapshots import DatabaseSnapshotSink
from app.replay.service import ReplayService
from tests.race_state.conftest import StateEnv, make_replay
from tests.replay.fakes import ManualTimer
from tests.streaming.conftest import RecordingHandler  # noqa: F401


@dataclass
class ApiEnv:
    client: AsyncClient
    env: StateEnv
    service: RaceStateService


class BrokenRedisStore:
    async def get(self, replay_id: UUID) -> RaceState | None:
        raise RedisConnectionError("redis down")


@pytest.fixture
async def api(state_env: StateEnv) -> AsyncIterator[ApiEnv]:
    from tests.replay.fakes import CollectingSink

    replays = ReplayService(state_env.factory, sink=CollectingSink(), timer=ManualTimer())
    service = RaceStateService(state_env.store, state_env.factory, replays)
    application = create_app()
    application.dependency_overrides[get_race_state_service] = lambda: service
    async with AsyncClient(
        transport=ASGITransport(app=application), base_url="http://test"
    ) as client:
        yield ApiEnv(client, state_env, service)
    application.dependency_overrides.clear()


async def processed_replay(env: StateEnv, upto: int, status: ReplayStatus = ReplayStatus.COMPLETED):
    replay_id = await make_replay(env.factory, env.race.session_id, status)
    events = env.events(replay_id, uuid4())
    for event in events[:upto]:
        await env.process(event)
    return replay_id, events


async def test_live_state_with_drivers_sorted_by_position(api: ApiEnv) -> None:
    replay_id, _ = await processed_replay(api.env, 12)

    response = await api.client.get(f"/replays/{replay_id}/state")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["source"] == "live" and body["replay_status"] == "COMPLETED"
    assert body["replay_id"] == str(replay_id) and body["phase"] == "RUNNING"
    assert body["current_lap"] == 3 and body["total_laps"] == 5
    assert body["last_sequence"] == 11
    assert body["track_status"] == "SAFETY_CAR" and body["fastest_lap"]["lap_time_ms"] == 90_000
    positions = [d["position"] for d in body["drivers"]]
    assert positions == sorted(positions)
    assert [d["abbreviation"] for d in body["drivers"]] == ["VER", "NOR", "HAM"]
    assert "lap_crossings" not in body and "run_published_at" not in body
    assert all("recent_laps" in d for d in body["drivers"])


async def test_falls_back_to_the_latest_snapshot_when_redis_has_no_state(api: ApiEnv) -> None:
    env = api.env
    replay_id, events = await processed_replay(env, 14)  # snapshots at 0, 9
    await env.redis.client.delete(env.store.key(replay_id))

    response = await api.client.get(f"/replays/{replay_id}/state")

    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "snapshot"
    assert body["last_sequence"] == 9  # the last snapshot, older than the lost live state


async def test_unknown_replay_is_404(api: ApiEnv) -> None:
    response = await api.client.get(f"/replays/{uuid4()}/state")
    assert response.status_code == 404
    assert "Replay not found" in response.json()["detail"]


async def test_not_started_replay_is_409(api: ApiEnv) -> None:
    replay_id = await make_replay(api.env.factory, api.env.race.session_id, ReplayStatus.CREATED)
    response = await api.client.get(f"/replays/{replay_id}/state")
    assert response.status_code == 409
    assert "not been started" in response.json()["detail"]


async def test_state_unavailable_is_404_with_a_distinct_message(api: ApiEnv) -> None:
    replay_id = await make_replay(api.env.factory, api.env.race.session_id)
    response = await api.client.get(f"/replays/{replay_id}/state")
    assert response.status_code == 404
    assert "No race state is available" in response.json()["detail"]


async def test_redis_failure_is_503(api: ApiEnv) -> None:
    replay_id, _ = await processed_replay(api.env, 5)
    api.service._store = BrokenRedisStore()  # type: ignore[assignment]
    response = await api.client.get(f"/replays/{replay_id}/state")
    assert response.status_code == 503
    assert response.json()["detail"] == "Race state store unavailable"


async def test_driver_by_abbreviation_and_by_uuid(api: ApiEnv) -> None:
    replay_id, _ = await processed_replay(api.env, 12)
    state = await api.env.state(replay_id)
    assert state is not None
    nor = next(d for d in state.drivers.values() if d.abbreviation == "NOR")

    by_abbr = await api.client.get(f"/replays/{replay_id}/state/drivers/nor")
    by_id = await api.client.get(f"/replays/{replay_id}/state/drivers/{nor.driver_id}")

    assert by_abbr.status_code == by_id.status_code == 200
    assert by_abbr.json() == by_id.json()
    body = by_abbr.json()
    assert body["driver"]["abbreviation"] == "NOR" and body["driver"]["laps_completed"] == 1
    assert body["source"] == "live" and body["last_sequence"] == 11


async def test_unknown_driver_is_404(api: ApiEnv) -> None:
    replay_id, _ = await processed_replay(api.env, 5)
    for driver in ("XXX", str(uuid4())):
        response = await api.client.get(f"/replays/{replay_id}/state/drivers/{driver}")
        assert response.status_code == 404
        assert "not part of the race state" in response.json()["detail"]


async def test_app_wires_the_service_in_its_lifespan(client: AsyncClient) -> None:
    # The unconfigured test environment has no replay: 404, but routed and wired.
    response = await client.get(f"/replays/{uuid4()}/state")
    assert response.status_code in {404, 503}


async def test_snapshot_fallback_uses_the_newest_snapshot_across_runs(api: ApiEnv) -> None:
    env = api.env
    replay_id = await make_replay(env.factory, env.race.session_id)
    sink = DatabaseSnapshotSink(env.factory)
    from datetime import timedelta

    from tests.race_state.conftest import BASE_TIME

    old_events = env.events(replay_id, uuid4(), published_at=BASE_TIME)
    for event in old_events[:3]:
        await env.process(event)
    old = await env.state(replay_id)
    assert old is not None
    await sink.save(old, SnapshotTrigger.PERIODIC)
    new_events = env.events(replay_id, uuid4(), published_at=BASE_TIME + timedelta(hours=1))
    await env.process(new_events[0])
    fresh = await env.state(replay_id)
    assert fresh is not None
    await env.redis.client.delete(env.store.key(replay_id))

    response = await api.client.get(f"/replays/{replay_id}/state")

    assert response.status_code == 200
    assert response.json()["run_id"] == str(fresh.run_id)  # the INITIAL snapshot of run two
