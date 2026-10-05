"""Database-failure handling in the replay service (SQLite, fake time, no waiting)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.replays import get_replay_service
from app.domain.enums import ReplayStatus
from app.main import create_app
from app.replay.errors import InvalidReplayTransitionError, ReplayPersistenceError
from app.replay.repository import ReplayRepository
from app.replay.service import INTERRUPTED_REASON
from tests.replay.conftest import ReplayEnv
from tests.replay.fakes import settle, wait_until_idle
from tests.replay.test_service import persisted

REPOSITORY_METHODS = (
    "get",
    "save",
    "load_timeline",
    "session_race_id",
    "timeline_exists",
    "stop_active",
)


class DbFault:
    """Makes chosen ``ReplayRepository`` methods raise ``OperationalError``."""

    def __init__(self) -> None:
        self.broken: set[str] = set()

    def break_(self, *names: str) -> None:
        self.broken.update(names)

    def heal(self) -> None:
        self.broken.clear()


@pytest.fixture
def fault(monkeypatch: pytest.MonkeyPatch) -> DbFault:
    fault = DbFault()

    def wrap(name: str, original: Callable[..., Any]) -> Callable[..., Any]:
        async def wrapper(self: ReplayRepository, *args: Any, **kwargs: Any) -> Any:
            if name in fault.broken:
                raise OperationalError("SELECT 1", {}, Exception("database down"))
            return await original(self, *args, **kwargs)

        return wrapper

    for name in REPOSITORY_METHODS:
        monkeypatch.setattr(ReplayRepository, name, wrap(name, getattr(ReplayRepository, name)))
    return fault


@pytest.fixture
async def api(replay_env: ReplayEnv) -> AsyncIterator[AsyncClient]:
    application = create_app()
    application.dependency_overrides[get_replay_service] = lambda: replay_env.service
    async with AsyncClient(
        transport=ASGITransport(app=application), base_url="http://test"
    ) as client:
        yield client
    application.dependency_overrides.clear()


async def new_replay(env: ReplayEnv, speed: int = 1) -> UUID:
    return (await env.service.create(env.race.session_id, speed)).state.replay_id


async def test_read_failures_return_503_via_api(
    api: AsyncClient, replay_env: ReplayEnv, fault: DbFault
) -> None:
    replay_id = await new_replay(replay_env)
    session_id = str(replay_env.race.session_id)

    fault.break_("get")
    for response in (
        await api.get(f"/replays/{replay_id}"),
        await api.post(f"/replays/{replay_id}/start"),
        await api.post(f"/replays/{replay_id}/pause"),
        await api.put(f"/replays/{replay_id}/speed", json={"playback_speed": 5}),
    ):
        assert response.status_code == 503
        assert response.json()["detail"] == "Replay storage unavailable"
    fault.heal()

    fault.break_("load_timeline")
    assert (await api.post(f"/replays/{replay_id}/start")).status_code == 503
    fault.heal()
    assert replay_env.service.active_replay_ids() == []

    for name in ("session_race_id", "timeline_exists"):
        fault.break_(name)
        assert (await api.post("/replays", json={"session_id": session_id})).status_code == 503
        fault.heal()

    assert (await api.post(f"/replays/{replay_id}/start")).status_code == 200


async def test_restart_read_failure_returns_503_and_keeps_running(
    api: AsyncClient, replay_env: ReplayEnv, fault: DbFault
) -> None:
    replay_id = await new_replay(replay_env)
    await replay_env.service.start(replay_id)

    fault.break_("get")
    assert (await api.post(f"/replays/{replay_id}/restart")).status_code == 503
    fault.heal()
    fault.break_("load_timeline")
    assert (await api.post(f"/replays/{replay_id}/restart")).status_code == 503
    fault.heal()

    assert (await api.get(f"/replays/{replay_id}")).json()["status"] == "RUNNING"
    assert replay_env.service.active_replay_ids() == [replay_id]


async def test_recover_interrupted_maps_database_failure(
    replay_env: ReplayEnv, fault: DbFault
) -> None:
    fault.break_("stop_active")
    with pytest.raises(ReplayPersistenceError):
        await replay_env.service.recover_interrupted()


async def test_failed_pause_save_keeps_memory_state_and_retries_on_next_access(
    api: AsyncClient,
    replay_env: ReplayEnv,
    session_factory: async_sessionmaker[AsyncSession],
    fault: DbFault,
) -> None:
    replay_id = await new_replay(replay_env)
    await replay_env.service.start(replay_id)
    await replay_env.timer.advance(50)

    fault.break_("save")
    response = await api.post(f"/replays/{replay_id}/pause")
    assert response.status_code == 503

    # The read still works and reports the in-memory state even though the deferred
    # write keeps failing.
    body = (await api.get(f"/replays/{replay_id}")).json()
    assert body["status"] == "PAUSED" and body["current_race_time_ms"] == 50_000
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.RUNNING

    # A retry follows the state machine instead of re-applying the change.
    assert (await api.post(f"/replays/{replay_id}/pause")).status_code == 409

    fault.heal()
    assert (await api.get(f"/replays/{replay_id}")).json()["status"] == "PAUSED"
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.PAUSED and row.paused_at is not None
    assert row.current_race_time_ms == 50_000


async def test_failed_speed_change_save_is_flushed_by_next_command(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession], fault: DbFault
) -> None:
    svc = replay_env.service
    replay_id = await new_replay(replay_env)
    await svc.start(replay_id)

    fault.break_("save")
    with pytest.raises(ReplayPersistenceError):
        await svc.change_speed(replay_id, 10)
    fault.heal()
    await svc.pause(replay_id)
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.PAUSED and str(row.playback_speed) in ("10", "10.00")


async def test_failed_stop_save_unregisters_runner_and_persists_later(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession], fault: DbFault
) -> None:
    svc = replay_env.service
    replay_id = await new_replay(replay_env)
    await svc.start(replay_id)
    await replay_env.timer.advance(10)

    fault.break_("save")
    with pytest.raises(ReplayPersistenceError):
        await svc.stop(replay_id)
    assert svc.active_replay_ids() == []

    view = await svc.get(replay_id)  # deferred save fails again; read still succeeds
    assert view.state.status is ReplayStatus.STOPPED and view.state.ended_at is not None
    assert view.state.status_reason is None  # not overwritten by orphan repair
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.RUNNING
    with pytest.raises(InvalidReplayTransitionError):  # never resurrected
        await svc.resume(replay_id)
    assert svc.active_replay_ids() == []

    fault.heal()
    view = await svc.get(replay_id)
    assert view.state.status is ReplayStatus.STOPPED and view.state.status_reason is None
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.STOPPED and row.status_reason is None
    assert row.status_reason != INTERRUPTED_REASON and row.ended_at is not None


async def test_shutdown_makes_final_attempt_for_pending_terminal_state(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession], fault: DbFault
) -> None:
    svc = replay_env.service
    replay_id = await new_replay(replay_env)
    await svc.start(replay_id)
    fault.break_("save")
    with pytest.raises(ReplayPersistenceError):
        await svc.stop(replay_id)
    fault.heal()
    await svc.shutdown()
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.STOPPED


async def test_failed_completion_save_unregisters_runner_and_persists_later(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession], fault: DbFault
) -> None:
    svc = replay_env.service
    replay_id = await new_replay(replay_env, 20)
    await svc.start(replay_id)

    fault.break_("save")
    await replay_env.timer.advance(1_000)
    await wait_until_idle(svc)  # runner unregistered although the save failed
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.RUNNING

    view = await svc.get(replay_id)
    assert view.state.status is ReplayStatus.COMPLETED
    assert view.state.emitted_event_count == replay_env.event_count

    fault.heal()
    assert (await svc.get(replay_id)).state.status is ReplayStatus.COMPLETED
    row = await persisted(session_factory, replay_id)
    assert row.status is ReplayStatus.COMPLETED and row.status_reason is None
    await settle()
    assert svc.active_replay_ids() == []


async def test_restart_supersedes_pending_terminal_state(
    replay_env: ReplayEnv, session_factory: async_sessionmaker[AsyncSession], fault: DbFault
) -> None:
    svc = replay_env.service
    replay_id = await new_replay(replay_env)
    await svc.start(replay_id)
    fault.break_("save")
    with pytest.raises(ReplayPersistenceError):
        await svc.stop(replay_id)
    with pytest.raises(ReplayPersistenceError):  # launch is atomic: nothing starts
        await svc.restart(replay_id)
    assert svc.active_replay_ids() == []

    fault.heal()
    assert (await svc.restart(replay_id)).state.status is ReplayStatus.RUNNING
    assert (await persisted(session_factory, replay_id)).status is ReplayStatus.RUNNING
    assert (await svc.get(replay_id)).state.status is ReplayStatus.RUNNING
