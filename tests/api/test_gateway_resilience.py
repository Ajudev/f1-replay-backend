"""Gateway edge cases: restarts overlapping tail lag, page saturation, floods, bad frames."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import event

from app.gateway.config import GatewayConfig
from app.gateway.fanout import StreamTail
from app.gateway.messages import STATE_DERIVED_TYPES
from tests.api.conftest import API, ApiEnv, settle


async def run_consumers(api_env: ApiEnv) -> None:
    for consumer in (api_env.state_consumer, api_env.detection_consumer):
        while await consumer.process_batch():
            pass


@pytest.mark.parametrize("resync", [False, True])
async def test_old_run_entries_never_follow_a_new_run_snapshot(
    api_env: ApiEnv, resync: bool
) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.timer.advance(6)
    await settle()
    await run_consumers(api_env)  # the old run is in the streams; the tail has not read it
    old_run = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()["run_id"]

    assert (await api_env.client.post(f"{API}/replays/{replay_id}/restart")).status_code == 200
    await api_env.timer.advance(2)
    await settle()
    await run_consumers(api_env)

    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    if resync:
        await ws.send_json({"type": "RESYNC"})
        snapshot = await ws.receive()
    new_run = snapshot["payload"]["state"]["run_id"]
    assert new_run != old_run and snapshot["run_id"] == new_run
    while await api_env.gateway.tail.read_once():  # now the tail delivers the backlog
        pass
    await settle()

    after = [m for m in await ws.messages() if m["type"] in STATE_DERIVED_TYPES]
    assert all(m["run_id"] != old_run for m in after)
    assert all(m["sequence"] > snapshot["sequence"] for m in after)


async def test_a_restart_while_connected_is_delivered_as_a_new_run(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.advance(6)
    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()

    await api_env.client.post(f"{API}/replays/{replay_id}/restart")
    await api_env.advance(6)

    state_messages = [m for m in await ws.messages() if m["type"] in STATE_DERIVED_TYPES]
    runs = [m["run_id"] for m in state_messages]
    assert runs and set(runs) == {runs[-1]} and runs[-1] != snapshot["run_id"]
    assert state_messages[0]["type"] == "RACE_STATE_SNAPSHOT"
    assert state_messages[0]["payload"]["reason"] == "STATE_INITIALIZED"


async def test_stream_tail_keeps_publish_order_when_a_page_saturates(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.finish()
    published: list[tuple[str, int, str]] = []

    async def collect(stream: str, item: Any) -> None:
        published.append((stream, item.sequence, item.event_type))

    config = GatewayConfig(stream_read_count=3, stream_block_ms=1)
    tail = StreamTail(api_env.redis, api_env.stream_config, config, collect)
    tail._last_ids = dict.fromkeys(tail._streams, "0-0")
    while await tail.read_once():
        pass

    entries: list[tuple[tuple[int, int], str]] = []
    for stream in tail._streams:
        for entry_id, _ in await api_env.redis.client.xrange(stream):
            ms, _, seq = entry_id.partition("-")
            entries.append(((int(ms), int(seq)), stream))
    assert len(published) == len(entries) > 6  # nothing lost or duplicated
    expected = [stream for _, stream in sorted(entries)]
    assert [stream for stream, _, _ in published] == expected


async def test_stream_tail_survives_unexpected_errors(api_env: ApiEnv) -> None:
    config = GatewayConfig(stream_block_ms=1, retry_backoff_seconds=0.01)
    seen: list[int] = []

    async def handler(stream: str, item: Any) -> None:
        seen.append(item.sequence)

    tail = StreamTail(api_env.redis, api_env.stream_config, config, handler)
    real = tail.read_once
    calls = 0

    async def flaky() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("unexpected")
        return await real()

    tail.read_once = flaky  # type: ignore[method-assign]
    tail.start()
    try:
        for _ in range(200):
            if calls >= 3:
                break
            await asyncio.sleep(0.01)
        assert calls >= 3 and tail._task is not None and not tail._task.done()
    finally:
        await tail.stop()


async def test_ping_flood_does_not_grow_the_queue(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    gate = asyncio.Event()
    ws = await api_env.ws(replay_id, send_gate=gate)
    await settle()

    for _ in range(2_000):
        await ws.send_json({"type": "PING"})
    await settle(300)

    (connection,) = api_env.gateway.manager._groups[replay_id].values()
    assert connection.queued <= 8 and not connection.closed
    gate.set()
    await settle()


async def test_unexpected_snapshot_failure_sends_internal_error_and_1011(
    api_env: ApiEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    replay_id = await api_env.create_replay()

    async def boom(_: Any) -> Any:
        raise RuntimeError("secret internals")

    monkeypatch.setattr(api_env.race_state, "get_state", boom)
    ws = await api_env.ws(replay_id)

    received, code = await ws.until_closed()

    assert code == 1011
    assert [m["type"] for m in received] == ["ERROR"]
    assert received[0]["payload"]["code"] == "INTERNAL_ERROR"
    assert "secret" not in str(received)
    assert api_env.gateway.manager.connection_count() == 0


async def test_binary_frames_get_an_error_and_the_connection_survives(api_env: ApiEnv) -> None:
    ws = await api_env.ws(await api_env.create_replay())
    await ws.receive()

    await ws._to_app.put({"type": "websocket.receive", "bytes": b"\x00\x01"})

    error = await ws.receive()
    assert error["type"] == "ERROR" and error["payload"]["code"] == "UNSUPPORTED_CLIENT_MESSAGE"
    await ws.send_json({"type": "PING"})
    assert (await ws.receive())["type"] == "PONG"


async def test_invalid_speed_message_is_plain_text(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()

    response = await api_env.client.patch(
        f"{API}/replays/{replay_id}/speed", json={"playback_speed": 1000000000}
    )

    assert response.status_code == 422
    message = response.json()["message"]
    assert "Decimal" not in message and "'" not in message.split(";")[0]
    assert "1000000000" in message


async def test_timing_resolves_drivers_without_a_query_per_driver(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.finish()
    statements: list[str] = []

    @event.listens_for(api_env.factory.kw["bind"].sync_engine, "before_cursor_execute")
    def count(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
        statements.append(statement)

    one = await api_env.client.get(f"{API}/replays/{replay_id}/timing", params={"driver": "VER"})
    statements_one = len(statements)
    statements.clear()
    three = await api_env.client.get(
        f"{API}/replays/{replay_id}/timing",
        params=[("driver", "VER"), ("driver", "nor"), ("driver", "HAM")],
    )
    assert one.status_code == three.status_code == 200
    assert len(statements) == statements_one
    missing = await api_env.client.get(
        f"{API}/replays/{replay_id}/timing", params=[("driver", "VER"), ("driver", str(uuid4()))]
    )
    assert missing.status_code == 404 and missing.json()["code"] == "DRIVER_NOT_FOUND"


async def test_old_run_detections_are_not_delivered_after_a_restart(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.timer.advance(12)
    await settle()
    await run_consumers(api_env)
    old_run = api_env.replays.current_run_id(replay_id)
    old_events = (await api_env.client.get(f"{API}/replays/{replay_id}/events")).json()["items"]
    assert old_events and {e["run_id"] for e in old_events} == {str(old_run)}

    await api_env.client.post(f"{API}/replays/{replay_id}/restart")
    await api_env.timer.advance(2)
    await settle()
    await run_consumers(api_env)
    ws = await api_env.ws(replay_id)
    await ws.receive()
    while await api_env.gateway.tail.read_once():
        pass
    await settle()

    received = await ws.messages()
    assert not [m for m in received if m["run_id"] == str(old_run)]


async def test_current_run_id_follows_runs_and_survives_retirement(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    assert api_env.replays.current_run_id(replay_id) is None  # never ran in this process

    await api_env.start(replay_id)
    first = api_env.replays.current_run_id(replay_id)
    await api_env.client.post(f"{API}/replays/{replay_id}/restart")
    second = api_env.replays.current_run_id(replay_id)
    assert first is not None and second is not None and first != second

    await api_env.finish()  # the runner retires
    assert api_env.replays.current_run_id(replay_id) == second
    assert api_env.replays.current_run_id(uuid4()) is None


async def test_tail_order_with_non_full_pages_on_both_streams(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.finish()
    seen: list[str] = []

    async def collect(stream: str, item: Any) -> None:
        seen.append(stream)

    tail = StreamTail(
        api_env.redis,
        api_env.stream_config,
        GatewayConfig(stream_read_count=10_000, stream_block_ms=1),
        collect,
    )
    tail._last_ids = dict.fromkeys(tail._streams, "0-0")
    assert await tail.read_once() == len(seen) > 0
    ids: list[tuple[tuple[int, int], str]] = []
    for stream in tail._streams:
        for entry_id, _ in await api_env.redis.client.xrange(stream):
            ms, _, seq = entry_id.partition("-")
            ids.append(((int(ms), int(seq)), stream))
    assert seen == [stream for _, stream in sorted(ids)]
    assert await tail.read_once() == 0
