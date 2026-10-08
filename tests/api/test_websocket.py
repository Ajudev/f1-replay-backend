"""WebSocket gateway behavior through the real ASGI route, replay engine and consumers."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.gateway.fanout import StreamTail
from app.gateway.messages import STATE_DERIVED_TYPES
from tests.api.conftest import API, ApiEnv, WsClient, settle

Messages = list[dict[str, Any]]

#: Documented order of the messages one state event produces.
EVENT_ORDER = [
    "RACE_STATE_SNAPSHOT",
    "RACE_STATE_UPDATE",
    "DRIVER_UPDATE",
    "LAP_COMPLETED",
    "POSITION_CHANGED",
    "PIT_STATUS_CHANGED",
    "TRACK_STATUS_CHANGED",
]


def of_type(messages: Messages, *types: str) -> Messages:
    return [m for m in messages if m["type"] in types]


def shape(messages: Messages) -> list[tuple[str, int | None]]:
    return [(m["type"], m["sequence"]) for m in messages if m["type"] != "REPLAY_CLOCK"]


async def running(api_env: ApiEnv, seconds: float = 6) -> UUID:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.advance(seconds)
    return replay_id


# -- connect and snapshot ---------------------------------------------------------------------


async def test_connect_before_start_gets_a_snapshot_without_state(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()

    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()

    assert snapshot["type"] == "SNAPSHOT" and snapshot["schema_version"] == 1
    assert snapshot["replay_id"] == str(replay_id)
    assert snapshot["run_id"] is None and snapshot["sequence"] is None
    assert snapshot["emitted_at"]
    assert snapshot["payload"]["state"] is None
    assert snapshot["payload"]["state_error"]["code"] == "REPLAY_NOT_STARTED"
    assert snapshot["payload"]["replay"]["status"] == "CREATED"
    assert api_env.gateway.manager.connection_count(replay_id) == 1


async def test_mid_race_snapshot_equals_rest_state(api_env: ApiEnv) -> None:
    replay_id = await running(api_env)

    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    state = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()
    replay = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()

    assert snapshot["type"] == "SNAPSHOT"
    assert snapshot["payload"]["state"] == state
    assert snapshot["payload"]["replay"] == replay
    assert snapshot["payload"]["state_error"] is None
    assert snapshot["run_id"] == state["run_id"] and snapshot["sequence"] == state["last_sequence"]


@pytest.mark.parametrize("path_id", ["not-a-uuid", "12345"])
async def test_malformed_replay_id_is_rejected_before_accept(api_env: ApiEnv, path_id: str) -> None:
    code = await WsClient(api_env.app, f"{API}/replays/{path_id}/stream").rejected()
    assert code == 1008
    assert api_env.gateway.manager.connection_count() == 0


async def test_unknown_replay_gets_error_then_4404(api_env: ApiEnv) -> None:
    ws = await api_env.ws(uuid4())

    received, code = await ws.until_closed()

    assert [m["type"] for m in received] == ["ERROR"]
    assert received[0]["payload"]["code"] == "REPLAY_NOT_FOUND" and code == 4404
    await ws.finished()
    assert api_env.gateway.manager.connection_count() == 0


# -- state messages -----------------------------------------------------------------------------


async def test_state_messages_follow_the_race(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    await ws.receive()
    await api_env.start(replay_id)
    await api_env.advance(6)
    await api_env.finish()

    messages = await ws.messages()
    types = {m["type"] for m in messages}
    assert {
        "REPLAY_STATUS",
        "RACE_STATE_SNAPSHOT",
        "RACE_STATE_UPDATE",
        "DRIVER_UPDATE",
        "LAP_COMPLETED",
        "POSITION_CHANGED",
        "PIT_STATUS_CHANGED",
        "TRACK_STATUS_CHANGED",
        "DETECTED_EVENT",
    } <= types

    bootstrap = of_type(messages, "RACE_STATE_SNAPSHOT")[0]
    assert bootstrap["payload"]["reason"] == "STATE_INITIALIZED"
    assert len(bootstrap["payload"]["state"]["drivers"]) == 3

    update = of_type(messages, "RACE_STATE_UPDATE")[0]
    assert update["payload"]["kinds"] == ["RACE_STARTED"]
    assert update["payload"]["race"]["phase"] == "RUNNING"

    driver_update = of_type(messages, "DRIVER_UPDATE")[0]["payload"]["driver"]
    assert driver_update["abbreviation"] in {"VER", "HAM", "NOR"}
    assert "recent_laps" not in driver_update and "position" in driver_update

    lap = of_type(messages, "LAP_COMPLETED")[0]["payload"]
    assert set(lap) == {"driver_id", "abbreviation", "lap", "laps_completed"}
    assert lap["lap"]["lap_number"] == 1

    moves = {
        (m["payload"]["abbreviation"], m["payload"]["previous_position"], m["lap_number"]): m[
            "payload"
        ]["position"]
        for m in of_type(messages, "POSITION_CHANGED")
    }
    assert moves[("NOR", 3, 2)] == 2  # NOR gains P2 on lap 1

    pit = [m["payload"] for m in of_type(messages, "PIT_STATUS_CHANGED")]
    assert any(p["abbreviation"] == "HAM" and p["pit_stop_count"] >= 1 for p in pit)
    assert {"driver_id", "abbreviation", "pit_status", "pit_stop_count"} <= set(pit[0])

    track = [m["payload"]["track_status"] for m in of_type(messages, "TRACK_STATUS_CHANGED")]
    assert track[:2] == ["GREEN", "SAFETY_CAR"]


async def test_detected_event_payload_equals_rest_item(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    await ws.receive()
    await api_env.start(replay_id)
    await api_env.finish()

    detected = of_type(await ws.messages(), "DETECTED_EVENT")
    page = (await api_env.client.get(f"{API}/replays/{replay_id}/events")).json()

    assert detected and len(detected) == page["total"]
    rest = {item["detected_event_id"]: item for item in page["items"]}
    for message in detected:
        event = message["payload"]["event"]
        expected = rest[event["detected_event_id"]]
        # SQLite hands back naive datetimes; the stream carries the aware original.
        assert event | {"detected_at": event["detected_at"].rstrip("Z")} == expected | {
            "detected_at": expected["detected_at"].rstrip("Z")
        }
        assert message["run_id"] == event["run_id"] and message["lap_number"] == event["lap_number"]


# -- replay lifecycle -----------------------------------------------------------------------------


async def test_status_messages_on_pause_resume_and_speed(api_env: ApiEnv) -> None:
    replay_id = await running(api_env)
    ws = await api_env.ws(replay_id)
    await ws.receive()

    await api_env.client.post(f"{API}/replays/{replay_id}/pause")
    await api_env.client.post(f"{API}/replays/{replay_id}/resume")
    await api_env.client.patch(f"{API}/replays/{replay_id}/speed", json={"playback_speed": 5})

    statuses = of_type(await ws.messages(), "REPLAY_STATUS")
    replays = [m["payload"]["replay"] for m in statuses]
    assert [r["status"] for r in replays] == ["PAUSED", "RUNNING", "RUNNING"]
    assert [r["playback_speed"] for r in replays][-2:] == [20.0, 5.0]
    assert all(m["run_id"] is None for m in statuses)


async def test_completion_sends_replay_completed_and_keeps_the_socket_open(
    api_env: ApiEnv,
) -> None:
    replay_id = await running(api_env)
    ws = await api_env.ws(replay_id)
    await ws.receive()

    await api_env.finish()

    messages = await ws.messages()
    completed = of_type(messages, "REPLAY_COMPLETED")
    assert len(completed) == 1
    assert completed[0]["payload"]["replay"]["status"] == "COMPLETED"
    assert completed[0]["payload"]["replay"]["is_completed"] is True
    assert not ws.done and ws.close_code is None
    await ws.send_json({"type": "PING"})
    assert (await ws.receive())["type"] == "PONG"


async def test_clock_ticks_only_for_running_replays(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    await ws.receive()

    api_env.gateway.tick()
    assert of_type(await ws.messages(), "REPLAY_CLOCK") == []  # CREATED

    await api_env.start(replay_id)
    await api_env.advance(3)
    await ws.messages()
    api_env.gateway.tick()
    clock = of_type(await ws.messages(), "REPLAY_CLOCK")
    assert len(clock) == 1
    payload = clock[0]["payload"]
    assert payload["status"] == "RUNNING" and payload["current_race_time_ms"] > 0
    assert set(payload) == {
        "status",
        "current_race_time_ms",
        "current_lap",
        "total_laps",
        "playback_speed",
        "emitted_event_count",
        "total_events",
    }
    assert clock[0]["race_time_ms"] == payload["current_race_time_ms"]

    await api_env.client.post(f"{API}/replays/{replay_id}/pause")
    await ws.messages()
    api_env.gateway.tick()
    assert of_type(await ws.messages(), "REPLAY_CLOCK") == []

    await api_env.client.post(f"{API}/replays/{replay_id}/resume")
    await api_env.finish()
    await ws.messages()
    api_env.gateway.tick()
    assert of_type(await ws.messages(), "REPLAY_CLOCK") == []  # COMPLETED


# -- client messages ---------------------------------------------------------------------------------


async def test_ping_pong_and_unsupported_messages(api_env: ApiEnv) -> None:
    ws = await api_env.ws(await api_env.create_replay())
    await ws.receive()

    await ws.send_json({"type": "ping"})
    pong = await ws.receive()
    assert pong["type"] == "PONG" and pong["payload"] == {}

    for document in ({"type": "SUBSCRIBE"}, {"nope": 1}, [1, 2]):
        await ws.send_json(document)
        error = await ws.receive()
        assert error["type"] == "ERROR" and error["payload"]["code"] == "UNSUPPORTED_CLIENT_MESSAGE"
    await ws._to_app.put({"type": "websocket.receive", "text": "{not json"})
    assert (await ws.receive())["payload"]["code"] == "UNSUPPORTED_CLIENT_MESSAGE"
    assert not ws.done  # the connection survives bad client messages


async def test_resync_sends_a_fresh_authoritative_snapshot(api_env: ApiEnv) -> None:
    replay_id = await running(api_env, seconds=3)
    ws = await api_env.ws(replay_id)
    first = await ws.receive()
    await api_env.advance(6)
    await ws.messages()

    await ws.send_json({"type": "RESYNC"})
    snapshot = await ws.receive()

    state = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()
    assert snapshot["type"] == "SNAPSHOT" and snapshot["payload"]["state"] == state
    assert snapshot["sequence"] > first["sequence"]
    await api_env.advance(6)
    later = await ws.messages()
    assert later and all(
        m["sequence"] > snapshot["sequence"] for m in later if m["type"] in STATE_DERIVED_TYPES
    )


# -- connection lifecycle ---------------------------------------------------------------------------------


async def test_disconnect_cleans_up_and_reconnect_gets_the_current_snapshot(
    api_env: ApiEnv,
) -> None:
    replay_id = await running(api_env, seconds=3)
    manager = api_env.gateway.manager
    ws = await api_env.ws(replay_id)
    old = await ws.receive()

    await ws.disconnect()
    assert manager.connection_count() == 0 and not manager.has_subscribers(replay_id)

    await api_env.advance(6)
    again = await api_env.ws(replay_id)
    snapshot = await again.receive()
    assert manager.connection_count(replay_id) == 1
    assert snapshot["type"] == "SNAPSHOT"
    assert snapshot["run_id"] == old["run_id"] and snapshot["sequence"] > old["sequence"]
    state = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()
    assert snapshot["payload"]["state"] == state


async def test_replay_continues_without_clients(api_env: ApiEnv) -> None:
    replay_id = await running(api_env)
    await api_env.finish()

    replay = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()
    assert replay["status"] == "COMPLETED"
    assert api_env.gateway.manager.connection_count() == 0
    late = await api_env.ws(replay_id)
    snapshot = await late.receive()
    assert snapshot["payload"]["replay"]["status"] == "COMPLETED"
    assert snapshot["payload"]["state"]["phase"] == "COMPLETED"


async def test_all_clients_of_a_replay_receive_the_same_messages(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    first, second = await api_env.ws(replay_id), await api_env.ws(replay_id)
    await first.receive()
    await second.receive()
    await api_env.start(replay_id)
    await api_env.advance(8)

    a, b = await first.messages(), await second.messages()
    assert a and shape(a) == shape(b)
    assert [m["payload"] for m in a if m["type"] != "REPLAY_CLOCK"] == [
        m["payload"] for m in b if m["type"] != "REPLAY_CLOCK"
    ]

    await second.disconnect()
    assert api_env.gateway.manager.connection_count(replay_id) == 1
    await api_env.finish()
    rest = await first.messages()
    assert of_type(rest, "REPLAY_COMPLETED") and of_type(rest, "DETECTED_EVENT")


async def test_replays_are_isolated_from_each_other(api_env: ApiEnv) -> None:
    replay_a = await api_env.create_replay(api_env.race)
    replay_b = await api_env.create_replay(api_env.other_race)
    ws_a, ws_b = await api_env.ws(replay_a), await api_env.ws(replay_b)
    await ws_a.receive()
    await ws_b.receive()

    await api_env.start(replay_a)
    await api_env.start(replay_b)
    await api_env.advance(8)
    await api_env.client.post(f"{API}/replays/{replay_a}/pause")
    api_env.gateway.tick()
    paused_a, paused_b = await ws_a.messages(), await ws_b.messages()
    assert "PAUSED" in [
        m["payload"]["replay"]["status"] for m in of_type(paused_a, "REPLAY_STATUS")
    ]
    assert [m["payload"]["replay"]["status"] for m in of_type(paused_b, "REPLAY_STATUS")] == [
        "RUNNING"
    ]
    await api_env.client.post(f"{API}/replays/{replay_a}/resume")
    await api_env.finish()

    a, b = paused_a + await ws_a.messages(), paused_b + await ws_b.messages()
    assert a and b
    assert {m["replay_id"] for m in a} == {str(replay_a)}
    assert {m["replay_id"] for m in b} == {str(replay_b)}
    runs_a = {m["run_id"] for m in a if m["run_id"]}
    runs_b = {m["run_id"] for m in b if m["run_id"]}
    assert len(runs_a) == len(runs_b) == 1 and runs_a.isdisjoint(runs_b)
    for message in of_type(a, "DETECTED_EVENT"):
        assert message["payload"]["event"]["replay_id"] == str(replay_a)
    sessions_a = {m["payload"]["state"]["session_id"] for m in of_type(a, "RACE_STATE_SNAPSHOT")}
    assert sessions_a == {str(api_env.race.session_id)}


# -- ordering ----------------------------------------------------------------------------------------------


async def test_sequences_never_decrease_and_one_events_messages_keep_their_order(
    api_env: ApiEnv,
) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    await ws.receive()
    await api_env.start(replay_id)
    await api_env.finish()

    state_messages = of_type(await ws.messages(), *STATE_DERIVED_TYPES)
    sequences = [m["sequence"] for m in state_messages]
    assert sequences == sorted(sequences) and len(sequences) > 20
    assert len({m["run_id"] for m in state_messages}) == 1
    groups: dict[int, list[int]] = {}
    for message in state_messages:
        groups.setdefault(message["sequence"], []).append(EVENT_ORDER.index(message["type"]))
    for ranks in groups.values():
        assert ranks == sorted(ranks)


async def test_messages_the_snapshot_already_covers_are_not_delivered(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.timer.advance(8)
    await settle()
    for _ in range(50):  # state consumer only: the gateway tail has not read these yet
        if not await api_env.state_consumer.process_batch():
            break

    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    assert snapshot["sequence"] > 0
    await api_env.drain()  # now the tail delivers entries at or below the snapshot
    await api_env.advance(8)

    stale = [
        m
        for m in await ws.messages()
        if m["type"] in STATE_DERIVED_TYPES and m["sequence"] <= snapshot["sequence"]
    ]
    assert stale == []


# -- backpressure --------------------------------------------------------------------------------------------


def tune(api_env: ApiEnv, **changes: Any) -> None:
    api_env.gateway.config = replace(api_env.gateway.config, **changes)


async def test_slow_client_drops_clock_ticks_but_keeps_the_connection(api_env: ApiEnv) -> None:
    tune(api_env, client_queue_size=4, send_timeout_seconds=30)
    replay_id = await running(api_env)
    gate = asyncio.Event()
    slow = await api_env.ws(replay_id, send_gate=gate)
    await settle()

    for _ in range(20):
        api_env.gateway.tick()
    (connection,) = api_env.gateway.manager._groups[replay_id].values()
    assert connection.queued <= 4 and connection.dropped >= 15 and not connection.closed

    gate.set()
    messages = await slow.messages()
    assert messages[0]["type"] == "SNAPSHOT"
    assert len(of_type(messages, "REPLAY_CLOCK")) <= 4
    assert slow.close_code is None


async def test_slow_client_with_state_overflow_is_dropped_without_affecting_others(
    api_env: ApiEnv,
) -> None:
    tune(api_env, client_queue_size=6, send_timeout_seconds=30)
    replay_id = await api_env.create_replay()
    gate = asyncio.Event()
    slow = await api_env.ws(replay_id, send_gate=gate)
    tune(api_env, client_queue_size=512)  # the queue size is fixed when a client connects
    healthy = await api_env.ws(replay_id)
    await healthy.receive()
    await api_env.start(replay_id)
    await api_env.advance(8)

    connections = list(api_env.gateway.manager._groups[replay_id].values())
    assert sorted(c.closed for c in connections) == [False, True]  # the slow one was dropped
    gate.set()
    sent, code = await slow.until_closed()
    assert code == 4408
    await slow.finished()
    assert api_env.gateway.manager.connection_count(replay_id) == 1
    assert sent[-1]["type"] == "ERROR" and sent[-1]["payload"]["code"] == "CLIENT_TOO_SLOW"

    await api_env.finish()
    received = await healthy.messages()
    assert of_type(received, "REPLAY_COMPLETED")
    assert len(of_type(received, "DETECTED_EVENT")) == 2
    replay = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()
    assert replay["status"] == "COMPLETED"
    assert not healthy.done


async def test_send_timeout_disconnects_a_stuck_client(api_env: ApiEnv) -> None:
    tune(api_env, send_timeout_seconds=0.05)
    replay_id = await api_env.create_replay()
    stuck = await api_env.ws(replay_id, send_gate=asyncio.Event())

    await stuck.finished()

    assert stuck.close_code == 4408
    assert api_env.gateway.manager.connection_count() == 0


async def test_failing_client_is_cleaned_up_and_the_replay_is_unaffected(
    api_env: ApiEnv,
) -> None:
    replay_id = await api_env.create_replay()
    broken = await api_env.ws(replay_id, fail_sends=True)
    healthy = await api_env.ws(replay_id)
    await healthy.receive()

    await broken.finished()
    assert api_env.gateway.manager.connection_count(replay_id) == 1

    await api_env.start(replay_id)
    await api_env.finish()
    assert of_type(await healthy.messages(), "REPLAY_COMPLETED")
    replay = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()
    assert replay["status"] == "COMPLETED"


# -- fan-out without consumer groups ----------------------------------------------------------------------------


async def test_independent_stream_tails_each_see_every_message(api_env: ApiEnv) -> None:
    seen: dict[str, list[tuple[str, int]]] = {"first": [], "second": []}

    async def collect(name: str, stream: str, event: Any) -> None:
        seen[name].append((stream, event.sequence))

    tails = {
        name: StreamTail(
            api_env.redis,
            api_env.stream_config,
            api_env.gateway.config,
            lambda stream, event, name=name: collect(name, stream, event),
        )
        for name in seen
    }
    for tail in tails.values():
        await tail.read_once()  # position at the stream tails

    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.finish()
    for tail in tails.values():
        while await tail.read_once():
            pass

    assert seen["first"] and seen["first"] == seen["second"]
    # Plain XREAD: the gateway tails create no consumer group of their own.
    state_groups = await api_env.redis.client.xinfo_groups(api_env.stream_config.state_stream)
    assert len(state_groups) == 1  # the detection engine's
    for stream in (api_env.stream_config.state_stream, api_env.stream_config.detected_stream):
        names = [g["name"] for g in await api_env.redis.client.xinfo_groups(stream)]
        assert not any("gateway" in n or n.startswith("ws") for n in names)
