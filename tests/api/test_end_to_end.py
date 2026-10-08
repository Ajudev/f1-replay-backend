"""One replay through every stage to a WebSocket client, checked against the REST state.

REST create/start → replay engine → raw stream → race state consumer → detection
consumer → state / detected streams → gateway → WebSocket. The client rebuilds the race
state from the SNAPSHOT plus the DRIVER_UPDATE / RACE_STATE_UPDATE / LAP_COMPLETED
deltas and must end up with exactly what ``GET /state`` returns.
"""

from __future__ import annotations

import copy
from typing import Any

from tests.api.conftest import API, ApiEnv


class ClientState:
    """What a frontend does: keep the snapshot, apply deltas."""

    def __init__(self, snapshot: dict[str, Any]) -> None:
        self.state = copy.deepcopy(snapshot["payload"]["state"])
        self.run_id = snapshot["run_id"]
        self.last_sequence = snapshot["sequence"]
        self.detected: list[dict[str, Any]] = []

    def apply(self, message: dict[str, Any]) -> None:
        kind, payload = message["type"], message["payload"]
        if message["type"] == "DETECTED_EVENT":
            self.detected.append(payload["event"])
            return
        if "sequence" not in message or message["sequence"] is None or message["run_id"] is None:
            return
        assert message["run_id"] == self.run_id
        assert message["sequence"] >= self.last_sequence
        if kind == "RACE_STATE_SNAPSHOT":
            self.state = copy.deepcopy(payload["state"])
        elif kind == "RACE_STATE_UPDATE":
            self.state.update(payload["race"])
        elif kind == "DRIVER_UPDATE":
            incoming = payload["driver"]
            current = next(
                d for d in self.state["drivers"] if d["driver_id"] == incoming["driver_id"]
            )
            current.update(incoming)  # recent_laps travel via LAP_COMPLETED
        elif kind == "LAP_COMPLETED":
            current = next(
                d for d in self.state["drivers"] if d["driver_id"] == payload["driver_id"]
            )
            # Keyed by lap number: the final state event carries the lap both in its
            # RACE_STATE_SNAPSHOT and in a LAP_COMPLETED, so clients must upsert.
            laps = {lap["lap_number"]: lap for lap in current["recent_laps"]}
            laps[payload["lap"]["lap_number"]] = payload["lap"]
            current["recent_laps"] = [laps[n] for n in sorted(laps)]
        else:
            return
        self.last_sequence = message["sequence"]


def comparable(state: dict[str, Any]) -> dict[str, Any]:
    """The state without bookkeeping that legitimately differs (replay status, timestamps)."""
    document = copy.deepcopy(state)
    for key in ("replay_status", "source", "updated_at", "last_event_id", "last_sequence"):
        document.pop(key, None)
    document["drivers"] = sorted(document["drivers"], key=lambda d: d["driver_id"])
    for driver in document["drivers"]:
        driver["recent_laps"] = [lap["lap_number"] for lap in driver["recent_laps"]]
    return document


async def test_state_rebuilt_from_websocket_equals_rest_state(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    created = await api_env.client.get(f"{API}/replays/{replay_id}")
    assert created.json()["status"] == "CREATED"

    await api_env.start(replay_id)
    await api_env.advance(7)  # mid-race
    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    mid_race = snapshot["payload"]["state"]
    assert mid_race is not None and mid_race["phase"] == "RUNNING"
    assert 0 < mid_race["last_sequence"] < mid_race["total_events"] - 1

    client = ClientState(snapshot)
    await api_env.finish()
    messages = await ws.messages()
    for message in messages:
        client.apply(message)

    replay = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()
    rest = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()
    assert replay["status"] == "COMPLETED" and replay["is_completed"] is True
    assert rest["phase"] == "COMPLETED" and rest["last_sequence"] == replay["current_sequence"]
    assert any(m["type"] == "REPLAY_COMPLETED" for m in messages)

    assert client.last_sequence == rest["last_sequence"]
    # Bookkeeping (replay_status, timestamps) is excluded by ``comparable``.
    assert comparable(client.state) == comparable(rest)
    assert [d["abbreviation"] for d in rest["drivers"]] == ["NOR", "VER", "HAM"]

    detected_rest = (await api_env.client.get(f"{API}/replays/{replay_id}/events")).json()
    seen_ids = {e["detected_event_id"] for e in client.detected}
    after_snapshot = [
        e for e in detected_rest["items"] if e["source_sequence"] > mid_race["last_sequence"]
    ]
    assert after_snapshot and {e["detected_event_id"] for e in after_snapshot} <= seen_ids


async def test_a_client_connected_from_the_start_sees_the_whole_race(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    first = await ws.receive()
    assert first["payload"]["state"] is None

    await api_env.start(replay_id)
    await api_env.finish()
    messages = await ws.messages()

    bootstrap = next(m for m in messages if m["type"] == "RACE_STATE_SNAPSHOT")
    client = ClientState(
        {
            "payload": {"state": bootstrap["payload"]["state"]},
            "run_id": bootstrap["run_id"],
            "sequence": bootstrap["sequence"],
        }
    )
    for message in messages:
        if message["sequence"] is not None and message["sequence"] > bootstrap["sequence"]:
            client.apply(message)

    rest = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()
    assert comparable(client.state) == comparable(rest)
    rest_events = (await api_env.client.get(f"{API}/replays/{replay_id}/events")).json()
    assert sorted(e["detected_event_id"] for e in client.detected) == sorted(
        e["detected_event_id"] for e in rest_events["items"]
    )
