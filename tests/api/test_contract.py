"""Frontend contract: pinned response shapes, no internal leakage, OpenAPI coverage.

These tests fail when a field is added, removed or retyped, so contract changes are
deliberate (and the README / schema version updated with them).
"""

from __future__ import annotations

import json
import re
from typing import Any
from uuid import uuid4

from fastapi.routing import APIRoute, APIWebSocketRoute

from app.api.exceptions import ErrorResponse
from app.api.router import API_V1_PREFIX, OPENAPI_TAGS
from tests.api.conftest import API, ApiEnv

REPLAY_FIELDS = {
    "id": str,
    "session_id": str,
    "race_id": str,
    "status": str,
    "is_completed": bool,
    "status_reason": (str, type(None)),
    "playback_speed": float,
    "current_race_time_ms": int,
    "current_sequence": (int, type(None)),
    "emitted_event_count": int,
    "total_events": (int, type(None)),
    "current_lap": (int, type(None)),
    "total_laps": (int, type(None)),
    "created_at": str,
    "started_at": (str, type(None)),
    "paused_at": (str, type(None)),
    "ended_at": (str, type(None)),
}
STATE_FIELDS = {
    "replay_id",
    "replay_status",
    "source",
    "run_id",
    "session_id",
    "race_id",
    "season",
    "round",
    "session_type",
    "phase",
    "current_race_time_ms",
    "current_lap",
    "total_laps",
    "leader_laps_completed",
    "leader_driver_id",
    "leader_driver_abbreviation",
    "track_status",
    "fastest_lap",
    "last_sequence",
    "last_event_id",
    "total_events",
    "updated_at",
    "drivers",
}
DRIVER_FIELDS = {
    "driver_id",
    "abbreviation",
    "driver_number",
    "full_name",
    "team_name",
    "grid_position",
    "position",
    "previous_position",
    "laps_completed",
    "current_lap",
    "last_lap_time_ms",
    "last_lap_race_time_ms",
    "best_lap_time_ms",
    "best_lap_number",
    "gap_to_leader_ms",
    "interval_to_ahead_ms",
    "gap_basis",
    "laps_behind_leader",
    "compound",
    "tyre_age_laps",
    "stint_number",
    "tyre_info_lap",
    "pit_status",
    "pit_stop_count",
    "last_pit_entry_lap",
    "last_pit_entry_race_time_ms",
    "last_pit_exit_race_time_ms",
    "last_pit_lane_duration_ms",
    "race_status",
    "recent_laps",
}
LAP_RECORD_FIELDS = {
    "lap_number",
    "lap_time_ms",
    "position",
    "compound",
    "tyre_age_laps",
    "stint_number",
    "is_pit_in_lap",
    "is_pit_out_lap",
    "is_deleted",
    "is_accurate",
    "track_status",
    "race_time_ms",
    "completion_time_source",
}
DETECTED_EVENT_FIELDS = {
    "detected_event_id",
    "event_type",
    "schema_version",
    "replay_id",
    "run_id",
    "session_id",
    "race_id",
    "race_time_ms",
    "lap_number",
    "primary_driver_id",
    "primary_driver_abbreviation",
    "secondary_driver_id",
    "secondary_driver_abbreviation",
    "severity",
    "confidence",
    "evidence",
    "source_event_ids",
    "source_sequence",
    "detector_name",
    "detector_version",
    "detected_at",
}
TIMING_POINT_FIELDS = {
    "lap_number",
    "race_time_ms",
    "lap_time_ms",
    "position",
    "gap_to_leader_ms",
    "compound",
    "tyre_age_laps",
    "stint_number",
    "is_pit_in_lap",
    "is_pit_out_lap",
    "is_deleted",
    "track_status",
    "sectors",
}
ENVELOPE_FIELDS = {
    "type",
    "schema_version",
    "replay_id",
    "run_id",
    "sequence",
    "race_time_ms",
    "lap_number",
    "emitted_at",
    "payload",
}
#: Strings that must never reach a client.
FORBIDDEN = (
    "lap_crossings",
    "run_published_at",
    "consumer_group",
    "race:replay:",
    "race.state.events",
    "race.detected.events",
    "race.raw.events",
    "stream_id",
    "redis",
)


def assert_clean(document: Any) -> None:
    """No internal identifiers and no NaN/Infinity in the serialized JSON."""
    text = json.dumps(document, allow_nan=False)  # raises on NaN / Infinity
    for token in FORBIDDEN:
        assert token not in text, f"internal detail {token!r} leaked"


async def played(api_env: ApiEnv) -> str:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.finish()
    return str(replay_id)


# -- HTTP responses ---------------------------------------------------------------------------------


async def test_replay_response_contract(api_env: ApiEnv) -> None:
    replay_id = await played(api_env)

    body = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()

    assert set(body) == set(REPLAY_FIELDS)
    for name, expected in REPLAY_FIELDS.items():
        assert isinstance(body[name], expected), (name, body[name])
    assert body["status"] == "COMPLETED"
    assert_clean(body)
    created = (
        await api_env.client.post(
            f"{API}/replays", json={"session_id": str(api_env.race.session_id)}
        )
    ).json()
    assert set(created) == set(REPLAY_FIELDS)


async def test_race_state_contract(api_env: ApiEnv) -> None:
    replay_id = await played(api_env)

    body = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()

    assert set(body) == STATE_FIELDS
    assert {"abbreviation", "driver_id", "lap_number", "lap_time_ms", "race_time_ms"} <= set(
        body["fastest_lap"]
    )
    for driver in body["drivers"]:
        assert set(driver) == DRIVER_FIELDS
        assert isinstance(driver["position"], int)
        assert isinstance(driver["pit_status"], str) and isinstance(driver["race_status"], str)
        for lap in driver["recent_laps"]:
            assert set(lap) == LAP_RECORD_FIELDS
    assert_clean(body)

    one = (await api_env.client.get(f"{API}/replays/{replay_id}/drivers/VER")).json()
    assert set(one) == {
        "replay_id",
        "replay_status",
        "source",
        "phase",
        "current_race_time_ms",
        "current_lap",
        "last_sequence",
        "driver",
    }
    assert set(one["driver"]) == DRIVER_FIELDS
    assert_clean(one)


async def test_detected_event_contract(api_env: ApiEnv) -> None:
    replay_id = await played(api_env)

    page = (await api_env.client.get(f"{API}/replays/{replay_id}/events")).json()

    assert set(page) == {"replay_id", "run_id", "items", "total", "limit", "offset"}
    assert page["items"]
    for item in page["items"]:
        assert set(item) == DETECTED_EVENT_FIELDS
        assert isinstance(item["evidence"], dict) and isinstance(item["source_event_ids"], list)
    assert_clean(page)


async def test_timing_contract(api_env: ApiEnv) -> None:
    replay_id = await played(api_env)

    body = (await api_env.client.get(f"{API}/replays/{replay_id}/timing")).json()

    assert set(body) == {
        "replay_id",
        "session_id",
        "upto_sequence",
        "lap_from",
        "lap_to",
        "drivers",
    }
    for series in body["drivers"]:
        assert set(series) == {"driver_id", "abbreviation", "points"}
        assert series["points"]
        for point in series["points"]:
            assert set(point) == TIMING_POINT_FIELDS
            assert isinstance(point["lap_number"], int) and isinstance(point["race_time_ms"], int)
    assert_clean(body)


async def test_race_catalog_contract_has_no_internal_fields(api_env: ApiEnv) -> None:
    races = (await api_env.client.get(f"{API}/races")).json()
    assert_clean(races)
    race_id = races["items"][0]["id"] if isinstance(races, dict) else races[0]["id"]
    assert_clean((await api_env.client.get(f"{API}/races/{race_id}")).json())
    assert_clean((await api_env.client.get(f"{API}/races/{race_id}/drivers")).json())


async def test_error_contract(api_env: ApiEnv) -> None:
    responses = [
        await api_env.client.get(f"{API}/replays/{uuid4()}"),  # 404 domain error
        await api_env.client.get(f"{API}/replays/not-a-uuid"),  # 422 validation
        await api_env.client.post(
            f"{API}/replays", json={"session_id": str(api_env.race.session_id), "playback_speed": 3}
        ),  # unsupported speed
        await api_env.client.get(f"{API}/nope"),  # 404 routing
        await api_env.client.delete(f"{API}/replays"),  # 405
    ]

    for response in responses:
        assert response.status_code >= 400
        body = response.json()
        assert set(body) == {"code", "message", "details"}, body
        assert isinstance(body["code"], str) and isinstance(body["message"], str)
        assert body["details"] is None or isinstance(body["details"], dict)
        assert set(ErrorResponse.model_fields) == {"code", "message", "details"}
        assert "Traceback" not in response.text and "sqlalchemy" not in response.text.lower()
    assert responses[1].json()["code"] == "VALIDATION_ERROR"
    assert isinstance(responses[1].json()["details"]["errors"], list)


# -- WebSocket envelope ---------------------------------------------------------------------------------


async def test_websocket_envelope_contract(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    await api_env.start(replay_id)
    await api_env.advance(3)
    api_env.gateway.tick()
    await api_env.finish()
    messages = [snapshot, *await ws.messages()]

    assert {m["type"] for m in messages} >= {"SNAPSHOT", "REPLAY_CLOCK", "DETECTED_EVENT"}
    uuid = re.compile(r"^[0-9a-f-]{36}$")
    for message in messages:
        assert set(message) == ENVELOPE_FIELDS, message["type"]
        assert message["schema_version"] == 1 and isinstance(message["payload"], dict)
        assert uuid.match(message["replay_id"])
        assert message["run_id"] is None or uuid.match(message["run_id"])
        assert message["sequence"] is None or isinstance(message["sequence"], int)
        assert message["emitted_at"].endswith("Z") or "+" in message["emitted_at"]
        assert_clean(message)


async def test_snapshot_state_matches_the_rest_contract(api_env: ApiEnv) -> None:
    replay_id = await played(api_env)

    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()

    assert set(snapshot["payload"]) == {"replay", "state", "state_error"}
    assert set(snapshot["payload"]["replay"]) == set(REPLAY_FIELDS)
    assert set(snapshot["payload"]["state"]) == STATE_FIELDS


# -- OpenAPI ---------------------------------------------------------------------------------------------------


def flat_routes(routes: Any) -> list[Any]:
    """Routes of the app with included routers expanded (FastAPI includes them lazily)."""
    flat: list[Any] = []
    for route in routes:
        original = getattr(route, "original_router", None)
        flat.extend(flat_routes(original.routes) if original is not None else [route])
    return flat


async def test_openapi_describes_the_versioned_api(api_env: ApiEnv) -> None:
    spec = (await api_env.client.get("/openapi.json")).json()
    app = api_env.app

    assert {t["name"] for t in spec["tags"]} == {t["name"] for t in OPENAPI_TAGS}
    for path in spec["paths"]:
        assert path.startswith(f"{API_V1_PREFIX}/") or path in {"/health", "/ready"}, path
    assert API_V1_PREFIX == "/api/v1"
    assert "/health" in spec["paths"] and "/ready" in spec["paths"]

    used_tags: set[str] = set()
    for route in flat_routes(app.routes):
        if not isinstance(route, APIRoute):
            continue
        path = next(p for p in (route.path, API_V1_PREFIX + route.path) if p in spec["paths"])
        operation = spec["paths"][path][sorted(route.methods)[0].lower()]
        assert operation.get("summary"), route.path
        assert operation["tags"], route.path
        used_tags.update(operation["tags"])
        success = [c for c in operation["responses"] if c.startswith("2")]
        assert success, route.path
        if route.response_model is None:
            raise AssertionError(f"{route.path} declares no response model")
        error_codes = [c for c in operation["responses"] if c[0] in "45" and c != "422"]
        for code in error_codes:
            schema = operation["responses"][code]["content"]["application/json"]["schema"]
            assert schema["$ref"].endswith("/ErrorResponse"), (route.path, code)
    assert (
        used_tags <= {t["name"] for t in OPENAPI_TAGS}
        and {
            "Races",
            "Replays",
            "Race State",
            "Events",
            "Timing",
            "Data Management",
            "Health",
        }
        <= used_tags
    )


async def test_websocket_route_is_registered(api_env: ApiEnv) -> None:
    websocket_paths = [
        r.path for r in flat_routes(api_env.app.routes) if isinstance(r, APIWebSocketRoute)
    ]
    assert websocket_paths == ["/replays/{replay_id}/stream"]  # mounted under API_V1_PREFIX


async def test_validation_errors_are_documented_with_the_error_shape(api_env: ApiEnv) -> None:
    spec = (await api_env.client.get("/openapi.json")).json()
    operation = spec["paths"][f"{API}/replays"]["post"]
    schema = operation["responses"]["422"]["content"]["application/json"]["schema"]
    assert schema["$ref"].endswith("/ErrorResponse")
