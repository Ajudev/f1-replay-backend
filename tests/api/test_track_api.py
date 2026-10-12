"""Circuit map API: track data import, circuit layout, telemetry traces, battles, CAR_FRAME."""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from app.api.track import get_track_data_loader
from app.domain.enums import SessionType
from app.ingestion.records import (
    CarSeries,
    ExtractedCircuit,
    ExtractedCorner,
    ExtractedDriverTrack,
    ExtractedTrackData,
    PositionSeries,
)
from app.models import Driver, DriverTrackSample
from app.positions.track import PositionTrack
from tests.api.conftest import API, ApiEnv, settle
from tests.timeline.factories import EPOCH, representative_race
from tests.timeline.seed import seed_source

#: Fake data covers race time -5 s .. 460 s (the representative race is 5 x 90 s).
POS_TIMES = list(range(EPOCH - 5_000, EPOCH + 460_000, 250))
CAR_TIMES = list(range(EPOCH - 5_000, EPOCH + 460_000, 230))
X_OFFSET = {"VER": 0, "HAM": 10_000, "NOR": 20_000}


class FakeTrackLoader:
    def __init__(self, *, positions: bool = True) -> None:
        self.positions = positions
        self.calls = 0

    def load_track_data(
        self, season: int, event: int | str, session_type: SessionType
    ) -> ExtractedTrackData:
        self.calls += 1
        drivers = []
        for abbr, offset in X_OFFSET.items():
            n = len(CAR_TIMES)
            drivers.append(
                ExtractedDriverTrack(
                    abbreviation=abbr,
                    driver_number=None,
                    team_color="3671c6" if abbr == "VER" else None,
                    positions=PositionSeries(
                        session_time_ms=POS_TIMES,
                        x=[offset + (t - EPOCH) // 100 for t in POS_TIMES],
                        y=[0] * len(POS_TIMES),
                        off_track=[False] * len(POS_TIMES),
                    )
                    if self.positions
                    else None,
                    car=CarSeries(
                        session_time_ms=CAR_TIMES,
                        speed_kph=[(t - EPOCH) // 1000 for t in CAR_TIMES],
                        rpm=[11_000] * n,
                        gear=[7] * n,
                        throttle_pct=[100] * n,
                        brake=[False] * n,
                        drs_code=[12] * n,
                    ),
                )
            )
        return ExtractedTrackData(
            drivers=drivers,
            circuit=ExtractedCircuit(
                rotation_deg=90.0, corners=[ExtractedCorner(1, "", 100.0, 200.0, 45.0)]
            ),
        )


@pytest.fixture
def loader(api_env: ApiEnv) -> FakeTrackLoader:
    fake = FakeTrackLoader()
    api_env.app.dependency_overrides[get_track_data_loader] = lambda: fake
    return fake


async def import_track(env: ApiEnv, session_id: UUID | None = None) -> dict[str, Any]:
    sid = session_id or env.race.session_id
    response = await env.client.post(f"{API}/sessions/{sid}/track-data")
    assert response.status_code == 201, response.text
    return response.json()


async def sample_count(env: ApiEnv) -> int:
    async with env.factory() as db:
        return int(
            await db.scalar(
                select(func.count())
                .select_from(DriverTrackSample)
                .where(DriverTrackSample.session_id == env.race.session_id)
            )
            or 0
        )


# -- import -----------------------------------------------------------------------------------


async def test_import_is_idempotent_and_reports_status(
    api_env: ApiEnv, loader: FakeTrackLoader
) -> None:
    sid = api_env.race.session_id
    status = (await api_env.client.get(f"{API}/sessions/{sid}/track-data/status")).json()
    assert status["status"] == "NOT_IMPORTED"

    first = await import_track(api_env)
    assert first["status"] == "imported"
    assert first["driver_count"] == 3
    assert first["position_sample_count"] > 0 and first["telemetry_sample_count"] > 0
    count = await sample_count(api_env)
    assert count > 0

    second = await import_track(api_env)
    assert second["status"] == "replaced"
    assert await sample_count(api_env) == count
    assert loader.calls == 2

    status = (await api_env.client.get(f"{API}/sessions/{sid}/track-data/status")).json()
    assert status["status"] == "IMPORTED"
    assert status["position_sample_count"] == first["position_sample_count"]
    assert status["sample_interval_ms"] == 500

    async with api_env.factory() as db:
        colours = dict(
            (
                await db.execute(
                    select(Driver.abbreviation, Driver.team_color).where(Driver.session_id == sid)
                )
            ).all()
        )
    assert colours == {"VER": "3671c6", "HAM": None, "NOR": None}


async def test_import_errors(api_env: ApiEnv) -> None:
    api_env.app.dependency_overrides[get_track_data_loader] = lambda: FakeTrackLoader(
        positions=False
    )
    unknown = await api_env.client.post(f"{API}/sessions/{uuid4()}/track-data")
    assert (unknown.status_code, unknown.json()["code"]) == (404, "SESSION_NOT_FOUND")

    no_timeline = replace(representative_race(), season=2022, round=3)
    await seed_source(api_env.factory, no_timeline)
    response = await api_env.client.post(f"{API}/sessions/{no_timeline.session_id}/track-data")
    assert (response.status_code, response.json()["code"]) == (404, "TIMELINE_NOT_GENERATED")

    response = await api_env.client.post(f"{API}/sessions/{api_env.race.session_id}/track-data")
    assert (response.status_code, response.json()["code"]) == (422, "TRACK_DATA_UNAVAILABLE")
    assert await sample_count(api_env) == 0


# -- circuit ----------------------------------------------------------------------------------


async def test_circuit_layout(api_env: ApiEnv, loader: FakeTrackLoader) -> None:
    sid = api_env.race.session_id
    missing = await api_env.client.get(f"{API}/sessions/{sid}/circuit")
    assert (missing.status_code, missing.json()["code"]) == (404, "TRACK_DATA_NOT_IMPORTED")
    unknown = await api_env.client.get(f"{API}/sessions/{uuid4()}/circuit")
    assert unknown.json()["code"] == "SESSION_NOT_FOUND"

    await import_track(api_env)
    body = (await api_env.client.get(f"{API}/sessions/{sid}/circuit")).json()
    assert set(body) == {
        "session_id",
        "circuit_name",
        "rotation_deg",
        "bounds",
        "outline",
        "corners",
        "sample_interval_ms",
        "warnings",
    }
    assert body["rotation_deg"] == 90.0
    assert body["corners"] == [{"number": 1, "letter": "", "x": 100, "y": 200, "angle_deg": 45.0}]
    assert len(body["outline"]) > 50 and all(len(p) == 2 for p in body["outline"])
    assert body["bounds"]["min_x"] <= body["outline"][0][0] <= body["bounds"]["max_x"]

    replay_id = await api_env.create_replay()
    via_replay = (await api_env.client.get(f"{API}/replays/{replay_id}/circuit")).json()
    assert via_replay == body


# -- telemetry trace --------------------------------------------------------------------------


async def test_lap_telemetry_never_reveals_the_future(
    api_env: ApiEnv, loader: FakeTrackLoader
) -> None:
    replay_id = await api_env.create_replay()  # speed 20
    url = f"{API}/replays/{replay_id}/drivers/ver/telemetry"
    missing = await api_env.client.get(url, params={"lap": 1})
    assert missing.json()["code"] == "TRACK_DATA_NOT_IMPORTED"

    await import_track(api_env)
    await api_env.start(replay_id)
    await api_env.advance(6)  # race time 120 s: lap 1 done, lap 2 in progress
    now = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()["current_race_time_ms"]
    assert 90_000 < now < 180_000

    lap1 = (await api_env.client.get(url, params={"lap": 1})).json()
    assert lap1["abbreviation"] == "VER" and lap1["complete"] is True
    assert lap1["samples"][0]["drs"] == "OPEN"
    assert all(0 <= s["race_time_ms"] <= 90_000 for s in lap1["samples"])

    lap2 = (await api_env.client.get(url, params={"lap": 2})).json()
    assert lap2["complete"] is False
    assert lap2["samples"] and max(s["race_time_ms"] for s in lap2["samples"]) <= now

    future = await api_env.client.get(url, params={"lap": 4})
    assert (future.status_code, future.json()["code"]) == (409, "LAP_NOT_REACHED")

    empty = (await api_env.client.get(url, params={"lap": 50})).json()
    assert empty["samples"] == [] and empty["warnings"]

    unknown = await api_env.client.get(
        f"{API}/replays/{replay_id}/drivers/XYZ/telemetry", params={"lap": 1}
    )
    assert unknown.json()["code"] == "DRIVER_NOT_FOUND"


# -- battles ----------------------------------------------------------------------------------


async def test_battles_endpoint(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    body = (await api_env.client.get(f"{API}/replays/{replay_id}/battles")).json()
    assert body == {"replay_id": str(replay_id), "run_id": None, "battles": []}

    await api_env.start(replay_id)
    await api_env.advance(10)
    body = (await api_env.client.get(f"{API}/replays/{replay_id}/battles")).json()
    assert body["run_id"] is not None and body["battles"] == []

    unknown = await api_env.client.get(f"{API}/replays/{uuid4()}/battles")
    assert unknown.json()["code"] == "REPLAY_NOT_FOUND"


# -- WebSocket CAR_FRAME ----------------------------------------------------------------------


def frames(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [m for m in messages if m["type"] == "CAR_FRAME"]


async def test_car_frames_follow_the_replay_clock(api_env: ApiEnv, loader: FakeTrackLoader) -> None:
    await import_track(api_env)
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    assert snapshot["type"] == "SNAPSHOT"
    assert snapshot["payload"]["positions_available"] is True
    assert snapshot["payload"]["telemetry_available"] is True
    assert snapshot["payload"]["battles"] == []

    # Not started: one frame at race time 0 so the map shows the grid.
    frame = await ws.receive()
    assert frame["type"] == "CAR_FRAME"
    assert frame["race_time_ms"] == 0
    cars = frame["payload"]["cars"]
    assert len(cars) == 3
    assert {c["x"] for c in cars} == {0, 10_000, 20_000}
    assert cars[0]["telemetry"]["drs"] == "OPEN"

    await api_env.gateway.position_tick()  # not running: nothing
    assert frames(await ws.messages()) == []

    await api_env.start(replay_id)
    await api_env.advance(3)
    await api_env.gateway.position_tick()
    [moving] = frames(await ws.messages())
    t = moving["payload"]["race_time_ms"]
    assert t > 0
    ver = next(c for c in moving["payload"]["cars"] if c["x"] < 10_000)
    assert ver["x"] == t // 100

    await api_env.client.post(f"{API}/replays/{replay_id}/pause")
    await settle()
    assert len(frames(await ws.messages())) == 1  # one frame on pause


async def test_a_frame_is_computed_once_per_replay(
    api_env: ApiEnv, loader: FakeTrackLoader, monkeypatch: pytest.MonkeyPatch
) -> None:
    await import_track(api_env)
    replay_id = await api_env.create_replay()
    a, b = await api_env.ws(replay_id), await api_env.ws(replay_id)
    for ws in (a, b):
        assert (await ws.receive())["type"] == "SNAPSHOT"
        assert (await ws.receive())["type"] == "CAR_FRAME"
    await api_env.start(replay_id)
    await api_env.advance(1)

    await a.messages(), await b.messages()
    calls = 0
    original = PositionTrack.frame_at

    def counting(self: PositionTrack, t: int) -> Any:
        nonlocal calls
        calls += 1
        return original(self, t)

    monkeypatch.setattr(PositionTrack, "frame_at", counting)
    await api_env.gateway.position_tick()
    assert calls == 1
    assert frames(await a.messages()) == frames(await b.messages())


async def test_no_frames_without_track_data(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    ws = await api_env.ws(replay_id)
    snapshot = await ws.receive()
    assert snapshot["payload"]["positions_available"] is False
    assert frames(await ws.messages()) == []
    await api_env.start(replay_id)
    await api_env.advance(1)
    await api_env.gateway.position_tick()
    assert frames(await ws.messages()) == []


async def test_reimport_invalidates_the_cached_track(
    api_env: ApiEnv, loader: FakeTrackLoader
) -> None:
    sid = api_env.race.session_id
    cache = api_env.app.state.position_cache
    assert await cache.get(sid) is None  # cached as "no data"
    await import_track(api_env)
    track = await cache.get(sid)
    assert track is not None and not track.empty
    await import_track(api_env)
    assert await cache.get(sid) is not track
