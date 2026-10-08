"""Replay-scoped read endpoints: race state, driver detail, detected events, timing."""

from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import select

from app.models import Lap, Sector
from tests.api.conftest import API, ApiEnv


async def started(api_env: ApiEnv, seconds: float = 6) -> UUID:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)
    await api_env.advance(seconds)
    return replay_id


# -- race state -----------------------------------------------------------------------------


async def test_state_drivers_are_ordered_by_position(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)

    body = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()

    assert body["replay_status"] == "RUNNING" and body["source"] == "live"
    positions = [d["position"] for d in body["drivers"] if d["position"] is not None]
    assert positions == sorted(positions)
    assert body["last_sequence"] >= 0 and body["total_laps"] == 5


async def test_state_of_a_created_replay_is_409(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    response = await api_env.client.get(f"{API}/replays/{replay_id}/state")
    assert response.status_code == 409 and response.json()["code"] == "REPLAY_NOT_STARTED"


async def test_state_without_redis_state_is_404(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    await api_env.start(replay_id)  # started, but the state worker has processed nothing
    response = await api_env.client.get(f"{API}/replays/{replay_id}/state")
    assert response.status_code == 404 and response.json()["code"] == "RACE_STATE_UNAVAILABLE"


async def test_completed_replay_keeps_its_final_state(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)
    await api_env.finish()

    replay = (await api_env.client.get(f"{API}/replays/{replay_id}")).json()
    state = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()

    assert replay["status"] == "COMPLETED" and replay["is_completed"] is True
    assert state["replay_status"] == "COMPLETED" and state["phase"] == "COMPLETED"
    assert state["last_sequence"] == replay["current_sequence"]


async def test_driver_detail(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)

    response = await api_env.client.get(f"{API}/replays/{replay_id}/drivers/ver")

    assert response.status_code == 200
    driver = response.json()["driver"]
    assert driver["abbreviation"] == "VER"
    for key in (
        "position",
        "gap_to_leader_ms",
        "interval_to_ahead_ms",
        "current_lap",
        "last_lap_time_ms",
        "best_lap_time_ms",
        "compound",
        "tyre_age_laps",
        "stint_number",
        "pit_status",
        "recent_laps",
    ):
        assert key in driver
    unknown = await api_env.client.get(f"{API}/replays/{replay_id}/drivers/XXX")
    assert unknown.status_code == 404 and unknown.json()["code"] == "DRIVER_NOT_FOUND"
    malformed = await api_env.client.get(f"{API}/replays/{replay_id}/drivers/a%20b")
    assert malformed.status_code == 422


# -- detected events ------------------------------------------------------------------------


async def test_detected_event_by_id_matches_the_list(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)
    await api_env.finish()

    page = (await api_env.client.get(f"{API}/replays/{replay_id}/events")).json()
    assert page["total"] >= 1
    first = page["items"][0]
    assert isinstance(first["evidence"], dict)

    one = await api_env.client.get(f"{API}/replays/{replay_id}/events/{first['detected_event_id']}")
    assert one.status_code == 200 and one.json() == first

    missing = await api_env.client.get(f"{API}/replays/{replay_id}/events/{uuid4()}")
    assert missing.status_code == 404 and missing.json()["code"] == "DETECTED_EVENT_NOT_FOUND"
    other_replay = await api_env.client.get(
        f"{API}/replays/{uuid4()}/events/{first['detected_event_id']}"
    )
    assert other_replay.status_code == 404 and other_replay.json()["code"] == "REPLAY_NOT_FOUND"


async def test_event_query_validation(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()
    url = f"{API}/replays/{replay_id}/events"
    for params in ({"lap_from": 3, "lap_to": 1}, {"event_type": "NOPE"}, {"limit": 0}):
        response = await api_env.client.get(url, params=params)
        assert response.status_code == 422 and response.json()["code"] == "VALIDATION_ERROR"


# -- timing ------------------------------------------------------------------------------------


async def test_timing_before_start_is_empty(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()

    body = (await api_env.client.get(f"{API}/replays/{replay_id}/timing")).json()

    assert body["upto_sequence"] is None
    assert [d["abbreviation"] for d in body["drivers"]] == ["HAM", "NOR", "VER"]
    assert all(d["points"] == [] for d in body["drivers"])


async def test_timing_only_contains_laps_the_replay_released(api_env: ApiEnv) -> None:
    replay_id = await started(api_env, seconds=12)

    timing = (await api_env.client.get(f"{API}/replays/{replay_id}/timing")).json()
    state = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()

    completed = {d["abbreviation"]: d["laps_completed"] for d in state["drivers"]}
    assert 0 < sum(completed.values()) < 15  # mid-race
    assert timing["upto_sequence"] == state["last_sequence"]
    for series in timing["drivers"]:
        laps = [p["lap_number"] for p in series["points"]]
        assert laps == list(range(1, completed[series["abbreviation"]] + 1))


async def test_timing_points_carry_gaps_positions_and_tyres(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)
    await api_env.finish()

    body = (await api_env.client.get(f"{API}/replays/{replay_id}/timing")).json()

    by_driver = {d["abbreviation"]: d["points"] for d in body["drivers"]}
    for lap in range(1, 6):
        gaps = [points[lap - 1]["gap_to_leader_ms"] for points in by_driver.values()]
        assert min(gaps) == 0 and all(g >= 0 for g in gaps)
        # The gap is measured from the first crossing of the lap; ``position`` is the
        # source's lap-end classification, which need not match crossing order.
        crossing_leader = min(by_driver, key=lambda a: by_driver[a][lap - 1]["race_time_ms"])
        assert by_driver[crossing_leader][lap - 1]["gap_to_leader_ms"] == 0
    assert {a: [p["position"] for p in pts] for a, pts in by_driver.items()} == {
        "VER": [1, 1, 1, 2, 2],
        "HAM": [2, 3, 3, 3, 3],
        "NOR": [2, 2, 2, 1, 1],
    }
    assert by_driver["NOR"][3]["position"] == 1  # NOR passes VER on lap 4
    assert any(p["is_pit_in_lap"] for p in by_driver["HAM"])
    assert all(p["sectors"] == [] for p in by_driver["VER"])  # no sector data: empty, not invented


async def test_timing_filters_by_driver_and_lap_range(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)
    await api_env.finish()
    url = f"{API}/replays/{replay_id}/timing"

    two = (await api_env.client.get(url, params=[("driver", "nor"), ("driver", "VER")])).json()
    assert [d["abbreviation"] for d in two["drivers"]] == ["NOR", "VER"]

    ranged = (await api_env.client.get(url, params={"lap_from": 2, "lap_to": 3})).json()
    assert all([p["lap_number"] for p in d["points"]] == [2, 3] for d in ranged["drivers"])
    assert ranged["lap_from"] == 2 and ranged["lap_to"] == 3

    single = await api_env.client.get(
        f"{API}/replays/{replay_id}/drivers/HAM/timing", params={"lap_from": 5}
    )
    assert single.status_code == 200
    assert single.json()["driver"]["abbreviation"] == "HAM"
    assert [p["lap_number"] for p in single.json()["driver"]["points"]] == [5]


async def test_timing_includes_sector_times_when_present(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)
    await api_env.finish()
    async with api_env.factory() as db:
        lap = await db.scalar(
            select(Lap).where(Lap.session_id == api_env.race.session_id, Lap.lap_number == 2)
        )
        assert lap is not None
        db.add_all(
            [
                Sector(lap_id=lap.id, sector_number=1, sector_time_ms=30_000),
                Sector(lap_id=lap.id, sector_number=2, sector_time_ms=None),
            ]
        )
        await db.commit()
        driver_id = lap.driver_id

    body = (
        await api_env.client.get(
            f"{API}/replays/{replay_id}/drivers/{driver_id}/timing", params={"lap_to": 2}
        )
    ).json()

    points = body["driver"]["points"]
    assert points[0]["sectors"] == []
    assert points[1]["sectors"] == [
        {"sector_number": 1, "sector_time_ms": 30_000},
        {"sector_number": 2, "sector_time_ms": None},
    ]


async def test_timing_errors(api_env: ApiEnv) -> None:
    replay_id = await api_env.create_replay()

    unknown_driver = await api_env.client.get(
        f"{API}/replays/{replay_id}/timing", params={"driver": "XXX"}
    )
    assert unknown_driver.status_code == 404 and unknown_driver.json()["code"] == "DRIVER_NOT_FOUND"
    unknown_replay = await api_env.client.get(f"{API}/replays/{uuid4()}/timing")
    assert unknown_replay.status_code == 404 and unknown_replay.json()["code"] == "REPLAY_NOT_FOUND"
    inverted = await api_env.client.get(
        f"{API}/replays/{replay_id}/timing", params={"lap_from": 3, "lap_to": 2}
    )
    assert inverted.status_code == 422


async def test_timing_gap_matches_the_race_state_gap(api_env: ApiEnv) -> None:
    replay_id = await started(api_env)
    await api_env.finish()

    timing = (await api_env.client.get(f"{API}/replays/{replay_id}/timing")).json()
    state = (await api_env.client.get(f"{API}/replays/{replay_id}/state")).json()

    last_points = {d["abbreviation"]: d["points"][-1] for d in timing["drivers"]}
    for driver in state["drivers"]:
        point = last_points[driver["abbreviation"]]
        assert point["lap_number"] == driver["laps_completed"]
        assert point["gap_to_leader_ms"] == driver["gap_to_leader_ms"]
