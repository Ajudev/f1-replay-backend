"""Race browsing endpoints: seasons, filtering, race detail, drivers, laps, sessions."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import select

from app.models import Driver, TyreStint
from tests.api.conftest import API, ApiEnv


async def test_seasons_newest_first_with_counts(api_env: ApiEnv) -> None:
    response = await api_env.client.get(f"{API}/seasons")

    assert response.status_code == 200
    seasons = response.json()
    assert [s["season"] for s in seasons] == sorted(
        {api_env.race.season, api_env.other_race.season}, reverse=True
    )
    assert all(s["race_count"] == 1 for s in seasons)


async def test_races_list_includes_sessions(api_env: ApiEnv) -> None:
    response = await api_env.client.get(f"{API}/races")

    assert response.status_code == 200
    races = response.json()
    assert {r["id"] for r in races} == {str(api_env.race.race_id), str(api_env.other_race.race_id)}
    assert [r["season"] for r in races] == sorted((r["season"] for r in races), reverse=True)
    race = next(r for r in races if r["id"] == str(api_env.race.race_id))
    assert race["sessions"] == [
        {
            "id": str(api_env.race.session_id),
            "session_type": "RACE",
            "name": "Race",
            "start_time": race["sessions"][0]["start_time"],
            "end_time": race["sessions"][0]["end_time"],
        }
    ]


async def test_race_filters(api_env: ApiEnv) -> None:
    async def ids(**params: object) -> set[str]:
        response = await api_env.client.get(f"{API}/races", params=params)  # type: ignore[arg-type]
        assert response.status_code == 200, response.text
        return {r["id"] for r in response.json()}

    race, other = str(api_env.race.race_id), str(api_env.other_race.race_id)
    assert await ids(season=api_env.other_race.season) == {other}
    assert await ids(round=api_env.race.round, season=api_env.race.season) == {race}
    assert await ids(event="test grand") == {race, other}
    assert await ids(event="testville") == {race, other}
    assert await ids(event="monaco") == set()
    assert await ids(session_type="RACE") == {race, other}
    assert await ids(session_type="QUALIFYING") == set()


async def test_race_filter_validation(api_env: ApiEnv) -> None:
    for params in ({"season": 1800}, {"round": 0}, {"session_type": "WARMUP"}, {"event": ""}):
        response = await api_env.client.get(f"{API}/races", params=params)
        assert response.status_code == 422, params
        assert response.json()["code"] == "VALIDATION_ERROR"


async def test_race_detail_and_missing_race(api_env: ApiEnv) -> None:
    found = await api_env.client.get(f"{API}/races/{api_env.race.race_id}")
    assert found.status_code == 200
    assert found.json()["name"] == "Test Grand Prix"
    assert found.json()["sessions"][0]["id"] == str(api_env.race.session_id)

    missing = await api_env.client.get(f"{API}/races/{uuid4()}")
    assert missing.status_code == 404
    assert missing.json()["code"] == "RACE_NOT_FOUND"
    assert set(missing.json()) == {"code", "message", "details"}


async def test_race_drivers(api_env: ApiEnv) -> None:
    response = await api_env.client.get(f"{API}/races/{api_env.race.race_id}/drivers")

    assert response.status_code == 200
    assert [d["abbreviation"] for d in response.json()] == ["HAM", "NOR", "VER"]

    no_session = await api_env.client.get(
        f"{API}/races/{api_env.race.race_id}/drivers", params={"session_type": "QUALIFYING"}
    )
    assert no_session.status_code == 404 and no_session.json()["code"] == "SESSION_NOT_FOUND"
    no_race = await api_env.client.get(f"{API}/races/{uuid4()}/drivers")
    assert no_race.status_code == 404 and no_race.json()["code"] == "RACE_NOT_FOUND"


async def test_race_laps_filter_by_driver_and_lap_range(api_env: ApiEnv) -> None:
    url = f"{API}/races/{api_env.race.race_id}/laps"

    nor = (await api_env.client.get(url, params={"driver": "nor"})).json()
    assert nor["total"] == 5
    assert {lap["driver_abbreviation"] for lap in nor["items"]} == {"NOR"}

    ranged = (await api_env.client.get(url, params={"lap_from": 2, "lap_to": 3})).json()
    assert ranged["total"] == 6
    assert [(lap["lap_number"], lap["driver_abbreviation"]) for lap in ranged["items"]] == [
        (2, "HAM"),
        (2, "NOR"),
        (2, "VER"),
        (3, "HAM"),
        (3, "NOR"),
        (3, "VER"),
    ]


async def test_race_laps_pagination(api_env: ApiEnv) -> None:
    url = f"{API}/races/{api_env.race.race_id}/laps"
    everything = (await api_env.client.get(url, params={"limit": 500})).json()["items"]

    page = (await api_env.client.get(url, params={"limit": 4, "offset": 4})).json()

    assert page["total"] == 15 and page["limit"] == 4 and page["offset"] == 4
    assert [lap["id"] for lap in page["items"]] == [lap["id"] for lap in everything[4:8]]


async def test_lap_query_errors(api_env: ApiEnv) -> None:
    url = f"{API}/races/{api_env.race.race_id}/laps"

    inverted = await api_env.client.get(url, params={"lap_from": 4, "lap_to": 2})
    assert inverted.status_code == 422
    assert inverted.json()["code"] == "VALIDATION_ERROR"
    assert inverted.json()["details"]["errors"][0]["loc"] == ["query", "lap_to"]

    unknown = await api_env.client.get(url, params={"driver": "XXX"})
    assert unknown.status_code == 404 and unknown.json()["code"] == "DRIVER_NOT_FOUND"

    malformed = await api_env.client.get(url, params={"driver": "N@R"})
    assert malformed.status_code == 422

    too_big = await api_env.client.get(url, params={"limit": 501})
    assert too_big.status_code == 422


async def test_session_laps_stints_and_track_status(api_env: ApiEnv) -> None:
    sid = api_env.race.session_id
    async with api_env.factory() as db:
        for driver in (await db.scalars(select(Driver).where(Driver.session_id == sid))).all():
            db.add(
                TyreStint(
                    session_id=sid,
                    driver_id=driver.id,
                    stint_number=1,
                    compound="MEDIUM",
                    start_lap=1,
                    end_lap=None,
                    tyre_age_at_start=0,
                )
            )
        await db.commit()

    laps = (await api_env.client.get(f"{API}/sessions/{sid}/laps", params={"lap_to": 1})).json()
    assert laps["total"] == 3

    stints = (
        await api_env.client.get(f"{API}/sessions/{sid}/stints", params={"driver": "HAM"})
    ).json()
    assert [(s["driver_abbreviation"], s["stint_number"]) for s in stints] == [("HAM", 1)]

    track = await api_env.client.get(f"{API}/sessions/{sid}/track-status")
    assert track.status_code == 200
    assert [t["status"] for t in track.json()] == ["GREEN", "SAFETY_CAR", "GREEN"]

    missing = await api_env.client.get(f"{API}/sessions/{uuid4()}/laps")
    assert missing.status_code == 404 and missing.json()["code"] == "SESSION_NOT_FOUND"
