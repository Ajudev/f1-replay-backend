"""Persistence of detected events and the ``/replays/{id}/events`` endpoint."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4, uuid5

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.detection.models import DetectedEvent
from app.detection.repository import DatabaseDetectedEventSink, DetectedEventRepository
from app.domain.enums import DetectedEventType, Severity
from app.main import create_app
from app.models import DetectedEvent as DetectedEventRow
from tests.detection.builders import BASE_TIME, SESSION, driver_id
from tests.detection.conftest import DetectEnv

RUN_A = UUID(int=11)
RUN_B = UUID(int=12)


def make_event(
    replay_id: UUID,
    event_type: DetectedEventType,
    sequence: int,
    *,
    run_id: UUID = RUN_A,
    primary: str = "AAA",
    secondary: str | None = None,
    lap: int | None = 5,
    severity: Severity | None = None,
    detected_at: datetime = BASE_TIME,
) -> DetectedEvent:
    return DetectedEvent(
        detected_event_id=uuid5(run_id, f"{event_type.value}:{sequence}:{primary}:{secondary}"),
        event_type=event_type,
        replay_id=replay_id,
        run_id=run_id,
        session_id=SESSION,
        race_id=UUID(int=4000),
        race_time_ms=sequence * 1000,
        lap_number=lap,
        primary_driver_id=driver_id(primary),
        primary_driver_abbreviation=primary,
        secondary_driver_id=driver_id(secondary) if secondary else None,
        secondary_driver_abbreviation=secondary,
        severity=severity,
        evidence={"gap_ms": 800, "nested": {"a": [1, 2]}},
        source_event_ids=[uuid4()],
        source_sequence=sequence,
        detector_name="test",
        detector_version=1,
        detected_at=detected_at,
    )


@dataclass
class ApiEnv:
    client: AsyncClient
    env: DetectEnv
    replay_id: UUID


@pytest.fixture
async def api(detect_env: DetectEnv) -> AsyncIterator[ApiEnv]:
    replay_id = uuid4()
    await detect_env.add_replay(replay_id)
    application = create_app()

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        async with detect_env.factory() as session:
            yield session

    application.dependency_overrides[get_db] = override_db
    async with AsyncClient(
        transport=ASGITransport(app=application), base_url="http://test"
    ) as client:
        yield ApiEnv(client, detect_env, replay_id)


async def store(env: DetectEnv, *events: DetectedEvent) -> None:
    await DatabaseDetectedEventSink(env.factory).save(list(events))


# -- persistence -------------------------------------------------------------------------------


async def test_inserting_the_same_event_twice_keeps_one_row(detect_env: DetectEnv) -> None:
    replay_id = uuid4()
    await detect_env.add_replay(replay_id)
    event = make_event(replay_id, DetectedEventType.OVERTAKE, 3, secondary="BBB")

    async with detect_env.factory() as db:
        repo = DetectedEventRepository(db)
        first = await repo.insert_many([event])
        second = await repo.insert_many([event])
        await db.commit()
        count = await db.scalar(select(func.count()).select_from(DetectedEventRow))

    assert (first, second, count) == (1, 0, 1)


async def test_a_row_round_trips_every_field(detect_env: DetectEnv) -> None:
    replay_id = uuid4()
    await detect_env.add_replay(replay_id)
    event = make_event(
        replay_id, DetectedEventType.PACE_ANOMALY, 7, severity=Severity.HIGH, secondary="BBB"
    )
    await store(detect_env, event)

    [row] = await detect_env.rows()

    assert row.id == event.detected_event_id
    assert (row.replay_id, row.run_id, row.session_id) == (replay_id, RUN_A, SESSION)
    assert (row.event_type, row.severity, row.lap_number) == ("PACE_ANOMALY", "HIGH", 5)
    assert (row.primary_driver_abbreviation, row.secondary_driver_abbreviation) == ("AAA", "BBB")
    assert row.evidence == {"gap_ms": 800, "nested": {"a": [1, 2]}}
    assert row.source_event_ids == [str(event.source_event_ids[0])]
    assert (row.detector_name, row.detector_version, row.source_sequence) == ("test", 1, 7)
    assert row.confidence is None


async def test_deleting_the_replay_removes_its_detections(detect_env: DetectEnv) -> None:
    replay_id = uuid4()
    await detect_env.add_replay(replay_id)
    await store(detect_env, make_event(replay_id, DetectedEventType.OVERTAKE, 1))

    from sqlalchemy import delete

    from app.models import ReplaySession

    async with detect_env.factory() as db:
        await db.execute(delete(ReplaySession).where(ReplaySession.id == replay_id))
        await db.commit()

    assert await detect_env.rows() == []


# -- endpoint ------------------------------------------------------------------------------------


async def get(api: ApiEnv, **params: object) -> dict:
    response = await api.client.get(f"/api/v1/replays/{api.replay_id}/events", params=params)  # type: ignore[arg-type]
    assert response.status_code == 200, response.text
    return response.json()


async def test_unknown_replay_is_404(api: ApiEnv) -> None:
    response = await api.client.get(f"/api/v1/replays/{uuid4()}/events")

    assert response.status_code == 404
    assert "Replay not found" in response.json()["message"]


async def test_a_replay_without_detections_returns_an_empty_page(api: ApiEnv) -> None:
    body = await get(api)

    assert body == {
        "replay_id": str(api.replay_id),
        "run_id": None,
        "items": [],
        "total": 0,
        "limit": 100,
        "offset": 0,
    }


async def test_items_are_ordered_and_carry_the_full_contract(api: ApiEnv) -> None:
    rid = api.replay_id
    await store(
        api.env,
        make_event(rid, DetectedEventType.PERSONAL_BEST, 9),
        make_event(rid, DetectedEventType.OVERTAKE, 3, secondary="BBB"),
        make_event(rid, DetectedEventType.BATTLE_FORMING, 3, secondary="BBB"),
    )

    body = await get(api)

    assert [(i["source_sequence"], i["event_type"]) for i in body["items"]] == [
        (3, "BATTLE_FORMING"),
        (3, "OVERTAKE"),
        (9, "PERSONAL_BEST"),
    ]
    assert body["total"] == 3 and body["run_id"] == str(RUN_A)
    item = body["items"][1]
    assert set(item) == {
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
    assert item["evidence"] == {"gap_ms": 800, "nested": {"a": [1, 2]}}
    assert item["primary_driver_id"] == str(driver_id("AAA"))


async def test_filters(api: ApiEnv) -> None:
    rid = api.replay_id
    await store(
        api.env,
        make_event(rid, DetectedEventType.OVERTAKE, 1, primary="AAA", secondary="BBB", lap=3),
        make_event(rid, DetectedEventType.OVERTAKE, 2, primary="CCC", secondary="AAA", lap=6),
        make_event(rid, DetectedEventType.PERSONAL_BEST, 3, primary="BBB", lap=9),
        make_event(rid, DetectedEventType.NEW_STINT, 4, primary="DDD", lap=None),
    )

    async def sequences(**params: object) -> list[int]:
        return [i["source_sequence"] for i in (await get(api, **params))["items"]]

    assert await sequences(event_type="OVERTAKE") == [1, 2]
    assert await sequences(event_type=["OVERTAKE", "PERSONAL_BEST"]) == [1, 2, 3]
    assert await sequences(driver="aaa") == [1, 2]  # abbreviation, either side, any case
    assert await sequences(driver=str(driver_id("BBB"))) == [1, 3]
    assert await sequences(lap_from=4, lap_to=8) == [2]
    assert await sequences(lap_from=6) == [2, 3]
    assert await sequences(event_type="OVERTAKE", driver="CCC", lap_to=6) == [2]
    assert await sequences(limit=2) == [1, 2]
    assert await sequences(limit=2, offset=2) == [3, 4]
    assert (await get(api, limit=2))["total"] == 4

    bad = await api.client.get(f"/api/v1/replays/{api.replay_id}/events?event_type=NOPE")
    assert bad.status_code == 422


async def test_by_default_only_the_latest_run_is_returned(api: ApiEnv) -> None:
    rid = api.replay_id
    await store(api.env, make_event(rid, DetectedEventType.OVERTAKE, 1, run_id=RUN_A))
    await store(api.env, make_event(rid, DetectedEventType.OVERTAKE, 2, run_id=RUN_B))
    await store(api.env, make_event(rid, DetectedEventType.PERSONAL_BEST, 3, run_id=RUN_B))

    latest = await get(api)
    explicit = await get(api, run_id=str(RUN_A))

    assert latest["run_id"] == str(RUN_B) and latest["total"] == 2
    assert explicit["run_id"] == str(RUN_A) and [
        i["source_sequence"] for i in explicit["items"]
    ] == [1]
    unknown = await get(api, run_id=str(uuid4()))
    assert unknown["total"] == 0 and unknown["items"] == []


async def test_a_database_failure_is_a_503(api: ApiEnv) -> None:
    from sqlalchemy.exc import OperationalError

    class BrokenSession:
        async def scalar(self, *_args: object, **_kwargs: object) -> None:
            raise OperationalError("SELECT", {}, Exception("down"))

    async def broken_db() -> AsyncGenerator[BrokenSession, None]:
        yield BrokenSession()

    application = api.client._transport.app  # type: ignore[attr-defined]
    application.dependency_overrides[get_db] = broken_db

    response = await api.client.get(f"/api/v1/replays/{api.replay_id}/events")

    assert response.status_code == 503
