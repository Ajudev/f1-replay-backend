"""Integration tests: seeded SQLite -> TimelineService -> persisted events."""

from __future__ import annotations

from dataclasses import replace

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import EventType, SessionType
from app.models import RaceEvent, SessionTimeline
from app.services.race_queries import SessionNotFoundError
from app.timeline.builder import build_timeline
from app.timeline.errors import (
    TimelineBuildError,
    TimelineNotGeneratedError,
    UnsupportedSessionTypeError,
)
from app.timeline.service import TimelineService
from tests.timeline.factories import drv, lap, representative_race, source
from tests.timeline.seed import seed_source


@pytest.fixture
async def seeded(session_factory: async_sessionmaker[AsyncSession]):
    src = representative_race()
    await seed_source(session_factory, src)
    return src


async def test_generate_persists_events_matching_pure_builder(
    db_session: AsyncSession, seeded
) -> None:
    summary = await TimelineService(db_session).generate(seeded.session_id)
    expected = build_timeline(seeded)

    assert summary.status == "generated"
    assert summary.event_count == len(expected.events)
    assert summary.race_start_session_time_ms == expected.race_start_session_time_ms
    assert summary.counts_by_type["LAP_COMPLETED"] == 15
    assert summary.counts_by_type["RACE_STARTED"] == 1

    page = await TimelineService(db_session).get_events(seeded.session_id, limit=1000)
    assert page.total == len(expected.events)
    assert [e.sequence for e in page.items] == list(range(len(expected.events)))
    assert [(e.event_type, e.race_time_ms, e.lap_number, e.driver_id) for e in page.items] == [
        (e.event_type, e.race_time_ms, e.lap_number, e.driver_id) for e in expected.events
    ]
    assert page.items[0].event_type == EventType.RACE_STARTED
    abbreviations = {d.id: d.abbreviation for d in seeded.drivers}
    for item in page.items:
        if item.driver_id is not None:
            assert item.driver_abbreviation == abbreviations[item.driver_id]
        else:
            assert item.driver_abbreviation is None


async def test_representative_race_regression(db_session: AsyncSession, seeded) -> None:
    await TimelineService(db_session).generate(seeded.session_id)
    page = await TimelineService(db_session).get_events(seeded.session_id, limit=1000)
    events = page.items

    assert events[0].event_type == EventType.RACE_STARTED and events[0].race_time_ms == 0
    by_type: dict[EventType, list] = {}
    for item in events:
        by_type.setdefault(item.event_type, []).append(item)

    assert len(by_type[EventType.LAP_COMPLETED]) == 15
    # HAM: one pit in (lap 2) and one pit out (lap 3); pre-race exit excluded.
    assert [(e.driver_abbreviation, e.lap_number) for e in by_type[EventType.PIT_ENTRY]] == [
        ("HAM", 2)
    ]
    assert [(e.driver_abbreviation, e.lap_number) for e in by_type[EventType.PIT_EXIT]] == [
        ("HAM", 3)
    ]
    # Safety car appears as a status change and is reflected on lap events.
    statuses = [e.payload["status"] for e in by_type[EventType.TRACK_STATUS_CHANGED]]
    assert statuses == ["GREEN", "SAFETY_CAR", "GREEN"]
    # NOR passes VER on lap 4.
    nor_pos = [
        (e.lap_number, e.payload["previous_position"], e.payload["new_position"])
        for e in by_type[EventType.POSITION_CHANGED]
        if e.driver_abbreviation == "NOR"
    ]
    assert (4, 2, 1) in nor_pos
    # Fastest lap is NOR's lap 4.
    assert by_type[EventType.FASTEST_LAP][-1].driver_abbreviation == "NOR"
    assert by_type[EventType.FASTEST_LAP][-1].lap_number == 4
    # Last event is late in the race and times never decrease.
    assert events[-1].race_time_ms >= 5 * 90_000
    times = [e.race_time_ms for e in events]
    assert times == sorted(times)


async def test_generate_twice_returns_already_generated_without_rewrite(
    db_session: AsyncSession, seeded
) -> None:
    service = TimelineService(db_session)
    first = await service.generate(seeded.session_id)
    ids_before = set((await db_session.scalars(select(RaceEvent.id))).all())

    second = await service.generate(seeded.session_id)
    ids_after = set((await db_session.scalars(select(RaceEvent.id))).all())

    assert second.status == "already_generated"
    # SQLite drops tzinfo on read; compare the wall-clock value.
    assert second.generated_at.replace(tzinfo=None) == first.generated_at.replace(tzinfo=None)
    assert second.event_count == first.event_count
    assert second.counts_by_type == first.counts_by_type
    assert ids_before == ids_after


async def test_regenerate_replaces_without_duplicates(db_session: AsyncSession, seeded) -> None:
    service = TimelineService(db_session)
    first = await service.generate(seeded.session_id)
    ids_before = set((await db_session.scalars(select(RaceEvent.id))).all())

    again = await service.generate(seeded.session_id, regenerate=True)

    assert again.status == "regenerated"
    assert again.event_count == first.event_count
    total = await db_session.scalar(select(func.count()).select_from(RaceEvent))
    timelines = await db_session.scalar(select(func.count()).select_from(SessionTimeline))
    assert total == first.event_count
    assert timelines == 1
    ids_after = set((await db_session.scalars(select(RaceEvent.id))).all())
    assert ids_before.isdisjoint(ids_after)


async def test_failed_regeneration_keeps_existing_timeline(
    db_session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    seeded,
) -> None:
    service = TimelineService(db_session)
    first = await service.generate(seeded.session_id)
    # Remove every lap-1 start so the epoch cannot be derived.
    async with session_factory() as other:
        from sqlalchemy import update

        from app.models import Lap

        await other.execute(update(Lap).where(Lap.lap_number == 1).values(lap_start_time_ms=None))
        await other.commit()

    with pytest.raises(TimelineBuildError):
        await service.generate(seeded.session_id, regenerate=True)
    summary = await service.get_summary(seeded.session_id)
    assert summary.event_count == first.event_count


async def test_reads_never_generate_implicitly(db_session: AsyncSession, seeded) -> None:
    service = TimelineService(db_session)
    with pytest.raises(TimelineNotGeneratedError) as excinfo:
        await service.get_events(seeded.session_id)
    assert "POST" in excinfo.value.message
    with pytest.raises(TimelineNotGeneratedError):
        await service.get_summary(seeded.session_id)
    assert await db_session.scalar(select(func.count()).select_from(RaceEvent)) == 0


async def test_unknown_session_raises_session_not_found(db_session: AsyncSession) -> None:
    from uuid import uuid4

    service = TimelineService(db_session)
    for call in (
        service.generate(uuid4()),
        service.get_events(uuid4()),
        service.get_summary(uuid4()),
    ):
        with pytest.raises(SessionNotFoundError):
            await call


async def test_unsupported_session_type(
    db_session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    d = drv("VER", 1)
    src = replace(
        source([d], [lap(d, 1, start=0, end=90_000)]), session_type=SessionType.QUALIFYING
    )
    await seed_source(session_factory, src)
    with pytest.raises(UnsupportedSessionTypeError):
        await TimelineService(db_session).generate(src.session_id)


async def test_event_filters_and_pagination(db_session: AsyncSession, seeded) -> None:
    service = TimelineService(db_session)
    await service.generate(seeded.session_id)

    ham = await service.get_events(seeded.session_id, driver="ham", limit=1000)
    assert ham.items and all(e.driver_abbreviation == "HAM" for e in ham.items)

    pits = await service.get_events(
        seeded.session_id,
        event_types=[EventType.PIT_ENTRY, EventType.PIT_EXIT],
        limit=1000,
    )
    assert [e.event_type for e in pits.items] == [EventType.PIT_ENTRY, EventType.PIT_EXIT]

    window = await service.get_events(
        seeded.session_id,
        event_types=[EventType.LAP_COMPLETED],
        lap_from=2,
        lap_to=3,
        limit=1000,
    )
    assert window.total == 6
    assert {e.lap_number for e in window.items} == {2, 3}

    nobody = await service.get_events(seeded.session_id, driver="ZZZ")
    assert nobody.total == 0 and nobody.items == []

    page1 = await service.get_events(seeded.session_id, limit=5, offset=0)
    page2 = await service.get_events(seeded.session_id, limit=5, offset=5)
    assert [e.sequence for e in page1.items] == [0, 1, 2, 3, 4]
    assert [e.sequence for e in page2.items] == [5, 6, 7, 8, 9]
    assert page1.total == page2.total


async def test_summary_includes_warnings(db_session: AsyncSession, seeded) -> None:
    service = TimelineService(db_session)
    await service.generate(seeded.session_id)
    summary = await service.get_summary(seeded.session_id)
    assert summary.status == "available"
    assert summary.schema_version == 1
    assert any("before race start" in w for w in summary.warnings)
    assert any("no completion time" in w for w in summary.warnings) is False


async def test_loading_source_uses_constant_number_of_queries(
    db_session: AsyncSession, sqlite_engine, seeded
) -> None:
    from app.timeline.repository import TimelineRepository

    statements: list[str] = []

    def count(_conn, _cursor, statement, *_args) -> None:  # noqa: ANN001
        statements.append(statement)

    event.listen(sqlite_engine.sync_engine, "before_cursor_execute", count)
    try:
        loaded = await TimelineRepository(db_session).load_source(seeded.session_id)
    finally:
        event.remove(sqlite_engine.sync_engine, "before_cursor_execute", count)

    assert loaded is not None and len(loaded.laps) == 15
    assert len(statements) == 4


async def test_outdated_schema_version_flagged_and_regenerate_upgrades(
    db_session: AsyncSession, seeded, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.timeline.service as service_module

    service = TimelineService(db_session)
    first = await service.generate(seeded.session_id)
    assert first.is_current_schema_version is True

    # Simulate a newer schema: the stored timeline (v1) is now outdated.
    monkeypatch.setattr(service_module, "TIMELINE_SCHEMA_VERSION", 2)
    assert (await service.get_summary(seeded.session_id)).is_current_schema_version is False
    again = await service.generate(seeded.session_id)
    assert again.status == "already_generated"
    assert again.is_current_schema_version is False

    upgraded = await service.generate(seeded.session_id, regenerate=True)
    assert upgraded.schema_version == 2
    assert upgraded.is_current_schema_version is True


def _integrity_error(message: str):  # noqa: ANN202
    from sqlalchemy.exc import IntegrityError

    return IntegrityError("INSERT", {}, Exception(message))


@pytest.mark.parametrize(
    "message",
    [
        "UNIQUE constraint failed: race_events.session_id, race_events.sequence",
        "UNIQUE constraint failed: session_timelines.session_id",
    ],
)
async def test_persist_unique_violation_maps_to_conflict(
    db_session: AsyncSession, seeded, monkeypatch: pytest.MonkeyPatch, message: str
) -> None:
    from app.timeline.errors import TimelineConflictError
    from app.timeline.repository import TimelineRepository

    async def boom(*_a, **_k):  # noqa: ANN002, ANN003, ANN202
        raise _integrity_error(message)

    monkeypatch.setattr(TimelineRepository, "replace_timeline", boom)
    with pytest.raises(TimelineConflictError):
        await TimelineService(db_session).generate(seeded.session_id)


async def test_constraint_name_attribute_is_used_when_present(
    db_session: AsyncSession, seeded, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.timeline.errors import TimelineConflictError
    from app.timeline.repository import TimelineRepository

    class _Pg(Exception):
        constraint_name = "uq_race_events_session_id_sequence"

    async def boom(*_a, **_k):  # noqa: ANN002, ANN003, ANN202
        from sqlalchemy.exc import IntegrityError

        raise IntegrityError("INSERT", {}, _Pg("duplicate key"))

    monkeypatch.setattr(TimelineRepository, "replace_timeline", boom)
    with pytest.raises(TimelineConflictError):
        await TimelineService(db_session).generate(seeded.session_id)


async def test_other_integrity_errors_are_reraised(
    db_session: AsyncSession, seeded, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlalchemy.exc import IntegrityError

    from app.timeline.repository import TimelineRepository

    async def boom(*_a, **_k):  # noqa: ANN002, ANN003, ANN202
        raise _integrity_error("FOREIGN KEY constraint failed")

    monkeypatch.setattr(TimelineRepository, "replace_timeline", boom)
    with pytest.raises(IntegrityError):
        await TimelineService(db_session).generate(seeded.session_id)
