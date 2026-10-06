"""Fixtures for race state tests: fakeredis streams, SQLite timeline, processor wiring."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import ReplayStatus
from app.infrastructure.redis import RedisClient
from app.models import ReplaySession
from app.race_state.config import RaceStateConfig
from app.race_state.models import RaceState, SnapshotTrigger
from app.race_state.processor import RaceStateProcessor
from app.race_state.repository import RaceStateStore
from app.race_state.seed import RaceSeedLoader
from app.race_state.snapshots import DatabaseSnapshotSink
from app.streaming.config import StreamConfig
from app.streaming.consumer import ReceivedMessage
from app.streaming.envelope import StreamEvent, derive_event_id
from app.timeline.repository import TimelineRepository
from app.timeline.source import TimelineSource
from tests.replay.conftest import ReplayEnv, seed_with_timeline  # noqa: F401
from tests.streaming.conftest import (  # noqa: F401
    cleanup_streams,
    config,
    redis_client,
    replay_env,
)
from tests.timeline.factories import representative_race

BASE_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class FakeSleep:
    """Injectable sleep: records the pauses, never waits, optionally runs a hook."""

    def __init__(self, hook: object | None = None) -> None:
        self.calls: list[float] = []
        self.hook = hook

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if self.hook is not None:
            await self.hook(len(self.calls))  # type: ignore[operator]


class FailingSnapshots:
    """Snapshot sink that raises for chosen triggers (``None`` = every trigger)."""

    def __init__(self, fail_for: set[SnapshotTrigger] | None = None) -> None:
        from sqlalchemy.exc import OperationalError

        self.fail_for = fail_for
        self.saved: list[tuple[int, SnapshotTrigger]] = []
        self._error = OperationalError("INSERT", {}, Exception("database down"))

    async def save(self, state: RaceState, trigger: SnapshotTrigger) -> None:
        if self.fail_for is None or trigger in self.fail_for:
            raise self._error
        self.saved.append((state.last_sequence, trigger))


@dataclass
class StateEnv:
    factory: async_sessionmaker[AsyncSession]
    redis: RedisClient
    stream_config: StreamConfig
    race: TimelineSource
    store: RaceStateStore
    processor: RaceStateProcessor
    state_config: RaceStateConfig
    timeline: list[StreamEvent] = field(default_factory=list)
    runs: dict[UUID, UUID] = field(default_factory=dict)

    def events(
        self, replay_id: UUID, run_id: UUID, *, published_at: datetime = BASE_TIME
    ) -> list[StreamEvent]:
        """The whole persisted timeline as stream events of one run."""
        return [
            StreamEvent(
                event_id=derive_event_id(run_id, e.sequence),
                schema_version=1,
                event_type=e.event_type.value,
                replay_id=replay_id,
                run_id=run_id,
                session_id=self.race.session_id,
                sequence=e.sequence,
                race_time_ms=e.race_time_ms or 0,
                lap_number=e.lap_number,
                driver_id=e.driver_id,
                driver_abbreviation=e.driver_abbreviation,
                published_at=published_at + timedelta(milliseconds=e.sequence),
                payload=e.payload,
            )
            for e in self.timeline
        ]

    def make_processor(
        self,
        snapshots: object | None = None,
        *,
        sleep: object | None = None,
        state_config: RaceStateConfig | None = None,
    ) -> RaceStateProcessor:
        return RaceStateProcessor(
            self.store,
            RaceSeedLoader(self.factory),
            snapshots or DatabaseSnapshotSink(self.factory),  # type: ignore[arg-type]
            config=state_config or self.state_config,
            clock=lambda: BASE_TIME,
            sleep=sleep or FakeSleep(),  # type: ignore[arg-type]
        )

    async def process(
        self, event: StreamEvent, processor: RaceStateProcessor | None = None
    ) -> None:
        await (processor or self.processor).handle(
            ReceivedMessage(self.stream_config.raw_stream, "1-0", event, 1)
        )

    async def state(self, replay_id: UUID) -> RaceState | None:
        return await self.store.get(replay_id)

    async def state_events(self) -> list[StreamEvent]:
        entries = await self.redis.client.xrange(self.stream_config.state_stream)
        return [StreamEvent.from_fields(fields) for _, fields in entries]


async def make_replay(
    factory: async_sessionmaker[AsyncSession],
    session_id: UUID,
    status: ReplayStatus = ReplayStatus.RUNNING,
) -> UUID:
    async with factory() as db:
        row = ReplaySession(id=uuid4(), session_id=session_id, status=status)
        db.add(row)
        await db.commit()
        return row.id


@pytest.fixture
async def state_env(
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,  # noqa: F811
    config: StreamConfig,  # noqa: F811
) -> AsyncIterator[StateEnv]:
    race = representative_race()
    await seed_with_timeline(session_factory, race)
    async with session_factory() as db:
        timeline = await TimelineRepository(db).load_events(race.session_id)
    state_config = RaceStateConfig(lap_history=10, snapshot_every_laps=2)
    store = RaceStateStore(redis_client, stream_config=config, config=state_config)
    env = StateEnv(
        factory=session_factory,
        redis=redis_client,
        stream_config=config,
        race=race,
        store=store,
        processor=None,  # type: ignore[arg-type]
        state_config=state_config,
        timeline=timeline,
    )
    env.processor = env.make_processor()
    yield env
    keys = [k async for k in redis_client.client.scan_iter(match="race:*:state")]
    if keys:
        await redis_client.client.delete(*keys)
