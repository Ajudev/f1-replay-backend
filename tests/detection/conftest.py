"""Fixtures for detection tests: fakeredis streams, SQLite, a wired processor."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.detection.config import DetectionConfig
from app.detection.engine import DetectionEngine
from app.detection.processor import DetectionProcessor
from app.detection.registry import build_default_registry
from app.detection.repository import DatabaseDetectedEventSink
from app.detection.store import DetectionContextStore
from app.infrastructure.redis import RedisClient
from app.models import DetectedEvent as DetectedEventRow
from app.models import ReplaySession
from app.race_state.config import RaceStateConfig
from app.race_state.repository import RaceStateStore
from app.streaming.config import StreamConfig
from app.streaming.consumer import ReceivedMessage
from app.streaming.envelope import StreamEvent
from tests.detection.builders import BASE_TIME
from tests.replay.conftest import replay_env  # noqa: F401
from tests.streaming.conftest import (  # noqa: F401
    cleanup_streams,
    config,
    redis_client,
)
from tests.timeline.factories import representative_race
from tests.timeline.seed import seed_source


@dataclass
class DetectEnv:
    factory: async_sessionmaker[AsyncSession]
    redis: RedisClient
    stream_config: StreamConfig
    detection_config: DetectionConfig
    store: DetectionContextStore
    state_store: RaceStateStore
    processor: DetectionProcessor
    session_id: UUID

    async def add_replay(self, replay_id: UUID) -> None:
        async with self.factory() as db:
            db.add(ReplaySession(id=replay_id, session_id=self.session_id))
            await db.commit()

    async def handle(self, event: StreamEvent, processor: DetectionProcessor | None = None) -> None:
        await (processor or self.processor).handle(
            ReceivedMessage(self.stream_config.state_stream, "1-0", event, 1)
        )

    async def published(self) -> list[StreamEvent]:
        entries = await self.redis.client.xrange(self.stream_config.detected_stream)
        return [StreamEvent.from_fields(fields) for _, fields in entries]

    async def rows(self, replay_id: UUID | None = None) -> list[DetectedEventRow]:
        async with self.factory() as db:
            query = select(DetectedEventRow).order_by(
                DetectedEventRow.source_sequence, DetectedEventRow.id
            )
            if replay_id is not None:
                query = query.where(DetectedEventRow.replay_id == replay_id)
            return list(await db.scalars(query))

    def make_processor(self, sink: object | None = None) -> DetectionProcessor:
        engine = DetectionEngine(
            build_default_registry(self.detection_config),
            self.detection_config,
            clock=lambda: BASE_TIME,
        )
        return DetectionProcessor(
            engine,
            self.store,
            self.state_store,
            sink or DatabaseDetectedEventSink(self.factory),  # type: ignore[arg-type]
            config=self.detection_config,
        )


@pytest.fixture
async def detect_env(
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,  # noqa: F811
    config: StreamConfig,  # noqa: F811
) -> AsyncIterator[DetectEnv]:
    source = representative_race()
    await seed_source(session_factory, source)
    detection_config = DetectionConfig()
    env = DetectEnv(
        factory=session_factory,
        redis=redis_client,
        stream_config=config,
        detection_config=detection_config,
        store=DetectionContextStore(redis_client, stream_config=config, config=detection_config),
        state_store=RaceStateStore(
            redis_client, stream_config=config, config=RaceStateConfig(lap_history=10)
        ),
        processor=None,  # type: ignore[arg-type]
        session_id=source.session_id,
    )
    env.processor = env.make_processor()
    yield env
    keys = [k async for k in redis_client.client.scan_iter(match="race:*:detection")]
    keys += [k async for k in redis_client.client.scan_iter(match="race:*:state")]
    if keys:
        await redis_client.client.delete(*keys)


def new_replay_id() -> UUID:
    return uuid4()
