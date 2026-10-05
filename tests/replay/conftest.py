"""Seeded sessions with generated timelines and a replay service on fake time."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, replace

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.replay.service import ReplayService
from app.timeline.service import TimelineService
from app.timeline.source import TimelineSource
from tests.replay.fakes import CollectingSink, ManualTimer
from tests.timeline.factories import representative_race
from tests.timeline.seed import seed_source


@dataclass
class ReplayEnv:
    service: ReplayService
    sink: CollectingSink
    timer: ManualTimer
    race: TimelineSource
    other_race: TimelineSource
    event_count: int


async def seed_with_timeline(factory: async_sessionmaker[AsyncSession], src: TimelineSource) -> int:
    await seed_source(factory, src)
    async with factory() as db:
        summary = await TimelineService(db).generate(src.session_id)
    return summary.event_count


@pytest.fixture
async def replay_env(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[ReplayEnv]:
    race = representative_race()
    other = replace(representative_race(), season=2023, round=10)
    count = await seed_with_timeline(session_factory, race)
    await seed_with_timeline(session_factory, other)
    sink, timer = CollectingSink(), ManualTimer()
    service = ReplayService(session_factory, sink=sink, timer=timer, stop_timeout=0.5)
    yield ReplayEnv(service, sink, timer, race, other, count)
    await service.shutdown()
