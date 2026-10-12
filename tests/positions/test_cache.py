"""PositionTrackCache: one load per session, failures back off instead of retrying per tick."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.positions.cache import PositionTrackCache


async def test_a_session_without_data_is_cached(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    calls = 0

    def counting() -> AsyncSession:
        nonlocal calls
        calls += 1
        return session_factory()

    cache = PositionTrackCache(counting, max_gap_ms=3_000, max_sessions=2)  # type: ignore[arg-type]
    sid = uuid4()
    results = await asyncio.gather(cache.get(sid), cache.get(sid))
    assert results == [None, None]
    assert await cache.get(sid) is None
    assert calls == 1


async def test_a_failed_load_is_not_retried_until_the_backoff_passes() -> None:
    calls = 0

    def broken() -> AsyncSession:
        nonlocal calls
        calls += 1
        raise OSError("database down")

    cache = PositionTrackCache(
        broken,  # type: ignore[arg-type]
        max_gap_ms=3_000,
        max_sessions=2,
        retry_after_seconds=0.05,
    )
    sid = uuid4()
    assert await cache.get(sid) is None
    assert await cache.get(sid) is None
    assert calls == 1
    await asyncio.sleep(0.06)
    assert await cache.get(sid) is None
    assert calls == 2
