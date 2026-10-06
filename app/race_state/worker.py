"""Race state worker process.

    uv run python -m app.race_state.worker

Consumes ``race.raw.events`` as group ``race-state-processors``, maintains the hot
state in Redis, publishes ``race.state.events`` and writes PostgreSQL snapshots. It
runs outside the API process. Run a single worker: the compare-and-set on the state
document keeps several workers correct, but they contend for the same sequence and
nothing partitions replays between them.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import create_engine, create_session_factory
from app.infrastructure.redis import RedisClient
from app.race_state.config import RaceStateConfig
from app.race_state.processor import RaceStateProcessor
from app.race_state.repository import RaceStateStore
from app.race_state.seed import RaceSeedLoader
from app.race_state.snapshots import DatabaseSnapshotSink
from app.streaming.config import GROUP_STATE_PROCESSORS, StreamConfig
from app.streaming.consumer import StreamConsumer, make_consumer_name
from app.streaming.idempotency import RedisIdempotencyStore

logger = logging.getLogger(__name__)


def build_consumer(
    redis: RedisClient,
    session_factory: async_sessionmaker[AsyncSession],
    stream_config: StreamConfig,
    state_config: RaceStateConfig,
) -> StreamConsumer:
    """The ``race-state-processors`` consumer with its processor wired up."""
    processor = RaceStateProcessor(
        RaceStateStore(redis, stream_config=stream_config, config=state_config),
        RaceSeedLoader(session_factory),
        DatabaseSnapshotSink(session_factory),
        config=state_config,
    )
    return StreamConsumer(
        redis,
        stream=stream_config.raw_stream,
        group=GROUP_STATE_PROCESSORS,
        consumer_name=make_consumer_name("race-state"),
        handler=processor,
        config=stream_config,
        idempotency_store=RedisIdempotencyStore(redis, stream_config.idempotency_ttl_seconds),
    )


async def run_worker(stop_event: asyncio.Event | None = None) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    redis = RedisClient(settings.redis_url)
    consumer = build_consumer(
        redis,
        session_factory,
        StreamConfig.from_settings(settings),
        RaceStateConfig.from_settings(settings),
    )

    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    stopper = asyncio.create_task(_stop_when_set(consumer, stop))
    try:
        await consumer.run()
    finally:
        stopper.cancel()
        await redis.aclose()
        await engine.dispose()


async def _stop_when_set(consumer: StreamConsumer, stop: asyncio.Event) -> None:
    await stop.wait()
    logger.info("Race state worker stopping")
    consumer.stop()


def main() -> int:
    try:
        asyncio.run(run_worker())
    except RedisError as exc:
        logger.error("Redis error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
