"""Event detection worker process.

    uv run python -m app.detection.worker

Consumes ``race.state.events`` as group ``race-event-detectors``, runs the registered
detectors, persists detections to PostgreSQL and publishes them on
``race.detected.events``. It runs outside the API process, and needs the race state
worker to be running. Run a single worker: the compare-and-set on the detection context
keeps several correct, but they contend for the same replay and nothing partitions
replays between them.
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
from app.detection.config import DetectionConfig
from app.detection.engine import DetectionEngine
from app.detection.processor import DetectionProcessor
from app.detection.registry import build_default_registry
from app.detection.repository import DatabaseDetectedEventSink
from app.detection.store import DetectionContextStore
from app.infrastructure.redis import RedisClient
from app.race_state.config import RaceStateConfig
from app.race_state.repository import RaceStateStore
from app.streaming.config import GROUP_EVENT_DETECTORS, StreamConfig
from app.streaming.consumer import StreamConsumer, make_consumer_name
from app.streaming.idempotency import RedisIdempotencyStore

logger = logging.getLogger(__name__)


def build_consumer(
    redis: RedisClient,
    session_factory: async_sessionmaker[AsyncSession],
    stream_config: StreamConfig,
    detection_config: DetectionConfig,
    state_config: RaceStateConfig,
) -> StreamConsumer:
    """The ``race-event-detectors`` consumer with its processor wired up."""
    engine = DetectionEngine(build_default_registry(detection_config), detection_config)
    processor = DetectionProcessor(
        engine,
        DetectionContextStore(redis, stream_config=stream_config, config=detection_config),
        RaceStateStore(redis, stream_config=stream_config, config=state_config),
        DatabaseDetectedEventSink(session_factory),
        config=detection_config,
    )
    return StreamConsumer(
        redis,
        stream=stream_config.state_stream,
        group=GROUP_EVENT_DETECTORS,
        consumer_name=make_consumer_name("event-detection"),
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
        DetectionConfig.from_settings(settings),
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
    logger.info("Event detection worker stopping")
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
