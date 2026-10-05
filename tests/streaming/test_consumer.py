"""Consumer-group behaviour: ack, retry, reclaim, dead-letter, idempotency, scaling."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from app.infrastructure.redis import RedisClient
from app.streaming.config import StreamConfig
from app.streaming.consumer import make_consumer_name
from app.streaming.dead_letter import DeadLetterQueue
from app.streaming.idempotency import InMemoryIdempotencyStore, RedisIdempotencyStore
from app.streaming.inspection import StreamInspector
from app.streaming.publisher import RedisStreamPublisher
from tests.streaming.conftest import GROUP, RecordingHandler, make_consumer, make_event, raw


async def publish(redis: RedisClient, config: StreamConfig, count: int, start: int = 0, **kw):
    publisher = RedisStreamPublisher(redis, config)
    return [await publisher.publish_event(make_event(i, **kw)) for i in range(start, start + count)]


async def pending_count(redis: RedisClient, config: StreamConfig, group: str = GROUP) -> int:
    return (
        await StreamInspector(redis, config.dead_letter_stream).pending_summary(
            config.raw_stream, group
        )
    ).pending


async def test_ensure_group_is_idempotent(redis_client: RedisClient, config: StreamConfig) -> None:
    consumer = make_consumer(redis_client, config, RecordingHandler())
    await consumer.ensure_group()
    await consumer.ensure_group()
    groups = await StreamInspector(redis_client, config.dead_letter_stream).groups(
        config.raw_stream
    )
    assert [g.name for g in groups] == [GROUP]


async def test_read_decode_process_and_ack(redis_client: RedisClient, config: StreamConfig) -> None:
    handler = RecordingHandler()
    consumer = make_consumer(redis_client, config, handler)
    await consumer.ensure_group()
    message_ids = await publish(redis_client, config, 3)

    await consumer.process_batch()

    assert handler.sequences == [0, 1, 2]
    assert [m.message_id for m in handler.handled] == message_ids
    assert all(m.delivery_count == 1 for m in handler.handled)
    assert all(str(m.event.event_id) != m.message_id for m in handler.handled)
    assert consumer.stats.processed == 3
    assert await pending_count(redis_client, config) == 0


async def test_empty_stream_read_does_not_busy_loop(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    consumer = make_consumer(redis_client, config, RecordingHandler())
    await consumer.ensure_group()
    assert await consumer.process_batch() == 0


async def test_handler_failure_leaves_message_pending_then_reclaim_succeeds(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    handler = RecordingHandler(fail_times={1: 1})
    consumer = make_consumer(redis_client, config, handler)
    await consumer.ensure_group()
    await publish(redis_client, config, 3)

    await consumer.process_batch(reclaim=False)
    assert handler.sequences == [0, 2]  # the failing message did not block 2
    assert consumer.stats.failed == 1
    assert await pending_count(redis_client, config) == 1

    await consumer.process_batch()  # reclaims sequence 1 (idle threshold 0)
    assert handler.sequences == [0, 2, 1]
    retried = handler.handled[-1]
    assert retried.delivery_count == 2
    assert consumer.stats.reclaimed == 1 and consumer.stats.retried == 1
    assert await pending_count(redis_client, config) == 0


async def test_max_deliveries_moves_message_to_dead_letter(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    handler = RecordingHandler(fail_for={1})
    consumer = make_consumer(redis_client, config, handler)
    await consumer.ensure_group()
    message_ids = await publish(redis_client, config, 2)

    for _ in range(config.max_deliveries + 2):
        await consumer.process_batch()

    assert handler.sequences == [0]
    assert consumer.stats.dead_lettered == 1
    assert await pending_count(redis_client, config) == 0
    letters = await DeadLetterQueue(redis_client, config.dead_letter_stream, 0).recent()
    assert len(letters) == 1
    letter = letters[0]
    assert letter.original_stream == config.raw_stream
    assert letter.original_message_id == message_ids[1]
    assert letter.group == GROUP and letter.consumer == "consumer-1"
    assert letter.delivery_count == config.max_deliveries + 1
    assert "Delivery limit exceeded" in letter.reason and "permanent failure" in letter.reason
    assert letter.failed_at.endswith("+00:00")
    assert '"sequence":1' in letter.data
    assert letter.event_id is not None


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({"data": "{broken"}, "MalformedEventError"),
        ({"other": "x"}, "MalformedEventError"),
        ({"data": '{"schema_version":99}'}, "UnsupportedSchemaVersionError"),
    ],
)
async def test_invalid_message_is_dead_lettered_immediately_without_blocking_others(
    redis_client: RedisClient, config: StreamConfig, fields: dict[str, str], reason: str
) -> None:
    handler = RecordingHandler()
    consumer = make_consumer(redis_client, config, handler)
    await consumer.ensure_group()
    await raw(redis_client).xadd(config.raw_stream, fields)
    await publish(redis_client, config, 2)

    await consumer.process_batch()

    assert handler.sequences == [0, 1]
    assert consumer.stats.dead_lettered == 1 and consumer.stats.failed == 0
    assert await pending_count(redis_client, config) == 0
    (letter,) = await DeadLetterQueue(redis_client, config.dead_letter_stream, 0).recent()
    assert letter.delivery_count == 1 and reason in letter.reason
    assert letter.data == fields.get("data", "")


async def test_dead_letter_stream_is_trimmed_and_reason_truncated(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    maxlen, total = 10, 400
    queue = DeadLetterQueue(redis_client, config.dead_letter_stream, maxlen=maxlen)
    await raw(redis_client).xgroup_create(config.raw_stream, GROUP, id="0", mkstream=True)
    for _ in range(total):
        message_id = await raw(redis_client).xadd(config.raw_stream, {"data": "x"})
        await queue.move(
            stream=config.raw_stream,
            group=GROUP,
            consumer="c",
            message_id=message_id,
            fields={"data": "x"},
            reason="r" * 2000,
            delivery_count=1,
        )
    letters = await queue.recent(total)
    # Approximate trimming (real Redis) may keep more than maxlen, never fewer.
    assert maxlen <= len(letters) < total
    assert len(letters[0].reason) == 500


async def test_idempotency_skips_duplicate_events(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    handler = RecordingHandler()
    consumer = make_consumer(redis_client, config, handler, store=InMemoryIdempotencyStore())
    await consumer.ensure_group()
    event = make_event(1)
    publisher = RedisStreamPublisher(redis_client, config)
    await publisher.publish(event)
    await publisher.publish(event)  # same logical event, same event_id

    await consumer.process_batch()

    assert handler.sequences == [1]
    assert consumer.stats.duplicates_skipped == 1
    assert await pending_count(redis_client, config) == 0


async def test_redis_idempotency_store_is_namespaced_by_group(
    redis_client: RedisClient,
) -> None:
    store = RedisIdempotencyStore(redis_client, ttl_seconds=60)
    event_id = uuid4()
    assert not await store.is_processed("g1", event_id)
    await store.mark_processed("g1", event_id)
    await store.mark_processed("g1", event_id)
    assert await store.is_processed("g1", event_id)
    assert not await store.is_processed("g2", event_id)
    assert 0 < await raw(redis_client).ttl(store.key("g1", event_id)) <= 60


async def test_handler_failure_does_not_mark_processed(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    store = InMemoryIdempotencyStore()
    handler = RecordingHandler(fail_times={0: 1})
    consumer = make_consumer(redis_client, config, handler, store=store)
    await consumer.ensure_group()
    await publish(redis_client, config, 1)
    await consumer.process_batch(reclaim=False)
    await consumer.process_batch()
    assert handler.sequences == [0]  # retried, not skipped as a duplicate
    assert consumer.stats.duplicates_skipped == 0


async def test_consumers_in_one_group_split_messages(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    h1, h2 = RecordingHandler(), RecordingHandler()
    c1 = make_consumer(redis_client, config, h1, name="a")
    c2 = make_consumer(redis_client, config, h2, name="b")
    await c1.ensure_group()
    await publish(redis_client, config, 10)

    c1_config = replace(config, read_count=3)
    c1._config = c2._config = c1_config  # type: ignore[attr-defined]
    await c1.process_batch()
    await c2.process_batch()
    await c1.process_batch()
    await c2.process_batch()

    combined = sorted(h1.sequences + h2.sequences)
    assert combined == list(range(10))  # every message exactly once overall
    assert h1.sequences and h2.sequences


async def test_each_group_receives_every_message(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    h1, h2 = RecordingHandler(), RecordingHandler()
    c1 = make_consumer(redis_client, config, h1, group="group-one")
    c2 = make_consumer(redis_client, config, h2, group="group-two")
    await c1.ensure_group()
    await c2.ensure_group()
    await publish(redis_client, config, 5)
    await c1.process_batch()
    await c2.process_batch()
    assert h1.sequences == h2.sequences == [0, 1, 2, 3, 4]


async def test_failing_group_does_not_affect_other_group(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    broken, healthy = RecordingHandler(fail_for=None), RecordingHandler()
    cb = make_consumer(redis_client, config, broken, group="broken")
    ch = make_consumer(redis_client, config, healthy, group="healthy")
    await cb.ensure_group()
    await ch.ensure_group()
    await publish(redis_client, config, 3)
    await cb.process_batch()
    await ch.process_batch()
    assert healthy.sequences == [0, 1, 2]
    assert await pending_count(redis_client, config, "healthy") == 0
    assert await pending_count(redis_client, config, "broken") == 3


async def test_crashed_consumer_pending_messages_are_reclaimed_by_another(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    crashed = make_consumer(redis_client, config, RecordingHandler(fail_for=None), name="a")
    await crashed.ensure_group()
    await publish(redis_client, config, 3)
    await crashed.process_batch(reclaim=False)  # delivered to "a", never acked
    inspector = StreamInspector(redis_client, config.dead_letter_stream)
    summary = await inspector.pending_summary(config.raw_stream, GROUP)
    assert summary.per_consumer == {"a": 3}

    handler = RecordingHandler()
    survivor = make_consumer(redis_client, config, handler, name="b")
    await survivor.process_batch()

    assert handler.sequences == [0, 1, 2]
    assert await pending_count(redis_client, config) == 0


async def test_pending_messages_are_not_reclaimed_before_idle_timeout(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    slow = replace(config, reclaim_idle_ms=60_000)
    a = make_consumer(redis_client, slow, RecordingHandler(fail_for=None), name="a")
    await a.ensure_group()
    await publish(redis_client, slow, 1)
    await a.process_batch(reclaim=False)
    handler = RecordingHandler()
    await make_consumer(redis_client, slow, handler, name="b").process_batch()
    assert handler.handled == []
    assert await pending_count(redis_client, slow) == 1


async def test_restart_with_same_name_processes_own_pending(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    slow = replace(config, reclaim_idle_ms=60_000)
    before = make_consumer(redis_client, slow, RecordingHandler(fail_for=None), name="stable")
    await before.ensure_group()
    await publish(redis_client, slow, 2)
    await before.process_batch(reclaim=False)

    handler = RecordingHandler()
    after = make_consumer(redis_client, slow, handler, name="stable")
    assert await after.process_own_pending() == 2
    assert handler.sequences == [0, 1]
    assert await pending_count(redis_client, slow) == 0


async def test_run_loop_consumes_until_stopped(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    handler = RecordingHandler()
    consumer = make_consumer(redis_client, config, handler)
    task = asyncio.create_task(consumer.run())
    try:
        await publish(redis_client, config, 20)
        async with asyncio.timeout(2):
            while len(handler.handled) < 20:
                await asyncio.sleep(0.01)
    finally:
        consumer.stop()
        async with asyncio.timeout(2):
            await task
    assert handler.sequences == list(range(20))
    assert await pending_count(redis_client, config) == 0


async def test_consumers_receive_published_order(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    handler = RecordingHandler()
    consumer = make_consumer(redis_client, config, handler)
    await consumer.ensure_group()
    await publish(redis_client, config, 1000)
    while len(handler.handled) < 1000:
        await consumer.process_batch()
    assert handler.sequences == list(range(1000))


def test_consumer_names_are_unique() -> None:
    names = {make_consumer_name("auditor") for _ in range(20)}
    assert len(names) == 20
    assert all(n.startswith("auditor-") for n in names)


class CrashOnceStore(InMemoryIdempotencyStore):
    """Models a crash after the handler finished but before the event is marked."""

    def __init__(self) -> None:
        super().__init__()
        self.crashed = False

    async def mark_processed(self, group: str, event_id: UUID) -> None:
        if not self.crashed:
            self.crashed = True
            raise RuntimeError("crash before mark_processed")
        await super().mark_processed(group, event_id)


async def test_crash_after_handler_before_ack_redelivers_and_reruns_handler(
    redis_client: RedisClient, config: StreamConfig
) -> None:
    """At-least-once: handlers must be idempotent because they can run twice."""
    handler = RecordingHandler()
    consumer = make_consumer(redis_client, config, handler, store=CrashOnceStore())
    await consumer.ensure_group()
    await publish(redis_client, config, 1)

    with pytest.raises(RuntimeError):
        await consumer.process_batch(reclaim=False)
    assert handler.sequences == [0]  # handler finished
    assert await pending_count(redis_client, config) == 1  # but was never acknowledged

    await consumer.process_batch()  # redelivered after the idle timeout
    assert handler.sequences == [0, 0]
    assert handler.handled[-1].delivery_count == 2
    assert await pending_count(redis_client, config) == 0
