"""Debugging CLI for the event streams.

    uv run python -m app.streaming.cli info
    uv run python -m app.streaming.cli tail --count 5
    uv run python -m app.streaming.cli pending --group raw-event-auditors
    uv run python -m app.streaming.cli dead-letters
    uv run python -m app.streaming.cli consume --group raw-event-auditors

``consume`` runs the validation consumer as its own process; consumers are never
started inside the API process.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import signal
import sys
from collections.abc import Sequence
from typing import Any, TextIO

from redis.exceptions import RedisError

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.redis import RedisClient
from app.streaming.audit import AuditHandler
from app.streaming.config import GROUP_RAW_EVENT_AUDITORS, StreamConfig
from app.streaming.consumer import StreamConsumer, make_consumer_name
from app.streaming.idempotency import RedisIdempotencyStore
from app.streaming.inspection import StreamInspector

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.streaming.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("info", help="length, first/last id and groups of every stream")

    tail = sub.add_parser("tail", help="show the latest entries of a stream")
    tail.add_argument("--stream", help="default: the raw events stream")
    tail.add_argument("--count", type=int, default=10)

    pending = sub.add_parser("pending", help="pending (unacknowledged) messages of a group")
    pending.add_argument("--stream", help="default: the raw events stream")
    pending.add_argument("--group", default=GROUP_RAW_EVENT_AUDITORS)
    pending.add_argument("--count", type=int, default=20)

    dead = sub.add_parser("dead-letters", help="list dead-lettered messages")
    dead.add_argument("--count", type=int, default=20)

    consume = sub.add_parser("consume", help="run the validation consumer")
    consume.add_argument("--stream", help="default: the raw events stream")
    consume.add_argument("--group", default=GROUP_RAW_EVENT_AUDITORS)
    return parser


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {k: _jsonable(v) for k, v in dataclasses.asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _emit(out: TextIO, value: Any) -> None:
    out.write(json.dumps(_jsonable(value), indent=2, sort_keys=True) + "\n")


async def run_command(
    args: argparse.Namespace,
    redis: RedisClient,
    config: StreamConfig,
    out: TextIO,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Execute one subcommand against ``redis`` (separated from ``main`` for tests)."""
    inspector = StreamInspector(redis, config.dead_letter_stream, config.dead_letter_maxlen)
    stream = getattr(args, "stream", None) or config.raw_stream

    if args.command == "info":
        report = {}
        for name in (
            config.raw_stream,
            config.state_stream,
            config.detected_stream,
            config.dead_letter_stream,
        ):
            report[name] = {
                "stream": await inspector.stream_info(name),
                "groups": await inspector.groups(name),
            }
        _emit(out, report)
    elif args.command == "tail":
        _emit(out, await inspector.tail(stream, args.count))
    elif args.command == "pending":
        _emit(
            out,
            {
                "summary": await inspector.pending_summary(stream, args.group),
                "entries": await inspector.pending_entries(stream, args.group, args.count),
            },
        )
    elif args.command == "dead-letters":
        _emit(out, await inspector.dead_letters(args.count))
    elif args.command == "consume":
        consumer = StreamConsumer(
            redis,
            stream=stream,
            group=args.group,
            consumer_name=make_consumer_name("auditor"),
            handler=AuditHandler(),
            config=config,
            idempotency_store=RedisIdempotencyStore(redis, config.idempotency_ttl_seconds),
        )
        if stop_event is not None:
            asyncio.get_running_loop().create_task(_stop_when_set(consumer, stop_event))
        await consumer.run()
    else:  # pragma: no cover - argparse enforces the choices
        raise ValueError(f"Unknown command {args.command!r}")


async def _stop_when_set(consumer: StreamConsumer, stop_event: asyncio.Event) -> None:
    await stop_event.wait()
    consumer.stop()


async def _amain(args: argparse.Namespace) -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    redis = RedisClient(settings.redis_url)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    try:
        await run_command(
            args, redis, StreamConfig.from_settings(settings), sys.stdout, stop_event=stop_event
        )
    except RedisError as exc:
        logger.error("Redis error: %s", exc)
        sys.stderr.write(f"Redis error: {exc}\n")
        return 1
    finally:
        await redis.aclose()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
