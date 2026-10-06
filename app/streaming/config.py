"""Stream, group and tuning configuration for the Redis Streams transport.

``StreamConfig`` is built from ``Settings`` so every component shares one source of
truth, and tests can pass isolated (uniquely named) streams via ``dataclasses.replace``.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.config import Settings

# Consumer groups. Each group independently receives every message on its stream.
GROUP_STATE_PROCESSORS = "race-state-processors"
GROUP_EVENT_DETECTORS = "race-event-detectors"  # reads the state stream
GROUP_EVENT_PERSISTERS = "event-persisters"
GROUP_WEBSOCKET_GATEWAY = "websocket-gateway"
GROUP_RAW_EVENT_AUDITORS = "raw-event-auditors"  # validation consumer


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Names and tuning for publishers and consumers."""

    raw_stream: str = "race.raw.events"
    state_stream: str = "race.state.events"  # reserved: derived race-state events
    detected_stream: str = "race.detected.events"  # reserved: detected analytical events
    dead_letter_stream: str = "race.dead_letter.events"
    maxlen: int = 100_000  # approximate retention of the raw stream; 0 = unbounded
    dead_letter_maxlen: int = 10_000
    read_count: int = 100
    block_ms: int = 5000
    max_deliveries: int = 5
    reclaim_idle_ms: int = 30_000
    publish_attempts: int = 3
    publish_backoff_seconds: float = 0.05
    idempotency_ttl_seconds: int = 86_400
    # ``$`` = only messages published after the group exists. Use ``0`` to also
    # receive the retained backlog when a group is created late.
    group_start_id: str = "$"
    supported_versions: frozenset[int] = frozenset({1})

    @classmethod
    def from_settings(cls, settings: Settings) -> StreamConfig:
        return cls(
            raw_stream=settings.stream_raw_events,
            state_stream=settings.stream_state_events,
            detected_stream=settings.stream_detected_events,
            dead_letter_stream=settings.stream_dead_letter,
            maxlen=settings.stream_maxlen,
            dead_letter_maxlen=settings.stream_dead_letter_maxlen,
            read_count=settings.stream_read_count,
            block_ms=settings.stream_block_ms,
            max_deliveries=settings.stream_max_deliveries,
            reclaim_idle_ms=settings.stream_reclaim_idle_ms,
            publish_attempts=settings.stream_publish_attempts,
            idempotency_ttl_seconds=settings.stream_idempotency_ttl_seconds,
        )
