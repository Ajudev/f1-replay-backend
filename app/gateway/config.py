"""WebSocket gateway tuning (built from ``Settings``)."""

from __future__ import annotations

from dataclasses import dataclass

from app.core.config import Settings


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    #: Bounded outgoing queue per client. A client that lets it fill up with
    #: non-droppable messages is disconnected (it can reconnect and resync).
    client_queue_size: int = 512
    #: A single ``send`` slower than this marks the client dead.
    send_timeout_seconds: float = 5.0
    #: ``REPLAY_CLOCK`` cadence for running replays with subscribers.
    clock_interval_ms: int = 1000
    #: ``XREAD BLOCK`` timeout of the stream tail.
    stream_block_ms: int = 1000
    #: Max entries per stream per ``XREAD``.
    stream_read_count: int = 200
    #: Backoff after a Redis failure in the stream tail.
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_settings(cls, settings: Settings) -> GatewayConfig:
        return cls(
            client_queue_size=settings.ws_client_queue_size,
            send_timeout_seconds=settings.ws_send_timeout_seconds,
            clock_interval_ms=settings.ws_clock_interval_ms,
            stream_block_ms=settings.ws_stream_block_ms,
        )
