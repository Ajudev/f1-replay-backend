"""WebSocket client connections and per-replay subscription groups.

Backpressure strategy: ``broadcast`` never awaits a socket. Each client has a bounded
queue drained by its own sender task, so one slow client cannot delay the stream tail,
the replay or other clients. When a queue is full:

- droppable messages (``REPLAY_CLOCK``) are discarded; the next tick supersedes them;
- any other message marks the client as too slow: it gets a final ``ERROR``
  (``CLIENT_TOO_SLOW``) and is disconnected rather than silently missing a state
  transition. It can reconnect and resynchronize from the initial snapshot.

A ``send`` that exceeds the configured timeout is treated the same way (dead client).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Callable, Iterable
from uuid import UUID, uuid4

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from app.gateway.messages import (
    RUN_SCOPED_TYPES,
    STATE_DERIVED_TYPES,
    WsMessage,
    WsMessageType,
    error_message,
)

logger = logging.getLogger(__name__)

#: WebSocket close codes (4000-4999 are application-defined).
CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_INTERNAL_ERROR = 1011
CLOSE_TRY_AGAIN_LATER = 1013
CLOSE_REPLAY_NOT_FOUND = 4404
CLOSE_TOO_SLOW = 4408


#: Replies to client messages that jump the queue.
CONTROL_TYPES = frozenset({WsMessageType.PONG, WsMessageType.ERROR})
MAX_PENDING_CONTROL = 8


class ClientConnection:
    """One connected client: bounded outgoing queue plus a sender task.

    Until ``activate`` is called the client is *pending*: broadcasts are buffered
    (bounded the same way) while the initial snapshot is being built, then replayed
    after it, skipping state messages the snapshot already covers. The same filter keeps
    applying afterwards, so a stream tail that lags behind the snapshot cannot deliver
    older state than the client already has.
    """

    def __init__(
        self,
        websocket: WebSocket,
        replay_id: UUID,
        *,
        queue_size: int,
        send_timeout: float,
        current_run: Callable[[UUID], UUID | None] = lambda _: None,
    ) -> None:
        self.id = uuid4()
        self.replay_id = replay_id
        self._ws = websocket
        self._queue_size = queue_size
        self._current_run = current_run
        self._send_timeout = send_timeout
        self._queue: deque[WsMessage] = deque()
        self._ready = asyncio.Event()
        self._active = False
        self._closed = False
        self._close_code = CLOSE_NORMAL
        self._sender: asyncio.Task[None] | None = None
        # Snapshot coverage, see ``_admit``: its run and the sequence it already covers.
        self._run_id: UUID | None = None
        self._after_sequence: int | None = None
        self.dropped = 0

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def queued(self) -> int:
        return len(self._queue)

    def start(self) -> None:
        self._sender = asyncio.create_task(self._send_loop(), name=f"ws-send-{self.id}")

    def offer(self, message: WsMessage) -> bool:
        """Queue ``message`` without waiting. False once the client has been dropped."""
        if self._closed:
            return False
        if self._active and not self._admit(message):
            return True  # (while pending, everything is buffered and filtered at activation)
        if len(self._queue) >= self._queue_size:
            if message.droppable:
                self.dropped += 1
                return True
            self._evict_droppable()
            if len(self._queue) >= self._queue_size:
                logger.warning(
                    "WebSocket client too slow; disconnecting client=%s replay_id=%s queued=%d",
                    self.id,
                    self.replay_id,
                    len(self._queue),
                )
                self.close(CLOSE_TOO_SLOW, final=error_message(self.replay_id, _too_slow_body()))
                return False
        self._queue.append(message)
        if self._active:
            self._ready.set()
        return True

    def _admit(self, message: WsMessage) -> bool:
        """Should this message reach the client, given the snapshot it already has?

        Run-scoped messages (state-derived and detections) are admitted only for the replay's
        *current* run, as reported by the replay service at delivery time: entries of an older
        run that the stream tail had not read yet are stale, whatever they announce. When the
        replay has not run in this process, the snapshot's run is followed instead. State
        messages of the snapshot's own run at or below its sequence are already covered by it.
        No wall clock is involved.
        """
        if message.type not in RUN_SCOPED_TYPES:
            return True
        current = self._current_run(self.replay_id) or self._run_id
        if current is None or message.run_id != current:
            return False
        return not (
            message.type in STATE_DERIVED_TYPES
            and message.run_id == self._run_id
            and self._after_sequence is not None
            and message.sequence is not None
            and message.sequence <= self._after_sequence
        )

    def activate(
        self,
        first: WsMessage,
        *,
        run_id: UUID | None,
        after_sequence: int | None,
    ) -> None:
        """Send ``first`` (the snapshot), then the buffered messages it does not cover."""
        self._run_id, self._after_sequence = run_id, after_sequence
        buffered = list(self._queue)
        self._queue.clear()
        self._queue.append(first)
        for message in buffered:
            if self._admit(message):
                self._queue.append(message)
        self._active = True
        self._ready.set()

    def deactivate(self) -> None:
        """Back to pending (buffering) until the next ``activate``, e.g. for a resync."""
        if not self._closed:
            self._active = False
            self._ready.clear()
            self._run_id = None
            self._after_sequence = None

    def send_now(self, message: WsMessage) -> None:
        """Queue ahead of everything else (control replies such as ``PONG``).

        Bounded: at most ``MAX_PENDING_CONTROL`` control replies wait at once (a ``PONG`` is
        coalesced into a pending one) and they never push the queue past its bound; extra
        replies from a flooding client are dropped.
        """
        if self._closed:
            return
        pending = sum(1 for m in self._queue if m.type in CONTROL_TYPES)
        if (
            pending >= MAX_PENDING_CONTROL
            or len(self._queue) >= self._queue_size
            or (
                message.type is WsMessageType.PONG
                and any(m.type is WsMessageType.PONG for m in self._queue)
            )
        ):
            self.dropped += 1
            return
        self._queue.appendleft(message)
        if self._active:
            self._ready.set()

    def close(self, code: int = CLOSE_NORMAL, *, final: WsMessage | None = None) -> None:
        """Stop accepting messages; the sender flushes ``final`` (if any) and closes."""
        if self._closed:
            return
        self._closed = True
        self._close_code = code
        self._queue.clear()
        if final is not None:
            self._queue.append(final)
        self._active = True
        self._ready.set()

    async def wait_closed(self) -> None:
        if self._sender is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._sender

    async def _send_loop(self) -> None:
        try:
            while True:
                await self._ready.wait()
                while self._queue and self._active:
                    message = self._queue.popleft()
                    await asyncio.wait_for(
                        self._ws.send_text(message.to_json()), timeout=self._send_timeout
                    )
                if self._closed:
                    break
                self._ready.clear()
        except TimeoutError:
            logger.warning(
                "WebSocket send timed out; disconnecting client=%s replay_id=%s",
                self.id,
                self.replay_id,
            )
            self._closed = True
            self._close_code = CLOSE_TOO_SLOW
        except Exception as exc:  # the socket is gone (disconnect, reset, closed)
            logger.debug("WebSocket send failed client=%s: %s", self.id, exc)
            self._closed = True
        finally:
            self._queue.clear()
            await self._close_socket()

    async def _close_socket(self) -> None:
        if self._ws.application_state is WebSocketState.CONNECTED:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._ws.close(self._close_code), self._send_timeout)

    def _evict_droppable(self) -> None:
        kept = [m for m in self._queue if not m.droppable]
        self.dropped += len(self._queue) - len(kept)
        self._queue = deque(kept)


def _too_slow_body() -> dict[str, object]:
    return {
        "code": "CLIENT_TOO_SLOW",
        "message": "The client could not keep up with replay updates; reconnect to resynchronize",
        "details": None,
    }


class ConnectionManager:
    """Replay-scoped subscription groups. A message for replay A is only ever offered
    to connections registered under replay A."""

    def __init__(self) -> None:
        self._groups: dict[UUID, dict[UUID, ClientConnection]] = {}

    def register(self, connection: ClientConnection) -> None:
        self._groups.setdefault(connection.replay_id, {})[connection.id] = connection
        logger.info(
            "WebSocket client connected client=%s replay_id=%s subscribers=%d",
            connection.id,
            connection.replay_id,
            len(self._groups[connection.replay_id]),
        )

    def unregister(self, connection: ClientConnection) -> None:
        group = self._groups.get(connection.replay_id)
        if group is None or group.pop(connection.id, None) is None:
            return
        if not group:
            del self._groups[connection.replay_id]
        logger.info(
            "WebSocket client disconnected client=%s replay_id=%s subscribers=%d",
            connection.id,
            connection.replay_id,
            len(group),
        )

    def has_subscribers(self, replay_id: UUID) -> bool:
        return replay_id in self._groups

    def replay_ids(self) -> list[UUID]:
        return list(self._groups)

    def connection_count(self, replay_id: UUID | None = None) -> int:
        if replay_id is not None:
            return len(self._groups.get(replay_id, {}))
        return sum(len(group) for group in self._groups.values())

    def broadcast(self, replay_id: UUID, messages: Iterable[WsMessage]) -> None:
        """Offer ``messages`` (in order) to every subscriber of ``replay_id``; never waits."""
        group = self._groups.get(replay_id)
        if not group:
            return
        batch = list(messages)
        for connection in list(group.values()):
            for message in batch:
                if not connection.offer(message):
                    break

    async def close_all(self, code: int = CLOSE_GOING_AWAY) -> None:
        connections = [c for group in self._groups.values() for c in group.values()]
        for connection in connections:
            connection.close(code)
        await asyncio.gather(*(c.wait_closed() for c in connections), return_exceptions=True)
        self._groups.clear()
