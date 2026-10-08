"""WebSocket gateway: delivers replay lifecycle, clock, race state and detections.

A delivery layer only. Race state comes from the Race State Engine (Redis document
for snapshots, ``race.state.events`` for deltas); detections from
``race.detected.events``; lifecycle from the in-process ``ReplayService``. Nothing is
recomputed and no second state copy is kept: the snapshot a client receives is built
by the same services as ``GET /replays/{id}`` and ``GET /replays/{id}/state``.

Connection flow: accept → register (pending, buffering) → build snapshot → send it,
then the buffered messages newer than it → live messages. Joining mid-race therefore
starts from the current state with no gap.

Replays never depend on browsers: no client, many clients or failing clients make no
difference to the replay runner or to the stream consumers.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any
from uuid import UUID

from fastapi import WebSocket, WebSocketDisconnect

from app.api.exceptions import error_body, resolve_error
from app.domain.enums import ReplayStatus
from app.gateway.config import GatewayConfig
from app.gateway.connections import (
    CLOSE_INTERNAL_ERROR,
    CLOSE_REPLAY_NOT_FOUND,
    CLOSE_TRY_AGAIN_LATER,
    ClientConnection,
    ConnectionManager,
)
from app.gateway.fanout import StreamTail
from app.gateway.messages import (
    UntranslatableEventError,
    error_message,
    pong_message,
    replay_clock_message,
    replay_status_messages,
    snapshot_message,
    translate_detected_event,
    translate_state_event,
)
from app.infrastructure.redis import RedisClient
from app.race_state.service import RaceStateService
from app.replay.errors import ReplayNotFoundError
from app.replay.service import ReplayService, ReplayView
from app.schemas.race_state import RaceStateResponse
from app.schemas.replays import ReplayResponse
from app.streaming.config import StreamConfig
from app.streaming.envelope import StreamEvent

logger = logging.getLogger(__name__)


class WebSocketGateway:
    def __init__(
        self,
        *,
        redis: RedisClient,
        stream_config: StreamConfig,
        replay_service: ReplayService,
        race_state_service: RaceStateService,
        config: GatewayConfig,
    ) -> None:
        self.manager = ConnectionManager()
        self.config = config
        self._replays = replay_service
        self._race_state = race_state_service
        self._state_stream = stream_config.state_stream
        self._detected_stream = stream_config.detected_stream
        self.tail = StreamTail(redis, stream_config, config, self.on_stream_event)
        # Last known lifecycle status of replays with subscribers (for state snapshots
        # of replays no longer running in this process, e.g. just completed).
        self._statuses: dict[UUID, ReplayStatus] = {}
        self._clock_task: asyncio.Task[None] | None = None

    # -- lifecycle -------------------------------------------------------------------

    async def start(self, *, tail: bool = True, clock: bool = True) -> None:
        self._replays.add_listener(self.on_replay_change)
        if tail:
            self.tail.start()
        if clock:
            self._clock_task = asyncio.create_task(self._clock_loop(), name="ws-replay-clock")

    async def stop(self) -> None:
        self._replays.remove_listener(self.on_replay_change)
        await self.tail.stop()
        if self._clock_task is not None:
            self._clock_task.cancel()
            await asyncio.gather(self._clock_task, return_exceptions=True)
        await self.manager.close_all()

    # -- inputs -----------------------------------------------------------------------

    def on_replay_change(self, view: ReplayView) -> None:
        """Replay service listener (lifecycle transitions and speed changes)."""
        replay_id = view.state.replay_id
        if not self.manager.has_subscribers(replay_id):
            return
        self._statuses[replay_id] = view.state.status
        self.manager.broadcast(replay_id, replay_status_messages(ReplayResponse.from_view(view)))

    async def on_stream_event(self, stream: str, event: StreamEvent) -> None:
        """Stream tail handler: translate and broadcast to the replay's subscribers."""
        if not self.manager.has_subscribers(event.replay_id):
            return
        try:
            if stream == self._state_stream:
                messages = translate_state_event(event, self._status_of(event.replay_id))
            elif stream == self._detected_stream:
                messages = [translate_detected_event(event)]
            else:
                return
        except UntranslatableEventError as exc:
            logger.warning(
                "Skipping event for WebSocket delivery replay_id=%s seq=%d type=%s: %s",
                event.replay_id,
                event.sequence,
                event.event_type,
                exc,
            )
            return
        self.manager.broadcast(event.replay_id, messages)

    def tick(self) -> None:
        """One ``REPLAY_CLOCK`` per running replay with subscribers (in-memory read)."""
        for replay_id in self.manager.replay_ids():
            state = self._replays.live_state(replay_id)
            if state is not None and state.status is ReplayStatus.RUNNING:
                self.manager.broadcast(replay_id, [replay_clock_message(state)])

    async def _clock_loop(self) -> None:
        interval = self.config.clock_interval_ms / 1000
        while True:
            await asyncio.sleep(interval)
            try:
                self.tick()
            except Exception:
                logger.exception("Replay clock tick failed")

    def _status_of(self, replay_id: UUID) -> ReplayStatus | None:
        live = self._replays.live_state(replay_id)
        return live.status if live is not None else self._statuses.get(replay_id)

    # -- one client --------------------------------------------------------------------

    async def serve(self, websocket: WebSocket, replay_id: UUID) -> None:
        """Run one client connection until it disconnects or is dropped."""
        await websocket.accept()
        connection = ClientConnection(
            websocket,
            replay_id,
            queue_size=self.config.client_queue_size,
            send_timeout=self.config.send_timeout_seconds,
            current_run=self._replays.current_run_id,
        )
        # Register before reading the snapshot so nothing published meanwhile is missed.
        self.manager.register(connection)
        connection.start()
        try:
            if not await self._snapshot(connection):
                return
            receiver = asyncio.create_task(self._receive_loop(websocket, connection))
            closed = asyncio.create_task(connection.wait_closed())
            await asyncio.wait({receiver, closed}, return_when=asyncio.FIRST_COMPLETED)
            for task in (receiver, closed):
                task.cancel()
            await asyncio.gather(receiver, closed, return_exceptions=True)
        finally:
            self.manager.unregister(connection)
            connection.close()
            await connection.wait_closed()
            if not self.manager.has_subscribers(replay_id):
                self._statuses.pop(replay_id, None)

    async def _snapshot(self, connection: ClientConnection) -> bool:
        """Build and send the snapshot; False (connection closed) if the replay is unusable."""
        replay_id = connection.replay_id
        try:
            view = await self._replays.get(replay_id)
        except Exception as exc:
            resolved = resolve_error(exc)
            if resolved is None:
                logger.exception("WebSocket snapshot failed replay_id=%s", replay_id)
                status, body = 500, error_body("INTERNAL_ERROR", "Internal server error")
            else:
                status, body = resolved
            code = (
                CLOSE_REPLAY_NOT_FOUND
                if isinstance(exc, ReplayNotFoundError)
                else CLOSE_TRY_AGAIN_LATER
            )
            logger.info("WebSocket rejected replay_id=%s status=%d", replay_id, status)
            connection.close(code, final=error_message(replay_id, body))
            return False

        self._statuses[replay_id] = view.state.status
        state: RaceStateResponse | None = None
        state_error: dict[str, Any] | None = None
        try:
            state_view = await self._race_state.get_state(replay_id)
            state = RaceStateResponse.build(
                state_view.state, state_view.source, state_view.replay_status
            )
        except Exception as exc:
            resolved = resolve_error(exc)
            if resolved is None:
                logger.exception("WebSocket snapshot failed replay_id=%s", replay_id)
                connection.close(
                    CLOSE_INTERNAL_ERROR,
                    final=error_message(
                        replay_id, error_body("INTERNAL_ERROR", "Internal server error")
                    ),
                )
                return False
            state_error = resolved[1]
        connection.activate(
            snapshot_message(ReplayResponse.from_view(view), state, state_error),
            run_id=state.run_id if state else None,
            after_sequence=state.last_sequence if state else None,
        )
        return True

    async def _receive_loop(self, websocket: WebSocket, connection: ClientConnection) -> None:
        """Client → server control messages: ``PING`` and ``RESYNC``."""
        with contextlib.suppress(WebSocketDisconnect, RuntimeError):
            while not connection.closed:
                frame = await websocket.receive()
                if frame["type"] == "websocket.disconnect":
                    return
                kind = _message_type(frame.get("text"))
                if kind == "PING":
                    connection.send_now(pong_message(connection.replay_id))
                elif kind == "RESYNC":
                    connection.deactivate()
                    if not await self._snapshot(connection):
                        return
                else:
                    connection.send_now(
                        error_message(
                            connection.replay_id,
                            error_body(
                                "UNSUPPORTED_CLIENT_MESSAGE",
                                'Supported client messages: {"type": "PING"}, {"type": "RESYNC"}',
                            ),
                        )
                    )


def _message_type(raw: str | None) -> str | None:
    if raw is None:  # binary frame
        return None
    try:
        document = json.loads(raw)
    except ValueError:
        return None
    kind = document.get("type") if isinstance(document, dict) else None
    return kind.upper() if isinstance(kind, str) else None
