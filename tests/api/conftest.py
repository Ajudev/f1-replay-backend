"""Full application environment for API and WebSocket tests.

Real services on SQLite + fakeredis: the replay service publishes to the raw stream,
the race state and detection consumers are drained by hand, and the WebSocket gateway's
stream tail and clock are stepped explicitly (``read_once`` / ``tick``) so every test
is deterministic. WebSocket clients talk to the real ASGI route in this event loop.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.detection.config import DetectionConfig
from app.detection.worker import build_consumer as build_detection_consumer
from app.gateway.config import GatewayConfig
from app.gateway.gateway import WebSocketGateway
from app.infrastructure.redis import RedisClient
from app.main import create_app
from app.race_state.config import RaceStateConfig
from app.race_state.repository import RaceStateStore
from app.race_state.service import RaceStateService
from app.race_state.worker import build_consumer as build_state_consumer
from app.replay.service import ReplayService
from app.services.timing import ReplayTimingService
from app.streaming.config import StreamConfig
from app.streaming.consumer import StreamConsumer
from app.streaming.publisher import RedisStreamPublisher
from app.timeline.source import TimelineSource
from tests.replay.conftest import seed_with_timeline
from tests.replay.fakes import ManualTimer, wait_until_idle
from tests.streaming.conftest import cleanup_streams, config, redis_client  # noqa: F401
from tests.timeline.factories import representative_race

API = "/api/v1"


class WsClosed(Exception):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"WebSocket closed with code {code}")


class WsClient:
    """Minimal in-loop ASGI WebSocket client.

    ``send_gate``: when set to a cleared ``asyncio.Event``, every server ``send`` blocks
    until it is set (a slow browser). ``fail_sends``: every server ``send`` raises
    (a broken connection).
    """

    def __init__(
        self,
        app: FastAPI,
        path: str,
        *,
        send_gate: asyncio.Event | None = None,
        fail_sends: bool = False,
    ) -> None:
        self._app = app
        self._path = path
        self._to_app: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._from_app: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._send_gate = send_gate
        self._fail_sends = fail_sends
        self._task: asyncio.Task[None] | None = None
        self.close_code: int | None = None

    def _scope(self) -> dict[str, Any]:
        return {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "scheme": "ws",
            "path": self._path,
            "raw_path": self._path.encode(),
            "root_path": "",
            "query_string": b"",
            "headers": [(b"host", b"test")],
            "client": ("test", 1),
            "server": ("test", 80),
            "subprotocols": [],
            "state": {},
        }

    async def connect(self) -> WsClient:
        scope = self._scope()
        await self._to_app.put({"type": "websocket.connect"})
        self._task = asyncio.create_task(self._app(scope, self._to_app.get, self._send))
        accept = await asyncio.wait_for(self._from_app.get(), 2)
        assert accept["type"] == "websocket.accept", accept
        return self

    async def rejected(self) -> int:
        """Connect to a route the server refuses before accepting; returns the close code."""
        scope = self._scope()
        await self._to_app.put({"type": "websocket.connect"})
        self._task = asyncio.create_task(self._app(scope, self._to_app.get, self._send))
        message = await asyncio.wait_for(self._from_app.get(), 2)
        assert message["type"] == "websocket.close", message
        await self.finished()
        return int(message.get("code", 1000))

    async def _send(self, message: dict[str, Any]) -> None:
        if message["type"] == "websocket.send":
            if self._fail_sends:
                raise OSError("connection reset by peer")
            if self._send_gate is not None:
                await self._send_gate.wait()
        if message["type"] == "websocket.close":
            self.close_code = message.get("code", 1000)
        await self._from_app.put(message)

    async def receive(self, timeout: float = 2.0) -> dict[str, Any]:
        message = await asyncio.wait_for(self._from_app.get(), timeout)
        if message["type"] == "websocket.close":
            raise WsClosed(message.get("code", 1000))
        return json.loads(message["text"])

    async def until_closed(self, timeout: float = 2.0) -> tuple[list[dict[str, Any]], int]:
        """Everything up to the server's close frame, and its close code."""
        received: list[dict[str, Any]] = []
        while True:
            try:
                received.append(await self.receive(timeout))
            except WsClosed as closed:
                return received, closed.code

    async def messages(self) -> list[dict[str, Any]]:
        """Everything the server has sent so far (lets pending sender tasks run first)."""
        await settle()
        received: list[dict[str, Any]] = []
        while not self._from_app.empty():
            message = self._from_app.get_nowait()
            if message["type"] == "websocket.send":
                received.append(json.loads(message["text"]))
        return received

    async def send_json(self, document: object) -> None:
        await self._to_app.put({"type": "websocket.receive", "text": json.dumps(document)})

    async def disconnect(self, code: int = 1000) -> None:
        await self._to_app.put({"type": "websocket.disconnect", "code": code})
        await self.finished()

    async def finished(self, timeout: float = 2.0) -> None:
        assert self._task is not None
        await asyncio.wait_for(asyncio.shield(self._task), timeout)

    @property
    def done(self) -> bool:
        return self._task is not None and self._task.done()


async def settle(rounds: int = 50) -> None:
    for _ in range(rounds):
        await asyncio.sleep(0)


@dataclass
class ApiEnv:
    app: FastAPI
    client: AsyncClient
    factory: async_sessionmaker[AsyncSession]
    redis: RedisClient
    stream_config: StreamConfig
    timer: ManualTimer
    replays: ReplayService
    race_state: RaceStateService
    gateway: WebSocketGateway
    state_consumer: StreamConsumer
    detection_consumer: StreamConsumer
    race: TimelineSource
    other_race: TimelineSource
    clients: list[WsClient] = field(default_factory=list)

    async def create_replay(self, race: TimelineSource | None = None, speed: int = 20) -> UUID:
        response = await self.client.post(
            f"{API}/replays",
            json={"session_id": str((race or self.race).session_id), "playback_speed": speed},
        )
        assert response.status_code == 201, response.text
        return UUID(response.json()["id"])

    async def start(self, replay_id: UUID) -> None:
        response = await self.client.post(f"{API}/replays/{replay_id}/start")
        assert response.status_code == 200, response.text

    async def advance(self, seconds: float) -> None:
        """Move the replay clock, then process everything it published."""
        await self.timer.advance(seconds)
        await settle()
        await self.drain()

    async def finish(self) -> None:
        await self.timer.advance(100_000)
        await wait_until_idle(self.replays)
        await self.drain()

    async def drain(self) -> None:
        """Run the state and detection consumers dry, then deliver to WebSockets."""
        for consumer in (self.state_consumer, self.detection_consumer):
            for _ in range(200):
                if not await consumer.process_batch():
                    break
        while await self.gateway.tail.read_once():
            pass
        await settle()

    async def ws(self, replay_id: UUID | str, **kwargs: Any) -> WsClient:
        client = await WsClient(self.app, f"{API}/replays/{replay_id}/stream", **kwargs).connect()
        self.clients.append(client)
        return client


@pytest.fixture
async def api_env(
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: RedisClient,  # noqa: F811
    config: StreamConfig,  # noqa: F811
) -> AsyncIterator[ApiEnv]:
    race = representative_race()
    other = replace(representative_race(), season=2023, round=10)
    await seed_with_timeline(session_factory, race)
    await seed_with_timeline(session_factory, other)

    stream_config = replace(config, block_ms=1)
    timer = ManualTimer()
    replays = ReplayService(
        session_factory,
        sink=RedisStreamPublisher(redis_client, stream_config),
        timer=timer,
        stop_timeout=0.5,
    )
    state_config = RaceStateConfig(snapshot_every_laps=2)
    race_state = RaceStateService(
        RaceStateStore(redis_client, stream_config=stream_config, config=state_config),
        session_factory,
        replays,
    )
    state_consumer = build_state_consumer(
        redis_client, session_factory, stream_config, state_config
    )
    detection_consumer = build_detection_consumer(
        redis_client, session_factory, stream_config, DetectionConfig(), RaceStateConfig()
    )
    await state_consumer.ensure_group()
    await detection_consumer.ensure_group()
    gateway = WebSocketGateway(
        redis=redis_client,
        stream_config=stream_config,
        replay_service=replays,
        race_state_service=race_state,
        config=GatewayConfig(client_queue_size=64, send_timeout_seconds=0.5, stream_block_ms=1),
    )
    await gateway.start(tail=False, clock=False)
    await gateway.tail.read_once()  # position the tail at the (empty) streams' end

    application = create_app()
    application.state.session_factory = session_factory
    application.state.replay_service = replays
    application.state.race_state_service = race_state
    application.state.timing_service = ReplayTimingService(replays, session_factory)
    application.state.ws_gateway = gateway

    async with AsyncClient(
        transport=ASGITransport(app=application), base_url="http://test"
    ) as client:
        env = ApiEnv(
            app=application,
            client=client,
            factory=session_factory,
            redis=redis_client,
            stream_config=stream_config,
            timer=timer,
            replays=replays,
            race_state=race_state,
            gateway=gateway,
            state_consumer=state_consumer,
            detection_consumer=detection_consumer,
            race=race,
            other_race=other,
        )
        yield env
        for ws in env.clients:
            if not ws.done:
                await ws.disconnect()
    await gateway.stop()
    await replays.shutdown()
    keys = [k async for k in redis_client.client.scan_iter(match="race:*")]
    keys += [k async for k in redis_client.client.scan_iter(match="detection:*")]
    if keys:
        await redis_client.client.delete(*keys)
