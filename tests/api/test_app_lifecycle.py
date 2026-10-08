"""Real application wiring: lifespan (gateway start/stop) and CORS."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from app.gateway.gateway import WebSocketGateway
from app.main import create_app

ALLOWED = "http://localhost:3000"
DISALLOWED = "https://evil.example.com"


async def test_lifespan_starts_and_stops_the_gateway() -> None:
    application = create_app()

    async with application.router.lifespan_context(application):
        gateway = application.state.ws_gateway
        assert isinstance(gateway, WebSocketGateway)
        assert gateway._clock_task is not None and not gateway._clock_task.done()
        assert gateway.tail._task is not None and not gateway.tail._task.done()
        assert gateway.manager.connection_count() == 0

    assert gateway._clock_task.done() and gateway.tail._task.done()


async def test_gateway_is_wired_with_the_replay_service(client: AsyncClient) -> None:
    application = client._transport.app  # type: ignore[attr-defined]
    gateway = application.state.ws_gateway
    assert gateway.on_replay_change in application.state.replay_service._listeners


@pytest.fixture
async def cors_client() -> AsyncIterator[AsyncClient]:
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application), base_url="http://test"
    ) as http:
        yield http


async def test_preflight_from_an_allowed_origin_succeeds(cors_client: AsyncClient) -> None:
    response = await cors_client.options(
        "/api/v1/replays",
        headers={
            "Origin": ALLOWED,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == ALLOWED
    assert "POST" in response.headers["access-control-allow-methods"]


async def test_preflight_from_a_disallowed_origin_gets_no_allow_origin(
    cors_client: AsyncClient,
) -> None:
    response = await cors_client.options(
        "/api/v1/replays",
        headers={"Origin": DISALLOWED, "Access-Control-Request-Method": "POST"},
    )

    assert response.status_code == 400
    assert "access-control-allow-origin" not in response.headers


async def test_simple_requests_only_echo_allowed_origins(cors_client: AsyncClient) -> None:
    allowed = await cors_client.get("/health", headers={"Origin": ALLOWED})
    other = await cors_client.get("/health", headers={"Origin": DISALLOWED})

    assert allowed.headers["access-control-allow-origin"] == ALLOWED
    assert "access-control-allow-origin" not in other.headers


async def test_configured_origins_replace_the_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "https://app.example.com, https://b.example.com")
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application), base_url="http://test"
    ) as http:
        ok = await http.get("/health", headers={"Origin": "https://b.example.com"})
        local = await http.get("/health", headers={"Origin": ALLOWED})

    assert ok.headers["access-control-allow-origin"] == "https://b.example.com"
    assert "access-control-allow-origin" not in local.headers
