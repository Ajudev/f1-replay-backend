"""Replay WebSocket route (thin; connection handling lives in the gateway)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, WebSocket

from app.gateway.gateway import WebSocketGateway

router = APIRouter(prefix="/replays/{replay_id}", tags=["Replays"])


def get_gateway(websocket: WebSocket) -> WebSocketGateway:
    return websocket.app.state.ws_gateway


@router.websocket("/stream", name="replay_stream")
async def replay_stream(websocket: WebSocket, replay_id: UUID) -> None:
    """Live replay updates: a ``SNAPSHOT`` first, then incremental messages.

    See the README "WebSocket API" section for the message contract.
    """
    await get_gateway(websocket).serve(websocket, replay_id)
