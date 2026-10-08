"""Versioned public API: the only place the version prefix is defined."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.detection import router as events_router
from app.api.race_state import router as race_state_router
from app.api.races import import_router
from app.api.races import router as races_router
from app.api.replays import router as replays_router
from app.api.stream import router as stream_router
from app.api.timeline import router as timeline_router
from app.api.timing import router as timing_router

API_V1_PREFIX = "/api/v1"

api_router = APIRouter(prefix=API_V1_PREFIX)
api_router.include_router(races_router)
api_router.include_router(replays_router)
api_router.include_router(race_state_router)
api_router.include_router(events_router)
api_router.include_router(timing_router)
api_router.include_router(stream_router)
api_router.include_router(import_router)
api_router.include_router(timeline_router)

OPENAPI_TAGS = [
    {"name": "Races", "description": "Imported historical races, sessions, drivers and laps"},
    {"name": "Replays", "description": "Create and control replays; live WebSocket stream"},
    {"name": "Race State", "description": "Authoritative current race state of a replay"},
    {"name": "Events", "description": "Detected race events with structured evidence"},
    {"name": "Timing", "description": "Lap-by-lap data series for charts"},
    {"name": "Data Management", "description": "Operator endpoints: import and timeline build"},
    {"name": "Health", "description": "Liveness and readiness probes (unversioned)"},
]
