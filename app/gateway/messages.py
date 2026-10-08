"""WebSocket message contract and the pure translation from backend events.

Every message has the same envelope (``WsMessage``). Translation is a projection of
what the Race State Engine and the Detection Engine already published: nothing is
recomputed here. One state event can produce several messages; they share its
``sequence`` and are emitted in a fixed order (race-level changes, driver updates,
then change notifications), so the client's state is updated before it is notified.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError

from app.detection.models import StatePayload
from app.domain.enums import ReplayStatus
from app.race_state.models import DriverState, RaceState, StateEventType, StateSource
from app.replay.state import ReplayState
from app.schemas.detection import DetectedEventOut
from app.schemas.race_state import RaceStateResponse
from app.schemas.replays import ReplayResponse
from app.streaming.envelope import StreamEvent

WS_SCHEMA_VERSION = 1


class WsMessageType(StrEnum):
    # handshake / control
    SNAPSHOT = "SNAPSHOT"
    PONG = "PONG"
    ERROR = "ERROR"
    # replay lifecycle and progress (from the replay service in this process)
    REPLAY_STATUS = "REPLAY_STATUS"
    REPLAY_CLOCK = "REPLAY_CLOCK"
    REPLAY_COMPLETED = "REPLAY_COMPLETED"
    # race state (from race.state.events)
    RACE_STATE_SNAPSHOT = "RACE_STATE_SNAPSHOT"
    RACE_STATE_UPDATE = "RACE_STATE_UPDATE"
    DRIVER_UPDATE = "DRIVER_UPDATE"
    LAP_COMPLETED = "LAP_COMPLETED"
    POSITION_CHANGED = "POSITION_CHANGED"
    PIT_STATUS_CHANGED = "PIT_STATUS_CHANGED"
    TRACK_STATUS_CHANGED = "TRACK_STATUS_CHANGED"
    # detections (from race.detected.events)
    DETECTED_EVENT = "DETECTED_EVENT"


#: Messages that may be dropped under backpressure (the next one supersedes them).
DROPPABLE_TYPES = frozenset({WsMessageType.REPLAY_CLOCK})

#: Messages derived from race state events; a snapshot already covers those whose
#: ``sequence`` is not newer than its ``last_sequence``.
STATE_DERIVED_TYPES = frozenset(
    {
        WsMessageType.RACE_STATE_SNAPSHOT,
        WsMessageType.RACE_STATE_UPDATE,
        WsMessageType.DRIVER_UPDATE,
        WsMessageType.LAP_COMPLETED,
        WsMessageType.POSITION_CHANGED,
        WsMessageType.PIT_STATUS_CHANGED,
        WsMessageType.TRACK_STATUS_CHANGED,
    }
)

#: Messages scoped to a replay run (filtered against the replay's current run).
RUN_SCOPED_TYPES = STATE_DERIVED_TYPES | {WsMessageType.DETECTED_EVENT}

#: Fields left out of ``DRIVER_UPDATE`` (sent incrementally via ``LAP_COMPLETED``).
DRIVER_UPDATE_EXCLUDE = {"recent_laps"}


class WsMessage(BaseModel):
    """Envelope of every server → client message."""

    type: WsMessageType
    schema_version: int = WS_SCHEMA_VERSION
    replay_id: UUID
    run_id: UUID | None = Field(
        default=None, description="Replay run (changes on restart); null for lifecycle messages"
    )
    sequence: int | None = Field(
        default=None, description="Timeline sequence the message derives from"
    )
    race_time_ms: int | None = None
    lap_number: int | None = None
    emitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any] = Field(default_factory=dict)

    @property
    def droppable(self) -> bool:
        return self.type in DROPPABLE_TYPES

    def to_json(self) -> str:
        return self.model_dump_json()


# -- replay lifecycle -----------------------------------------------------------------------


def replay_status_messages(replay: ReplayResponse) -> list[WsMessage]:
    """``REPLAY_STATUS`` (plus ``REPLAY_COMPLETED`` once the replay completed)."""
    payload = {"replay": replay.model_dump(mode="json")}
    messages = [
        WsMessage(
            type=WsMessageType.REPLAY_STATUS,
            replay_id=replay.id,
            race_time_ms=replay.current_race_time_ms,
            lap_number=replay.current_lap,
            payload=payload,
        )
    ]
    if replay.status is ReplayStatus.COMPLETED:
        messages.append(
            WsMessage(
                type=WsMessageType.REPLAY_COMPLETED,
                replay_id=replay.id,
                race_time_ms=replay.current_race_time_ms,
                lap_number=replay.current_lap,
                payload=payload,
            )
        )
    return messages


def replay_clock_message(state: ReplayState) -> WsMessage:
    return WsMessage(
        type=WsMessageType.REPLAY_CLOCK,
        replay_id=state.replay_id,
        race_time_ms=state.current_race_time_ms,
        lap_number=state.current_lap,
        payload={
            "status": state.status.value,
            "current_race_time_ms": state.current_race_time_ms,
            "current_lap": state.current_lap,
            "total_laps": state.total_laps,
            "playback_speed": float(state.playback_speed),
            "emitted_event_count": state.emitted_event_count,
            "total_events": state.total_events,
        },
    )


# -- snapshot / control ------------------------------------------------------------------------


def snapshot_message(
    replay: ReplayResponse,
    state: RaceStateResponse | None,
    state_error: dict[str, Any] | None,
) -> WsMessage:
    """Initial (or resync) snapshot: replay status plus the full race state if any.

    ``state_error`` explains a missing state using the REST error body, e.g.
    ``REPLAY_NOT_STARTED`` or ``RACE_STATE_UNAVAILABLE``.
    """
    return WsMessage(
        type=WsMessageType.SNAPSHOT,
        replay_id=replay.id,
        run_id=state.run_id if state else None,
        sequence=state.last_sequence if state else None,
        race_time_ms=state.current_race_time_ms if state else replay.current_race_time_ms,
        lap_number=state.current_lap if state else replay.current_lap,
        payload={
            "replay": replay.model_dump(mode="json"),
            "state": state.model_dump(mode="json") if state else None,
            "state_error": state_error,
        },
    )


def error_message(replay_id: UUID, body: dict[str, Any]) -> WsMessage:
    """``ERROR`` carrying the same ``{code, message, details}`` body as REST errors."""
    return WsMessage(type=WsMessageType.ERROR, replay_id=replay_id, payload=body)


def pong_message(replay_id: UUID) -> WsMessage:
    return WsMessage(type=WsMessageType.PONG, replay_id=replay_id)


# -- stream translation ---------------------------------------------------------------------------


class UntranslatableEventError(Exception):
    """A stream entry does not match the published contract (it is skipped and logged)."""


def _base(event: StreamEvent, type_: WsMessageType, payload: dict[str, Any]) -> WsMessage:
    return WsMessage(
        type=type_,
        replay_id=event.replay_id,
        run_id=event.run_id,
        sequence=event.sequence,
        race_time_ms=event.race_time_ms,
        lap_number=event.lap_number,
        payload=payload,
    )


def _driver_ref(driver: DriverState) -> dict[str, Any]:
    return {"driver_id": str(driver.driver_id), "abbreviation": driver.abbreviation}


def translate_state_event(
    event: StreamEvent, replay_status: ReplayStatus | None
) -> list[WsMessage]:
    """Messages for one ``race.state.events`` entry."""
    try:
        payload = StatePayload.model_validate(event.payload)
    except ValidationError as exc:
        raise UntranslatableEventError(
            f"Invalid state event payload: {exc.error_count()} errors"
        ) from exc

    messages: list[WsMessage] = []
    if event.event_type != StateEventType.STATE_UPDATED.value:
        if payload.state is None:
            raise UntranslatableEventError(f"{event.event_type} without a full state")
        messages.append(
            _base(
                event,
                WsMessageType.RACE_STATE_SNAPSHOT,
                {
                    "reason": event.event_type,
                    "state": _state_response(payload.state, replay_status).model_dump(mode="json"),
                },
            )
        )

    changes = payload.changes
    if changes.race:
        messages.append(
            _base(
                event,
                WsMessageType.RACE_STATE_UPDATE,
                {
                    "kinds": payload.kinds,
                    "race": changes.race,
                    "last_sequence": payload.last_sequence,
                },
            )
        )
    by_id = {d.driver_id: d for d in changes.drivers}
    for driver in changes.drivers:
        messages.append(
            _base(
                event,
                WsMessageType.DRIVER_UPDATE,
                {"driver": driver.model_dump(mode="json", exclude=DRIVER_UPDATE_EXCLUDE)},
            )
        )

    kinds = set(payload.kinds)
    lap_driver = by_id.get(event.driver_id) if event.driver_id else None
    if "LAP_COMPLETED" in kinds and lap_driver is not None and lap_driver.recent_laps:
        messages.append(
            _base(
                event,
                WsMessageType.LAP_COMPLETED,
                {
                    **_driver_ref(lap_driver),
                    "lap": lap_driver.recent_laps[-1].model_dump(mode="json"),
                    "laps_completed": lap_driver.laps_completed,
                },
            )
        )
    for driver_id in changes.position_changes:
        driver = by_id.get(driver_id)
        if driver is not None:
            messages.append(
                _base(
                    event,
                    WsMessageType.POSITION_CHANGED,
                    {
                        **_driver_ref(driver),
                        "position": driver.position,
                        "previous_position": driver.previous_position,
                    },
                )
            )
    for driver_id in changes.pit_changes:
        driver = by_id.get(driver_id)
        if driver is not None:
            messages.append(
                _base(
                    event,
                    WsMessageType.PIT_STATUS_CHANGED,
                    {
                        **_driver_ref(driver),
                        "pit_status": driver.pit_status.value,
                        "pit_stop_count": driver.pit_stop_count,
                        "last_pit_lane_duration_ms": driver.last_pit_lane_duration_ms,
                    },
                )
            )
    if "TRACK_STATUS_CHANGED" in kinds:
        messages.append(
            _base(
                event,
                WsMessageType.TRACK_STATUS_CHANGED,
                {"track_status": changes.race.get("track_status")},
            )
        )
    return messages


def translate_detected_event(event: StreamEvent) -> WsMessage:
    """``DETECTED_EVENT`` with the same body as ``GET /replays/{id}/events``."""
    try:
        detected = DetectedEventOut.model_validate(event.payload)
    except ValidationError as exc:
        raise UntranslatableEventError(
            f"Invalid detected event payload: {exc.error_count()} errors"
        ) from exc
    return _base(event, WsMessageType.DETECTED_EVENT, {"event": detected.model_dump(mode="json")})


def _state_response(state: RaceState, replay_status: ReplayStatus | None) -> RaceStateResponse:
    # The state stream is fed by the same Redis document REST reads ("live").
    return RaceStateResponse.build(state, StateSource.LIVE, replay_status or ReplayStatus.RUNNING)
