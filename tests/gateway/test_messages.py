"""Translation of backend events into WebSocket messages (pure functions)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.detection.models import StateChanges, StatePayload
from app.domain.enums import ReplayStatus
from app.gateway.messages import (
    DROPPABLE_TYPES,
    STATE_DERIVED_TYPES,
    UntranslatableEventError,
    WsMessage,
    WsMessageType,
    error_message,
    pong_message,
    replay_clock_message,
    replay_status_messages,
    snapshot_message,
    translate_detected_event,
    translate_state_event,
)
from app.race_state.models import DriverState, LapRecord, PitStatus, RaceState
from app.replay.state import ReplayState
from app.schemas.race_state import RaceStateResponse
from app.schemas.replays import ReplayResponse
from app.streaming.envelope import StreamEvent

REPLAY, RUN, SESSION = UUID(int=1), UUID(int=2), UUID(int=3)
VER, HAM = UUID(int=10), UUID(int=11)


def stream_event(
    payload: dict[str, Any],
    *,
    event_type: str = "STATE_UPDATED",
    sequence: int = 7,
    driver_id: UUID | None = VER,
) -> StreamEvent:
    return StreamEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type=event_type,
        replay_id=REPLAY,
        run_id=RUN,
        session_id=SESSION,
        sequence=sequence,
        race_time_ms=90_000,
        lap_number=2,
        driver_id=driver_id,
        driver_abbreviation="VER" if driver_id == VER else None,
        published_at=datetime(2024, 1, 1, tzinfo=UTC),
        payload=payload,
    )


def lap(number: int = 1) -> LapRecord:
    return LapRecord(lap_number=number, lap_time_ms=90_000, race_time_ms=90_000 * number)


def driver(driver_id: UUID, abbreviation: str, **fields: Any) -> DriverState:
    return DriverState(driver_id=driver_id, abbreviation=abbreviation, **fields)


def state_payload(
    kinds: list[str],
    *,
    race: dict[str, Any] | None = None,
    drivers: list[DriverState] | None = None,
    position_changes: list[UUID] | None = None,
    pit_changes: list[UUID] | None = None,
    state: RaceState | None = None,
) -> dict[str, Any]:
    return StatePayload(
        source_event_id=uuid4(),
        source_event_type="LAP_COMPLETED",
        last_sequence=7,
        kinds=kinds,
        changes=StateChanges(
            race=race or {},
            drivers=drivers or [],
            position_changes=position_changes or [],
            pit_changes=pit_changes or [],
        ),
        state=state,
    ).model_dump(mode="json")


def types(messages: list[WsMessage]) -> list[str]:
    return [m.type.value for m in messages]


# -- state events ---------------------------------------------------------------------------


def test_lap_event_translates_in_documented_order() -> None:
    ver = driver(
        VER,
        "VER",
        position=2,
        previous_position=1,
        laps_completed=1,
        recent_laps=[lap(1)],
        pit_status=PitStatus.IN_PIT,
        pit_stop_count=1,
    )
    event = stream_event(
        state_payload(
            ["LAP_COMPLETED", "PIT_STATUS_CHANGED"],
            race={"current_lap": 2},
            drivers=[ver],
            position_changes=[VER],
            pit_changes=[VER],
        )
    )

    messages = translate_state_event(event, ReplayStatus.RUNNING)

    assert types(messages) == [
        "RACE_STATE_UPDATE",
        "DRIVER_UPDATE",
        "LAP_COMPLETED",
        "POSITION_CHANGED",
        "PIT_STATUS_CHANGED",
    ]
    assert all(m.sequence == 7 and m.run_id == RUN and m.replay_id == REPLAY for m in messages)
    assert all(m.race_time_ms == 90_000 and m.lap_number == 2 for m in messages)
    update, driver_update, lap_done, moved, pitted = (m.payload for m in messages)
    assert update == {
        "kinds": ["LAP_COMPLETED", "PIT_STATUS_CHANGED"],
        "race": {"current_lap": 2},
        "last_sequence": 7,
    }
    assert "recent_laps" not in driver_update["driver"]
    assert driver_update["driver"]["abbreviation"] == "VER"
    assert lap_done["lap"]["lap_number"] == 1 and lap_done["laps_completed"] == 1
    assert moved == {
        "driver_id": str(VER),
        "abbreviation": "VER",
        "position": 2,
        "previous_position": 1,
    }
    assert pitted["pit_status"] == "IN_PIT" and pitted["pit_stop_count"] == 1


def test_changes_without_a_driver_update_are_not_notified() -> None:
    event = stream_event(
        state_payload(["LAP_COMPLETED"], drivers=[driver(HAM, "HAM")], position_changes=[VER])
    )

    messages = translate_state_event(event, None)

    # VER has no entry in this delta; HAM is not the event's driver, so no LAP_COMPLETED.
    assert types(messages) == ["DRIVER_UPDATE"]


def test_track_status_notification_carries_the_new_status() -> None:
    event = stream_event(
        state_payload(["TRACK_STATUS_CHANGED"], race={"track_status": "SAFETY_CAR"}),
        driver_id=None,
    )

    messages = translate_state_event(event, None)

    assert types(messages) == ["RACE_STATE_UPDATE", "TRACK_STATUS_CHANGED"]
    assert messages[1].payload == {"track_status": "SAFETY_CAR"}


def test_full_state_events_add_a_race_state_snapshot_first() -> None:
    state = RaceState(replay_id=REPLAY, run_id=RUN, session_id=SESSION)
    event = stream_event(
        state_payload(["RACE_STARTED"], race={"phase": "RUNNING"}, state=state),
        event_type="STATE_INITIALIZED",
        driver_id=None,
    )

    messages = translate_state_event(event, ReplayStatus.RUNNING)

    assert types(messages)[:2] == ["RACE_STATE_SNAPSHOT", "RACE_STATE_UPDATE"]
    snapshot = messages[0].payload
    assert snapshot["reason"] == "STATE_INITIALIZED"
    assert snapshot["state"]["replay_status"] == "RUNNING" and snapshot["state"]["source"] == "live"


def test_full_state_event_without_state_is_untranslatable() -> None:
    event = stream_event(state_payload(["RACE_STARTED"]), event_type="STATE_REBUILT")
    with pytest.raises(UntranslatableEventError):
        translate_state_event(event, None)


def test_malformed_state_payload_is_untranslatable() -> None:
    with pytest.raises(UntranslatableEventError):
        translate_state_event(stream_event({"last_sequence": "x"}), None)


def test_every_translated_message_serializes_without_internal_fields() -> None:
    event = stream_event(
        state_payload(["LAP_COMPLETED"], drivers=[driver(VER, "VER", recent_laps=[lap()])])
    )
    for message in translate_state_event(event, None):
        document = json.loads(message.to_json())
        assert {"type", "schema_version", "replay_id", "run_id", "sequence", "payload"} <= set(
            document
        )
        assert "lap_crossings" not in message.to_json()


# -- detected events ----------------------------------------------------------------------------


def detected_payload() -> dict[str, Any]:
    return {
        "detected_event_id": str(uuid4()),
        "event_type": "BATTLE_FORMING",
        "schema_version": 1,
        "replay_id": str(REPLAY),
        "run_id": str(RUN),
        "session_id": str(SESSION),
        "race_id": None,
        "race_time_ms": 90_000,
        "lap_number": 2,
        "primary_driver_id": str(VER),
        "primary_driver_abbreviation": "VER",
        "secondary_driver_id": str(HAM),
        "secondary_driver_abbreviation": "HAM",
        "severity": "MEDIUM",
        "confidence": 0.8,
        "source_event_ids": [str(uuid4())],
        "source_sequence": 7,
        "detector_name": "battle",
        "detector_version": 1,
        "evidence": {"gap_ms": 500},
        "detected_at": "2024-01-01T00:00:00Z",
    }


def test_detected_event_message_wraps_the_rest_shape() -> None:
    payload = detected_payload()

    message = translate_detected_event(stream_event(payload, event_type="BATTLE_FORMING"))

    assert message.type is WsMessageType.DETECTED_EVENT
    assert message.payload["event"]["detected_event_id"] == payload["detected_event_id"]
    assert message.payload["event"]["evidence"] == {"gap_ms": 500}
    assert message.sequence == 7 and message.run_id == RUN


def test_malformed_detected_event_is_untranslatable() -> None:
    with pytest.raises(UntranslatableEventError):
        translate_detected_event(stream_event({"event_type": "NOPE"}))


# -- lifecycle, snapshot and control ---------------------------------------------------------------


def replay_response(status: ReplayStatus = ReplayStatus.RUNNING) -> ReplayResponse:
    return ReplayResponse(
        id=REPLAY,
        session_id=SESSION,
        race_id=uuid4(),
        status=status,
        is_completed=status is ReplayStatus.COMPLETED,
        status_reason=None,
        playback_speed=2.0,
        current_race_time_ms=1_000,
        current_sequence=3,
        emitted_event_count=4,
        total_events=10,
        current_lap=1,
        total_laps=5,
        created_at=datetime(2024, 1, 1, tzinfo=UTC),
        started_at=None,
        paused_at=None,
        ended_at=None,
    )


def test_replay_status_messages_add_completion_once_completed() -> None:
    running = replay_status_messages(replay_response())
    done = replay_status_messages(replay_response(ReplayStatus.COMPLETED))

    assert types(running) == ["REPLAY_STATUS"]
    assert types(done) == ["REPLAY_STATUS", "REPLAY_COMPLETED"]
    assert done[1].payload["replay"]["status"] == "COMPLETED"
    assert done[0].run_id is None and done[0].sequence is None
    assert done[0].race_time_ms == 1_000 and done[0].lap_number == 1


def test_clock_message_is_droppable_and_carries_progress() -> None:
    state = ReplayState(
        replay_id=REPLAY,
        session_id=SESSION,
        status=ReplayStatus.RUNNING,
        playback_speed=Decimal(1),
        current_race_time_ms=5_000,
        current_sequence=1,
        current_lap=1,
        total_events=10,
        total_laps=5,
        started_at=None,
        paused_at=None,
        ended_at=None,
        status_reason=None,
    )

    message = replay_clock_message(state)

    assert message.droppable and message.type in DROPPABLE_TYPES
    assert message.payload["playback_speed"] == 1.0 and message.race_time_ms == 5_000
    assert message.payload["status"] == "RUNNING"


def test_snapshot_without_state_explains_why() -> None:
    error = {"code": "REPLAY_NOT_STARTED", "message": "m", "details": None}

    message = snapshot_message(replay_response(), None, error)

    assert message.type is WsMessageType.SNAPSHOT
    assert message.payload["state"] is None and message.payload["state_error"] == error
    assert message.run_id is None and message.sequence is None
    assert message.race_time_ms == 1_000 and message.lap_number == 1


def test_snapshot_with_state_is_keyed_by_run_and_sequence() -> None:
    state = RaceStateResponse.build(
        RaceState(replay_id=REPLAY, run_id=RUN, session_id=SESSION, last_sequence=9),
        "live",  # type: ignore[arg-type]
        ReplayStatus.RUNNING,
    )

    message = snapshot_message(replay_response(), state, None)

    assert message.run_id == RUN and message.sequence == 9
    assert message.payload["state"]["last_sequence"] == 9


def test_error_and_pong_messages() -> None:
    body = {"code": "REPLAY_NOT_FOUND", "message": "m", "details": None}
    assert error_message(REPLAY, body).payload == body
    assert pong_message(REPLAY).type is WsMessageType.PONG


def test_state_derived_types_are_exactly_the_state_messages() -> None:
    assert WsMessageType.SNAPSHOT not in STATE_DERIVED_TYPES
    assert WsMessageType.DETECTED_EVENT not in STATE_DERIVED_TYPES
    assert WsMessageType.DRIVER_UPDATE in STATE_DERIVED_TYPES
