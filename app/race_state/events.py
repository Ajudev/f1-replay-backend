"""State events published on ``race.state.events`` (reuse the stream envelope)."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid5

from app.race_state.models import RaceState, StateDelta, StateEventType
from app.streaming.envelope import STREAM_EVENT_SCHEMA_VERSION, StreamEvent

#: Fields left out of published state: internal bookkeeping, not part of the contract.
PUBLIC_STATE_EXCLUDE = {"lap_crossings", "run_published_at"}


def derive_state_event_id(source: StreamEvent) -> UUID:
    """Deterministic and distinct from the raw event id: ``uuid5(run_id, "state:<seq>")``."""
    return uuid5(source.run_id, f"state:{source.sequence}")


def build_state_event(
    *,
    event_type: StateEventType,
    state: RaceState,
    delta: StateDelta,
    source: StreamEvent,
    rebuilt: bool,
    published_at: datetime,
) -> StreamEvent:
    """Envelope for one state transition.

    ``STATE_UPDATED`` carries only the delta. The snapshot types
    (``STATE_INITIALIZED``, ``STATE_REBUILT``, ``STATE_COMPLETED``) additionally carry
    the full public ``state``. ``STATE_COMPLETED`` replaces the final delta: it still lists
    ``kinds`` and ``changes`` so consumers need no special case.
    """
    payload: dict[str, object] = {
        "source_event_id": str(source.event_id),
        "source_event_type": source.event_type,
        "last_sequence": state.last_sequence,
        "rebuilt": rebuilt,
        "kinds": [kind.value for kind in delta.kinds],
        "changes": {
            "race": delta.race,
            "drivers": [d.model_dump(mode="json") for d in delta.drivers],
            "position_changes": [str(i) for i in delta.position_changes],
            "pit_changes": [str(i) for i in delta.pit_changes],
        },
    }
    if event_type is not StateEventType.STATE_UPDATED:
        payload["state"] = state.model_dump(mode="json", exclude=PUBLIC_STATE_EXCLUDE)
    return StreamEvent(
        event_id=derive_state_event_id(source),
        schema_version=STREAM_EVENT_SCHEMA_VERSION,
        event_type=event_type.value,
        replay_id=source.replay_id,
        run_id=source.run_id,
        session_id=source.session_id,
        sequence=source.sequence,
        race_time_ms=state.current_race_time_ms,
        lap_number=state.current_lap,
        driver_id=source.driver_id,
        driver_abbreviation=source.driver_abbreviation,
        published_at=published_at,
        payload=payload,
    )
