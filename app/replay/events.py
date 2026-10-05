"""Replay emission contract: the emitted event and the sink it is published to."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from app.domain.enums import EventType

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    """A historical timeline event released by a replay.

    Carries the timeline's own ``sequence`` so consumers can detect gaps or
    duplicates. ``run_id`` identifies one start or restart of the replay: a restart
    re-emits the timeline from sequence 0, so consumers need ``(run_id, sequence)``
    to tell runs apart. ``payload`` is shared with the replay's loaded timeline and must
    be treated as read-only.
    """

    replay_id: UUID
    run_id: UUID
    session_id: UUID
    sequence: int
    event_type: EventType
    race_time_ms: int
    lap_number: int | None
    driver_id: UUID | None
    driver_abbreviation: str | None
    payload: dict[str, Any]


class ReplayEventSink(Protocol):
    """Destination for emitted replay events.

    ``publish`` is awaited once per event, in timeline order, from the replay's
    own task. An exception fails the replay; the event is then not counted as
    emitted. The engine does not know what backs the sink.
    """

    async def publish(self, event: ReplayEvent) -> None: ...


class LoggingEventSink:
    """Default sink: records emitted events at DEBUG level only."""

    async def publish(self, event: ReplayEvent) -> None:
        logger.debug(
            "Replay event replay_id=%s seq=%d type=%s race_time_ms=%d lap=%s driver=%s",
            event.replay_id,
            event.sequence,
            event.event_type.value,
            event.race_time_ms,
            event.lap_number,
            event.driver_abbreviation,
        )
