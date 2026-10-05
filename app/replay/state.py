"""Replay lifecycle state machine, playback speeds and progress state.

Pure domain logic: no FastAPI, SQLAlchemy, Redis or FastF1.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from uuid import UUID

from app.domain.enums import ReplayStatus
from app.replay.errors import InvalidPlaybackSpeedError, InvalidReplayTransitionError

SUPPORTED_PLAYBACK_SPEEDS: tuple[Decimal, ...] = tuple(
    Decimal(value) for value in ("1", "2", "5", "10", "20")
)
DEFAULT_PLAYBACK_SPEED = Decimal("1")


class ReplayCommand(StrEnum):
    """Lifecycle operations. COMPLETE and FAIL are issued by the engine only."""

    START = "start"
    PAUSE = "pause"
    RESUME = "resume"
    STOP = "stop"
    RESTART = "restart"
    COMPLETE = "complete"
    FAIL = "fail"


_S = ReplayStatus
TRANSITIONS: dict[ReplayCommand, tuple[frozenset[ReplayStatus], ReplayStatus]] = {
    ReplayCommand.START: (frozenset({_S.CREATED}), _S.RUNNING),
    ReplayCommand.PAUSE: (frozenset({_S.RUNNING}), _S.PAUSED),
    ReplayCommand.RESUME: (frozenset({_S.PAUSED}), _S.RUNNING),
    ReplayCommand.STOP: (frozenset({_S.RUNNING, _S.PAUSED}), _S.STOPPED),
    ReplayCommand.RESTART: (
        frozenset({_S.RUNNING, _S.PAUSED, _S.STOPPED, _S.COMPLETED, _S.FAILED}),
        _S.RUNNING,
    ),
    ReplayCommand.COMPLETE: (frozenset({_S.RUNNING}), _S.COMPLETED),
    # PAUSED is reachable: a pause can land while a publish is in flight and that
    # publish then raises.
    ReplayCommand.FAIL: (frozenset({_S.RUNNING, _S.PAUSED}), _S.FAILED),
}

TERMINAL_STATUSES = frozenset({_S.STOPPED, _S.COMPLETED, _S.FAILED})
ACTIVE_STATUSES = frozenset({_S.RUNNING, _S.PAUSED})


def transition(current: ReplayStatus, command: ReplayCommand) -> ReplayStatus:
    """Return the status after ``command`` or raise if it is not allowed."""
    allowed_from, target = TRANSITIONS[command]
    if current not in allowed_from:
        raise InvalidReplayTransitionError(command.value, current)
    return target


def validate_playback_speed(value: Decimal | float | int | str) -> Decimal:
    """Normalize a requested speed; only ``SUPPORTED_PLAYBACK_SPEEDS`` are accepted."""
    try:
        speed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidPlaybackSpeedError(f"Invalid playback speed: {value!r}") from exc
    if not speed.is_finite() or speed not in SUPPORTED_PLAYBACK_SPEEDS:
        supported = ", ".join(f"{s}x" for s in SUPPORTED_PLAYBACK_SPEEDS)
        raise InvalidPlaybackSpeedError(
            f"Unsupported playback speed {value!r}; supported speeds: {supported}"
        )
    return speed.quantize(Decimal("0.01"))


@dataclass(frozen=True, slots=True)
class ReplayState:
    """Mutable-per-lifecycle replay fields (what gets persisted on transitions).

    ``current_sequence`` is the sequence of the last emitted timeline event
    (null before the first emission). ``current_lap`` is the race lap the
    leader is on: 1 once the race has started, ``n + 1`` after the leader
    completes lap ``n``, capped at ``total_laps``.
    """

    replay_id: UUID
    session_id: UUID
    status: ReplayStatus
    playback_speed: Decimal
    current_race_time_ms: int
    current_sequence: int | None
    current_lap: int | None
    total_events: int | None
    total_laps: int | None
    started_at: datetime | None
    paused_at: datetime | None
    ended_at: datetime | None
    status_reason: str | None

    @property
    def emitted_event_count(self) -> int:
        # Timeline sequences are contiguous from 0, so position == last sequence + 1.
        return 0 if self.current_sequence is None else self.current_sequence + 1
