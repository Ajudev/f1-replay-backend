"""Replay lifecycle transitions and playback speed validation."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.domain.enums import ReplayStatus as S
from app.replay.errors import InvalidPlaybackSpeedError, InvalidReplayTransitionError
from app.replay.state import ReplayCommand as C
from app.replay.state import transition, validate_playback_speed


@pytest.mark.parametrize(
    ("current", "command", "expected"),
    [
        (S.CREATED, C.START, S.RUNNING),
        (S.RUNNING, C.PAUSE, S.PAUSED),
        (S.PAUSED, C.RESUME, S.RUNNING),
        (S.RUNNING, C.STOP, S.STOPPED),
        (S.PAUSED, C.STOP, S.STOPPED),
        (S.RUNNING, C.COMPLETE, S.COMPLETED),
        (S.RUNNING, C.FAIL, S.FAILED),
        (S.PAUSED, C.FAIL, S.FAILED),
        (S.RUNNING, C.RESTART, S.RUNNING),
        (S.PAUSED, C.RESTART, S.RUNNING),
        (S.STOPPED, C.RESTART, S.RUNNING),
        (S.COMPLETED, C.RESTART, S.RUNNING),
        (S.FAILED, C.RESTART, S.RUNNING),
    ],
)
def test_valid_transitions(current: S, command: C, expected: S) -> None:
    assert transition(current, command) is expected


@pytest.mark.parametrize(
    ("current", "command"),
    [
        (S.RUNNING, C.START),
        (S.PAUSED, C.START),
        (S.COMPLETED, C.START),
        (S.STOPPED, C.START),
        (S.CREATED, C.PAUSE),
        (S.PAUSED, C.PAUSE),
        (S.COMPLETED, C.PAUSE),
        (S.RUNNING, C.RESUME),
        (S.COMPLETED, C.RESUME),
        (S.STOPPED, C.RESUME),
        (S.CREATED, C.STOP),
        (S.STOPPED, C.STOP),
        (S.COMPLETED, C.STOP),
        (S.CREATED, C.RESTART),
        (S.PAUSED, C.COMPLETE),
        (S.STOPPED, C.FAIL),
    ],
)
def test_invalid_transitions_raise(current: S, command: C) -> None:
    with pytest.raises(InvalidReplayTransitionError) as exc_info:
        transition(current, command)
    assert exc_info.value.current_status is current
    assert current.value in exc_info.value.message


@pytest.mark.parametrize("value", [1, 2, 5, 10, 20, "5", 10.0, Decimal("20.00")])
def test_supported_speeds(value: object) -> None:
    speed = validate_playback_speed(value)  # type: ignore[arg-type]
    assert speed == Decimal(str(value))
    assert speed.as_tuple().exponent == -2


@pytest.mark.parametrize("value", [0, -1, 3, 0.5, 1000, "nan", "inf", "fast"])
def test_unsupported_speeds_rejected(value: object) -> None:
    with pytest.raises(InvalidPlaybackSpeedError):
        validate_playback_speed(value)  # type: ignore[arg-type]
