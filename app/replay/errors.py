"""Replay error hierarchy with client-safe messages."""

from __future__ import annotations

from uuid import UUID

from app.domain.enums import ReplayStatus


class ReplayError(Exception):
    """Base replay failure with a safe ``message`` for API clients."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class ReplayNotFoundError(ReplayError):
    def __init__(self, replay_id: UUID) -> None:
        super().__init__(f"Replay not found: {replay_id}")


class InvalidReplayTransitionError(ReplayError):
    """The command is not allowed from the replay's current status."""

    def __init__(self, command: str, current_status: ReplayStatus) -> None:
        self.command = command
        self.current_status = current_status
        super().__init__(f"Cannot {command} a replay that is {current_status.value}")


class InvalidPlaybackSpeedError(ReplayError):
    """The requested playback speed is not supported."""


class ReplayTimelineUnavailableError(ReplayError):
    """The session has no usable historical timeline to replay."""


class ReplayPersistenceError(ReplayError):
    """Replay state could not be written to the database."""
