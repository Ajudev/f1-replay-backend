"""Race state errors.

Processing errors (``RaceStateProcessingError``) are deliberately *not* subclasses of
``NonRetryableEventError``: the consumer leaves the message pending and retries it.
Dead-lettering a raw event permanently stalls that run's state (every later event
becomes a sequence gap), so only the consumer's own delivery limit may do it.

Query errors carry a client-safe ``message`` and are mapped to HTTP statuses in
``app/api/exceptions.py``.
"""

from __future__ import annotations

from uuid import UUID


class RaceStateError(Exception):
    """Base class with a client-safe ``message``."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


# -- processing (stream side; always retried) ---------------------------------------------


class RaceStateProcessingError(RaceStateError):
    """A raw event could not be applied now; the message stays pending."""


class StateConflictError(RaceStateProcessingError):
    """Another worker changed the replay's state while this event was being processed."""


class RaceSeedError(RaceStateProcessingError):
    """The session data needed to initialize a run's state is missing."""


class RaceStateRebuildError(RaceStateProcessingError):
    """The persisted timeline cannot reproduce the events before the received one."""


class SnapshotPersistError(RaceStateProcessingError):
    """A snapshot that must be durable (the final one) could not be written."""


# -- query (API side) ----------------------------------------------------------------------


class ReplayNotStartedError(RaceStateError):
    def __init__(self, replay_id: UUID) -> None:
        super().__init__(f"Replay {replay_id} has not been started; there is no race state yet")


class RaceStateUnavailableError(RaceStateError):
    def __init__(self, replay_id: UUID) -> None:
        super().__init__(
            f"No race state is available for replay {replay_id}: nothing has been processed "
            "yet or the state expired. Make sure the race state worker is running."
        )


class RaceStateStoreUnavailableError(RaceStateError):
    def __init__(self) -> None:
        super().__init__("Race state store unavailable")


class DriverNotInStateError(RaceStateError):
    def __init__(self, replay_id: UUID, driver: str) -> None:
        super().__init__(f"Driver {driver!r} is not part of the race state of replay {replay_id}")
