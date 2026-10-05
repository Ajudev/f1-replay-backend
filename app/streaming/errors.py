"""Errors raised by the streaming transport."""

from __future__ import annotations

from uuid import UUID


class StreamError(Exception):
    """Base class for streaming transport errors."""


class StreamSerializationError(StreamError):
    """An event could not be encoded (for example an unsupported payload type)."""


class NonRetryableEventError(StreamError):
    """A received message can never be processed; retrying cannot help."""


class MalformedEventError(NonRetryableEventError):
    """A stream entry is not a valid event envelope."""


class UnsupportedSchemaVersionError(NonRetryableEventError):
    """An envelope declares a schema version the consumer does not support."""

    def __init__(self, version: object, supported: frozenset[int]) -> None:
        self.version = version
        self.supported = supported
        super().__init__(f"Unsupported schema version {version!r}; supported: {sorted(supported)}")


class StreamPublishError(StreamError):
    """An event could not be published; the replay must not treat it as emitted."""

    def __init__(self, message: str, *, stream: str, replay_id: UUID, sequence: int) -> None:
        self.stream = stream
        self.replay_id = replay_id
        self.sequence = sequence
        super().__init__(f"{message} (stream={stream} replay_id={replay_id} sequence={sequence})")
