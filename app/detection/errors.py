"""Detection errors.

Processing errors are retryable by design (the consumer leaves the message pending), so
none of them derives from ``NonRetryableEventError``; only a malformed state event is
dead-lettered (``MalformedEventError``).
"""

from __future__ import annotations


class DetectionError(Exception):
    """Base class."""


class DetectionConflictError(DetectionError):
    """Another worker changed the replay's detection context during this event."""


class ReplayGoneError(DetectionError):
    """The replay was deleted, so its detections cannot be stored. Not retryable."""
