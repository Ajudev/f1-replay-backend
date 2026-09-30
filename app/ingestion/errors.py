"""Ingestion error hierarchy with client-safe messages."""


class IngestionError(Exception):
    """Base ingestion failure with a safe ``message`` for API clients."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class EventNotFoundError(IngestionError):
    """The requested Grand Prix event could not be found."""


class SessionNotFoundError(IngestionError):
    """The requested session within an event could not be found."""


class SessionLoadError(IngestionError):
    """FastF1 session load failed for a reason other than not-found."""


class NormalizationError(IngestionError):
    """Extracted data is structurally unusable for persistence."""


class PersistenceError(IngestionError):
    """Database persistence failed during import."""
