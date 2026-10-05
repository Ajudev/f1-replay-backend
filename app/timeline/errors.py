"""Timeline error hierarchy with client-safe messages."""

from __future__ import annotations


class TimelineError(Exception):
    """Base timeline failure with a safe ``message`` for API clients."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class UnsupportedSessionTypeError(TimelineError):
    """Timelines are only built for race and sprint sessions."""


class TimelineBuildError(TimelineError):
    """Required timing data is missing, so a timeline cannot be built."""


class TimelineValidationError(TimelineError):
    """The built timeline violates a structural invariant."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        shown = "; ".join(self.problems[:10])
        extra = f" (+{len(self.problems) - 10} more)" if len(self.problems) > 10 else ""
        super().__init__(f"Timeline failed validation: {shown}{extra}")


class TimelineNotGeneratedError(TimelineError):
    """No timeline has been generated for the session yet."""


class TimelineConflictError(TimelineError):
    """A concurrent generation for the same session won the race."""
