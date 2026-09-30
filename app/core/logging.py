"""Application logging configuration."""

import logging
import sys


def configure_logging(level: str) -> None:
    """Configure standard library logging for the application.

    Args:
        level: Log level name (e.g. ``INFO``, ``DEBUG``).
    """
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level.upper())
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)
