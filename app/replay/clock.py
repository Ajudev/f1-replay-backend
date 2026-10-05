"""Virtual race clock mapping real elapsed time to race elapsed time.

The clock is anchored at a ``(race position, real time)`` pair. While running,
``position = anchor_race_ms + (now - anchor_real) * speed * 1000``. Pausing,
resuming and speed changes re-anchor at the current position, so the race
time never jumps and no drift accumulates from repeated sleeps.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections.abc import Callable
from typing import Protocol

TimeSource = Callable[[], float]
"""Monotonic seconds (``time.monotonic`` in production, a fake in tests)."""

# Tolerates float error so an event due "exactly now" is not seen as 1 ms early.
_EPSILON_MS = 1e-6


class VirtualRaceClock:
    def __init__(
        self,
        time_source: TimeSource,
        *,
        speed: float = 1.0,
        position_ms: float = 0.0,
    ) -> None:
        self._time = time_source
        self._speed = self._checked_speed(speed)
        self._anchor_ms = self._checked_position(position_ms)
        self._anchor_real: float | None = None  # None while paused

    @property
    def running(self) -> bool:
        return self._anchor_real is not None

    @property
    def speed(self) -> float:
        return self._speed

    def position_ms(self) -> float:
        if self._anchor_real is None:
            return self._anchor_ms
        return self._anchor_ms + (self._time() - self._anchor_real) * self._speed * 1000.0

    def now_ms(self) -> int:
        """Current race time in whole milliseconds."""
        return math.floor(self.position_ms() + _EPSILON_MS)

    def start(self) -> None:
        """Start (or resume) advancing from the current position."""
        if self.running:
            raise RuntimeError("Clock is already running")
        self._anchor_real = self._time()

    resume = start

    def pause(self) -> None:
        """Freeze race time at the current position."""
        if not self.running:
            raise RuntimeError("Clock is not running")
        self._anchor_ms = self.position_ms()
        self._anchor_real = None

    def set_speed(self, speed: float) -> None:
        """Change speed without moving the current race position."""
        speed = self._checked_speed(speed)
        if self.running:
            now = self._time()
            self._anchor_ms += (now - self._anchor_real) * self._speed * 1000.0  # type: ignore[operator]
            self._anchor_real = now
        self._speed = speed

    def reset(self, position_ms: float = 0.0) -> None:
        """Move to ``position_ms`` and pause (used by restart; basis for future seek)."""
        self._anchor_ms = self._checked_position(position_ms)
        self._anchor_real = None

    def real_seconds_until(self, race_time_ms: float) -> float | None:
        """Real seconds until ``race_time_ms`` is reached; None while paused."""
        if not self.running:
            return None
        return max(0.0, (race_time_ms - self.position_ms()) / (self._speed * 1000.0))

    @staticmethod
    def _checked_speed(speed: float) -> float:
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError(f"Clock speed must be a positive finite number, got {speed!r}")
        return float(speed)

    @staticmethod
    def _checked_position(position_ms: float) -> float:
        if not math.isfinite(position_ms) or position_ms < 0:
            raise ValueError(f"Clock position must be >= 0, got {position_ms!r}")
        return float(position_ms)


class ReplayTimer(Protocol):
    """Real-time source plus an interruptible wait; injectable for tests."""

    def monotonic(self) -> float: ...

    async def wait(self, wake: asyncio.Event, timeout: float | None) -> None:
        """Return when ``wake`` is set or ``timeout`` real seconds have passed."""
        ...


class AsyncioReplayTimer:
    def monotonic(self) -> float:
        return time.monotonic()

    async def wait(self, wake: asyncio.Event, timeout: float | None) -> None:
        if timeout is None:
            await wake.wait()
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(wake.wait(), timeout)
