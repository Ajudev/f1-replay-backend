"""Deterministic time and sink doubles for replay tests (no real waiting)."""

from __future__ import annotations

import asyncio
from uuid import UUID

from app.replay.events import ReplayEvent
from app.replay.service import ReplayService


class ManualTimer:
    """Fake monotonic time; waits complete only when ``advance`` passes their deadline."""

    def __init__(self) -> None:
        self.now = 0.0
        self._waiters: list[tuple[float, asyncio.Future[None]]] = []

    def monotonic(self) -> float:
        return self.now

    async def wait(self, wake: asyncio.Event, timeout: float | None) -> None:
        if timeout is not None and timeout <= 0:
            return
        waits: set[asyncio.Future[object]] = {asyncio.ensure_future(wake.wait())}
        timed: asyncio.Future[None] | None = None
        if timeout is not None:
            timed = asyncio.get_running_loop().create_future()
            self._waiters.append((self.now + timeout, timed))
            waits.add(timed)  # type: ignore[arg-type]
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for fut in waits:
                fut.cancel()
            self._waiters = [(d, f) for d, f in self._waiters if f is not timed]

    async def advance(self, seconds: float) -> None:
        """Move time forward, release due waits and let the replay loops run."""
        self.now += seconds
        for deadline, fut in list(self._waiters):
            if deadline <= self.now + 1e-9 and not fut.done():
                fut.set_result(None)
        await settle()


async def settle(rounds: int = 50) -> None:
    """Yield to the event loop until in-flight replay work has run."""
    for _ in range(rounds):
        await asyncio.sleep(0)


async def wait_until_idle(service: ReplayService, timeout: float = 2.0) -> None:
    """Wait until finished runners have persisted their terminal state.

    Database work runs on aiosqlite's thread, so yielding alone is not enough.
    """
    async with asyncio.timeout(timeout):
        while service.active_replay_ids():
            await asyncio.sleep(0.001)


class CollectingSink:
    def __init__(self) -> None:
        self.events: list[ReplayEvent] = []

    async def publish(self, event: ReplayEvent) -> None:
        self.events.append(event)

    def sequences(self, replay_id: UUID | None = None) -> list[int]:
        return [e.sequence for e in self.events if replay_id is None or e.replay_id == replay_id]


class FailingSink(CollectingSink):
    """Raises when publishing ``fail_at`` (that event is not recorded)."""

    def __init__(self, fail_at: int) -> None:
        super().__init__()
        self.fail_at = fail_at

    async def publish(self, event: ReplayEvent) -> None:
        if event.sequence == self.fail_at:
            raise ConnectionError("sink unavailable")
        await super().publish(event)


class GatedSink(CollectingSink):
    """Blocks inside ``publish`` for ``gate_at`` until ``release`` is called."""

    def __init__(self, gate_at: int) -> None:
        super().__init__()
        self.gate_at = gate_at
        self.entered = asyncio.Event()
        self._gate = asyncio.Event()

    def release(self) -> None:
        self._gate.set()

    async def publish(self, event: ReplayEvent) -> None:
        if event.sequence == self.gate_at:
            self.entered.set()
            await self._gate.wait()
        await super().publish(event)
