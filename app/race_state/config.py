"""Race state tuning, built from ``Settings`` (pure value object)."""

from __future__ import annotations

from dataclasses import dataclass

from app.core.config import Settings


@dataclass(frozen=True, slots=True)
class RaceStateConfig:
    #: Laps kept in ``DriverState.recent_laps`` and in the gap crossing window.
    lap_history: int = 10
    #: Persist a PostgreSQL snapshot each time the leader's completed laps cross a
    #: multiple of this value. ``0`` disables periodic snapshots.
    snapshot_every_laps: int = 10
    #: Redis TTL of the hot state document, refreshed on every write.
    ttl_seconds: int = 604_800
    key_prefix: str = "race"
    #: Total time an event that is ahead of the next expected sequence waits in process
    #: for its predecessors before the state is rebuilt from the timeline.
    gap_wait_ms: int = 1_500
    #: Pause between re-reads of the state while waiting for a gap to close.
    gap_poll_ms: int = 100
    #: Attempts per event when other workers keep changing the state concurrently.
    conflict_attempts: int = 3

    @classmethod
    def from_settings(cls, settings: Settings) -> RaceStateConfig:
        return cls(
            lap_history=settings.race_state_lap_history,
            snapshot_every_laps=settings.race_state_snapshot_every_laps,
            ttl_seconds=settings.race_state_ttl_seconds,
            key_prefix=settings.race_state_key_prefix,
            gap_wait_ms=settings.race_state_gap_wait_ms,
        )
