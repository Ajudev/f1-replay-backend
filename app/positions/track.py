"""In-memory car positions and telemetry of one session, queried by race time.

Pure domain code: built from stored track samples, no I/O. ``frame_at(t)`` gives every
driver's car at race time ``t``:

- position: linear interpolation between the neighbouring stored samples;
- telemetry: the latest stored sample at or before ``t`` (stepped, never interpolated,
  so every value is one that was actually recorded).

Either is ``None`` when the nearest data is more than ``max_gap_ms`` away (missing data,
car stopped transmitting, before the first or after the last sample). Same input and
``t`` always give the same frame.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass, field
from uuid import UUID

from app.domain.enums import DrsState


@dataclass(frozen=True, slots=True)
class TrackRow:
    """One stored sample (a row of ``driver_track_samples``)."""

    driver_id: UUID
    race_time_ms: int
    x: int | None
    y: int | None
    off_track: bool | None
    speed_kph: int | None
    rpm: int | None
    gear: int | None
    throttle_pct: int | None
    brake: bool | None
    drs_code: int | None


@dataclass(frozen=True, slots=True)
class CarTelemetry:
    speed_kph: int | None
    rpm: int | None
    gear: int | None
    throttle_pct: int | None
    brake: bool | None
    drs: DrsState | None

    def to_payload(self) -> dict[str, object]:
        return {
            "speed_kph": self.speed_kph,
            "rpm": self.rpm,
            "gear": self.gear,
            "throttle_pct": self.throttle_pct,
            "brake": self.brake,
            "drs": self.drs.value if self.drs is not None else None,
        }


@dataclass(frozen=True, slots=True)
class CarFrame:
    driver_id: UUID
    x: int | None
    y: int | None
    off_track: bool | None
    telemetry: CarTelemetry | None

    def to_payload(self) -> dict[str, object]:
        return {
            "driver_id": str(self.driver_id),
            "x": self.x,
            "y": self.y,
            "off_track": self.off_track,
            "telemetry": self.telemetry.to_payload() if self.telemetry else None,
        }


@dataclass(slots=True)
class _DriverSeries:
    pos_times: list[int] = field(default_factory=list)
    xs: list[int] = field(default_factory=list)
    ys: list[int] = field(default_factory=list)
    off_track: list[bool | None] = field(default_factory=list)
    tel_times: list[int] = field(default_factory=list)
    telemetry: list[CarTelemetry] = field(default_factory=list)


class PositionTrack:
    def __init__(self, rows: Iterable[TrackRow], *, max_gap_ms: int, order: list[UUID]) -> None:
        """``rows`` in ``(driver_id, race_time_ms)`` order; ``order`` is the frame order."""
        self._max_gap_ms = max_gap_ms
        self._series: dict[UUID, _DriverSeries] = {}
        for row in rows:
            series = self._series.get(row.driver_id)
            if series is None:
                series = self._series[row.driver_id] = _DriverSeries()
            if row.x is not None and row.y is not None:
                series.pos_times.append(row.race_time_ms)
                series.xs.append(row.x)
                series.ys.append(row.y)
                series.off_track.append(row.off_track)
            if row.speed_kph is not None or row.gear is not None or row.drs_code is not None:
                series.tel_times.append(row.race_time_ms)
                series.telemetry.append(
                    CarTelemetry(
                        speed_kph=row.speed_kph,
                        rpm=row.rpm,
                        gear=row.gear,
                        throttle_pct=row.throttle_pct,
                        brake=row.brake,
                        drs=DrsState.from_code(row.drs_code),
                    )
                )
        self._order = [driver_id for driver_id in order if driver_id in self._series]
        self.positions_available = any(s.pos_times for s in self._series.values())
        self.telemetry_available = any(s.tel_times for s in self._series.values())

    @property
    def empty(self) -> bool:
        return not (self.positions_available or self.telemetry_available)

    def frame_at(self, race_time_ms: int) -> list[CarFrame]:
        return [self._car(driver_id, race_time_ms) for driver_id in self._order]

    def _car(self, driver_id: UUID, t: int) -> CarFrame:
        series = self._series[driver_id]
        x = y = None
        off_track: bool | None = None
        i = bisect_right(series.pos_times, t)  # pos_times[i - 1] <= t < pos_times[i]
        if i > 0 and series.pos_times[i - 1] == t:
            x, y, off_track = series.xs[i - 1], series.ys[i - 1], series.off_track[i - 1]
        elif 0 < i < len(series.pos_times):
            t0, t1 = series.pos_times[i - 1], series.pos_times[i]
            if t1 - t0 <= self._max_gap_ms:
                ratio = (t - t0) / (t1 - t0)
                x = round(series.xs[i - 1] + (series.xs[i] - series.xs[i - 1]) * ratio)
                y = round(series.ys[i - 1] + (series.ys[i] - series.ys[i - 1]) * ratio)
                off_track = series.off_track[i - 1]

        telemetry = None
        j = bisect_right(series.tel_times, t)
        if j > 0 and t - series.tel_times[j - 1] <= self._max_gap_ms:
            telemetry = series.telemetry[j - 1]
        return CarFrame(driver_id, x, y, off_track, telemetry)
