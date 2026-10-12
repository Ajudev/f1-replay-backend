"""FastF1 session loader adapter.

This is the only module that imports fastf1.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Protocol

import fastf1

from app.domain.enums import SessionType
from app.ingestion.cleaning import (
    clean_bool,
    clean_int,
    clean_optional_str,
    clean_team_color,
    is_missing,
    timedelta_to_ms,
    to_aware_datetime,
    to_date,
)
from app.ingestion.errors import EventNotFoundError, SessionLoadError, SessionNotFoundError
from app.ingestion.records import (
    CarSeries,
    ExtractedCircuit,
    ExtractedCorner,
    ExtractedDriver,
    ExtractedDriverTrack,
    ExtractedLap,
    ExtractedSession,
    ExtractedTrackData,
    ExtractedTrackStatus,
    PositionSeries,
)

logger = logging.getLogger(__name__)

_SESSION_TYPE_TO_FASTF1: dict[SessionType, str] = {
    SessionType.PRACTICE_1: "FP1",
    SessionType.PRACTICE_2: "FP2",
    SessionType.PRACTICE_3: "FP3",
    SessionType.QUALIFYING: "Q",
    SessionType.SPRINT_QUALIFYING: "SQ",
    SessionType.SPRINT: "S",
    SessionType.RACE: "R",
}


def fastf1_identifiers_for(session_type: SessionType) -> tuple[str, ...]:
    """Return FastF1 session identifiers to attempt, in order.

    Sprint qualifying tries ``SQ`` then ``SS`` (Sprint Shootout) because FastF1
    used both labels across seasons.
    """
    if session_type == SessionType.SPRINT_QUALIFYING:
        return ("SQ", "SS")
    return (_SESSION_TYPE_TO_FASTF1[session_type],)


def is_not_found_error(error: BaseException) -> bool:
    """True when an identifier/event was not found (safe to try the next id)."""
    return isinstance(error, (SessionNotFoundError, EventNotFoundError))


class SessionLoader(Protocol):
    """Protocol for loading a session into plain extracted records."""

    def load(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> ExtractedSession:
        """Load and extract a session without returning FastF1 objects."""
        ...


class TrackDataLoader(Protocol):
    """Protocol for loading a session's car positions, telemetry and circuit info."""

    def load_track_data(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> ExtractedTrackData:
        """Load and extract track data without returning FastF1 objects."""
        ...


def _row_get(row: Any, *keys: str) -> Any:
    """Read the first present key from a pandas Series-like or mapping."""
    for key in keys:
        try:
            if hasattr(row, "__contains__") and key in row:
                return row[key]
        except (TypeError, KeyError, ValueError):
            pass
        try:
            return row[key]
        except (KeyError, TypeError, IndexError, ValueError):
            pass
        if hasattr(row, key):
            return getattr(row, key)
    return None


class FastF1SessionLoader:
    """Loads historical sessions via FastF1 into plain Python records."""

    def __init__(self, cache_dir: str = ".fastf1-cache") -> None:
        self._cache_dir = cache_dir

    def load(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> ExtractedSession:
        session = self._open_session(
            season, event, session_type, telemetry=False, weather=False, messages=False
        )
        return self._extract(session, season=season, session_type=session_type)

    def load_track_data(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> ExtractedTrackData:
        """Car positions, car telemetry, team colours and circuit info of a session.

        Loads telemetry, which is far larger and slower than the timing data.
        """
        session = self._open_session(
            season, event, session_type, laps=True, telemetry=True, weather=False, messages=False
        )
        drivers = self._extract_driver_tracks(session)
        return ExtractedTrackData(drivers=drivers, circuit=self._extract_circuit(session))

    def _open_session(
        self,
        season: int,
        event: int | str,
        session_type: SessionType,
        **load_options: bool,
    ) -> Any:
        """Resolve and load a FastF1 session, trying fallback identifiers if not found."""
        cache_path = Path(self._cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(str(cache_path))

        identifiers = fastf1_identifiers_for(session_type)
        last_not_found: EventNotFoundError | SessionNotFoundError | None = None

        for index, identifier in enumerate(identifiers):
            try:
                session = fastf1.get_session(season, event, identifier)
            except Exception as exc:
                mapped = self._map_get_error(exc, season, event, session_type)
                if index < len(identifiers) - 1 and is_not_found_error(mapped):
                    last_not_found = mapped  # type: ignore[assignment]
                    logger.info(
                        "Session identifier %s not found; trying fallback",
                        identifier,
                    )
                    continue
                raise mapped from None

            try:
                session.load(**load_options)
            except Exception as exc:
                mapped = self._map_load_error(exc, season, event, session_type)
                if index < len(identifiers) - 1 and is_not_found_error(mapped):
                    last_not_found = mapped  # type: ignore[assignment]
                    logger.info(
                        "Session identifier %s failed to load as not-found; trying fallback",
                        identifier,
                    )
                    continue
                raise mapped from None

            return session

        if last_not_found is not None:
            raise last_not_found
        raise SessionNotFoundError(
            f"Session {session_type.value} not found for {season} event {event}"
        )

    def _extract(
        self,
        session: Any,
        *,
        season: int,
        session_type: SessionType,
    ) -> ExtractedSession:
        event = getattr(session, "event", None)
        round_number = clean_int(_row_get(event, "RoundNumber"))
        event_name = clean_optional_str(_row_get(event, "EventName"))
        official_name = clean_optional_str(_row_get(event, "OfficialEventName"))
        country = clean_optional_str(_row_get(event, "Country"))
        location = clean_optional_str(_row_get(event, "Location"))
        event_date = to_date(_row_get(event, "EventDate"))

        session_name = clean_optional_str(getattr(session, "name", None))
        session_start = to_aware_datetime(getattr(session, "date", None))

        drivers = self._extract_drivers(getattr(session, "results", None))
        laps = self._extract_laps(getattr(session, "laps", None))
        track_statuses = self._extract_track_status(getattr(session, "track_status", None))

        return ExtractedSession(
            season=season,
            round=round_number,
            event_name=event_name,
            official_event_name=official_name,
            country=country,
            location=location,
            event_date=event_date,
            session_type=session_type,
            session_name=session_name,
            session_start=session_start,
            drivers=drivers,
            laps=laps,
            track_statuses=track_statuses,
        )

    def _extract_drivers(self, results: Any) -> list[ExtractedDriver]:
        if results is None:
            return []
        try:
            empty = len(results) == 0
        except TypeError:
            return []
        if empty:
            return []

        drivers: list[ExtractedDriver] = []
        # Prefer iterrows for column-name access (itertuples mangles names).
        try:
            iterator = results.iterrows()
        except AttributeError:
            return []

        for _, row in iterator:
            drivers.append(
                ExtractedDriver(
                    driver_number=clean_int(_row_get(row, "DriverNumber")),
                    abbreviation=clean_optional_str(_row_get(row, "Abbreviation")),
                    first_name=clean_optional_str(_row_get(row, "FirstName")),
                    last_name=clean_optional_str(_row_get(row, "LastName")),
                    full_name=clean_optional_str(_row_get(row, "FullName")),
                    team_name=clean_optional_str(_row_get(row, "TeamName")),
                    grid_position=clean_int(_row_get(row, "GridPosition")),
                    finish_position=clean_int(_row_get(row, "Position")),
                    result_status=clean_optional_str(_row_get(row, "Status")),
                    team_color=clean_team_color(_row_get(row, "TeamColor")),
                )
            )
        return drivers

    def _extract_driver_tracks(self, session: Any) -> list[ExtractedDriverTrack]:
        """One entry per classified driver; ``pos_data``/``car_data`` are keyed by number."""
        pos_data = _loaded_channel(session, "pos_data")
        car_data = _loaded_channel(session, "car_data")
        tracks: list[ExtractedDriverTrack] = []
        for driver in self._extract_drivers(getattr(session, "results", None)):
            key = str(driver.driver_number) if driver.driver_number is not None else None
            tracks.append(
                ExtractedDriverTrack(
                    abbreviation=driver.abbreviation,
                    driver_number=driver.driver_number,
                    team_color=driver.team_color,
                    positions=_position_series(pos_data.get(key)) if key else None,
                    car=_car_series(car_data.get(key)) if key else None,
                )
            )
        return tracks

    def _extract_circuit(self, session: Any) -> ExtractedCircuit | None:
        try:
            info = session.get_circuit_info()
        except Exception:
            logger.warning("Circuit info unavailable; corners will not be imported", exc_info=True)
            return None
        if info is None:
            return None
        rotation = getattr(info, "rotation", None)
        corners: list[ExtractedCorner] = []
        table = getattr(info, "corners", None)
        if table is not None:
            for _, row in table.iterrows():
                number = clean_int(_row_get(row, "Number"))
                x, y = _row_get(row, "X"), _row_get(row, "Y")
                if number is None or is_missing(x) or is_missing(y):
                    continue
                angle = _row_get(row, "Angle")
                corners.append(
                    ExtractedCorner(
                        number=number,
                        letter=clean_optional_str(_row_get(row, "Letter")) or "",
                        x=float(x),
                        y=float(y),
                        angle_deg=None if is_missing(angle) else float(angle),
                    )
                )
        return ExtractedCircuit(
            rotation_deg=None if is_missing(rotation) else float(rotation),
            corners=corners,
        )

    def _extract_laps(self, laps: Any) -> list[ExtractedLap]:
        if laps is None:
            return []
        try:
            if len(laps) == 0:
                return []
        except TypeError:
            return []

        extracted: list[ExtractedLap] = []
        try:
            iterator = laps.iterrows()
        except AttributeError:
            return []

        for _, row in iterator:
            extracted.append(
                ExtractedLap(
                    driver_abbreviation=clean_optional_str(_row_get(row, "Driver")),
                    driver_number=clean_int(_row_get(row, "DriverNumber")),
                    lap_number=clean_int(_row_get(row, "LapNumber")),
                    lap_time_ms=timedelta_to_ms(_row_get(row, "LapTime")),
                    position=clean_int(_row_get(row, "Position")),
                    compound=clean_optional_str(_row_get(row, "Compound")),
                    tyre_age_laps=clean_int(_row_get(row, "TyreLife")),
                    stint_number=clean_int(_row_get(row, "Stint")),
                    pit_in_time_ms=timedelta_to_ms(_row_get(row, "PitInTime")),
                    pit_out_time_ms=timedelta_to_ms(_row_get(row, "PitOutTime")),
                    sector1_time_ms=timedelta_to_ms(_row_get(row, "Sector1Time")),
                    sector2_time_ms=timedelta_to_ms(_row_get(row, "Sector2Time")),
                    sector3_time_ms=timedelta_to_ms(_row_get(row, "Sector3Time")),
                    lap_start_time_ms=timedelta_to_ms(_row_get(row, "LapStartTime")),
                    is_deleted=clean_bool(_row_get(row, "Deleted")),
                    is_accurate=clean_bool(_row_get(row, "IsAccurate")),
                    team_name=clean_optional_str(_row_get(row, "Team")),
                    lap_end_time_ms=timedelta_to_ms(_row_get(row, "Time")),
                )
            )
        return extracted

    def _extract_track_status(self, track_status: Any) -> list[ExtractedTrackStatus]:
        if track_status is None:
            logger.warning("Track status data missing; skipping track status import")
            return []
        try:
            if len(track_status) == 0:
                logger.warning("Track status data empty; skipping track status import")
                return []
        except TypeError:
            logger.warning("Track status data unavailable; skipping track status import")
            return []

        extracted: list[ExtractedTrackStatus] = []
        try:
            iterator = track_status.iterrows()
        except AttributeError:
            logger.warning("Track status data unreadable; skipping track status import")
            return []

        for _, row in iterator:
            extracted.append(
                ExtractedTrackStatus(
                    race_time_ms=timedelta_to_ms(_row_get(row, "Time")),
                    source_code=clean_optional_str(_row_get(row, "Status")),
                    message=clean_optional_str(_row_get(row, "Message")),
                )
            )
        return extracted

    def _map_get_error(
        self,
        exc: Exception,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> IngestionMappedError:
        message = str(exc).lower()
        event_label = str(event)
        if "session" in message and ("not found" in message or "does not exist" in message):
            return SessionNotFoundError(
                f"Session {session_type.value} not found for {season} event {event_label}"
            )
        if (
            "event" in message
            or "round" in message
            or "not found" in message
            or "does not exist" in message
            or "invalid" in message
        ):
            return EventNotFoundError(f"Event not found for season {season}: {event_label}")
        return SessionLoadError(
            f"Failed to resolve session {session_type.value} for {season} event {event_label}"
        )

    def _map_load_error(
        self,
        exc: Exception,
        season: int,
        event: int | str,
        session_type: SessionType,
    ) -> EventNotFoundError | SessionNotFoundError | SessionLoadError:
        message = str(exc).lower()
        event_label = str(event)
        if "session" in message and ("not found" in message or "does not exist" in message):
            return SessionNotFoundError(
                f"Session {session_type.value} not found for {season} event {event_label}"
            )
        if "event" in message and ("not found" in message or "does not exist" in message):
            return EventNotFoundError(f"Event not found for season {season}: {event_label}")
        return SessionLoadError(
            f"Failed to load session {session_type.value} for {season} event {event_label}"
        )


def _loaded_channel(session: Any, name: str) -> dict[str, Any]:
    """``pos_data`` / ``car_data`` by driver number; empty when FastF1 has none."""
    try:
        data = getattr(session, name)
    except Exception:  # FastF1 raises DataNotLoadedError when the channel is unavailable
        logger.warning("FastF1 %s unavailable for this session", name, exc_info=True)
        return {}
    return data if isinstance(data, dict) else dict(data or {})


def _session_time_ms(frame: Any) -> Any:
    return frame["SessionTime"].dt.total_seconds().mul(1000).round()


def _position_series(frame: Any) -> PositionSeries | None:
    """Rows with a session time and both coordinates, ordered by time."""
    if frame is None or len(frame) == 0 or "SessionTime" not in frame:
        return None
    frame = frame.assign(_ms=_session_time_ms(frame)).dropna(subset=["_ms", "X", "Y"])
    if len(frame) == 0:
        return None
    frame = frame.sort_values("_ms", kind="stable")
    status = frame.get("Status")
    return PositionSeries(
        session_time_ms=frame["_ms"].astype("int64").tolist(),
        x=frame["X"].round().astype("int64").tolist(),
        y=frame["Y"].round().astype("int64").tolist(),
        off_track=[None] * len(frame)
        if status is None
        else [_off_track(value) for value in status.tolist()],
    )


def _car_series(frame: Any) -> CarSeries | None:
    if frame is None or len(frame) == 0 or "SessionTime" not in frame:
        return None
    frame = frame.assign(_ms=_session_time_ms(frame)).dropna(subset=["_ms"])
    if len(frame) == 0:
        return None
    frame = frame.sort_values("_ms", kind="stable")

    def ints(column: str) -> list[int | None]:
        if column not in frame:
            return [None] * len(frame)
        return [clean_int(value) for value in frame[column].tolist()]

    return CarSeries(
        session_time_ms=frame["_ms"].astype("int64").tolist(),
        speed_kph=ints("Speed"),
        rpm=ints("RPM"),
        gear=ints("nGear"),
        throttle_pct=ints("Throttle"),
        brake=[clean_bool(v) for v in frame["Brake"].tolist()]
        if "Brake" in frame
        else [None] * len(frame),
        drs_code=ints("DRS"),
    )


def _off_track(status: Any) -> bool | None:
    text = clean_optional_str(status)
    if text == "OffTrack":
        return True
    if text == "OnTrack":
        return False
    return None


# Type alias used only for internal annotation clarity above.
IngestionMappedError = EventNotFoundError | SessionNotFoundError | SessionLoadError
