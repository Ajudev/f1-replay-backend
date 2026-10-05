"""Normalize extracted FastF1 records into persistence-ready structures.

No pandas, FastF1, or ORM imports.
"""

from __future__ import annotations

from collections import defaultdict

from app.domain.enums import TrackStatus
from app.ingestion.errors import NormalizationError
from app.ingestion.records import (
    ExtractedSession,
    NormalizedDriver,
    NormalizedLap,
    NormalizedSector,
    NormalizedSession,
    NormalizedStint,
    NormalizedTrackStatus,
)

_MAX_WARNINGS = 50


def map_track_status_code(source_code: str) -> TrackStatus:
    """Map a FastF1 track-status code string to TrackStatus.

    Codes may be concatenated (e.g. ``"26"``). Priority:
    5 → RED_FLAG, 4 → SAFETY_CAR, 6 → VSC, 7 → VSC ending, 2 → YELLOW, 1 → GREEN.
    """
    code = source_code.strip()
    if "5" in code:
        return TrackStatus.RED_FLAG
    if "4" in code:
        return TrackStatus.SAFETY_CAR
    if "6" in code:
        return TrackStatus.VIRTUAL_SAFETY_CAR
    if "7" in code:
        return TrackStatus.VIRTUAL_SAFETY_CAR_ENDING
    if "2" in code:
        return TrackStatus.YELLOW
    if "1" in code:
        return TrackStatus.GREEN
    return TrackStatus.UNKNOWN


def normalize(extracted: ExtractedSession) -> NormalizedSession:
    """Convert an extracted session into normalized records ready for persistence."""
    if extracted.season is None:
        raise NormalizationError("Extracted session is missing season")
    if extracted.round is None:
        raise NormalizationError("Extracted session is missing round")
    if not extracted.event_name:
        raise NormalizationError("Extracted session is missing event name")

    warnings: list[str] = []
    skipped_count = 0

    drivers: list[NormalizedDriver] = []
    valid_abbreviations: set[str] = set()
    drivers_with_laps_no_stint: set[str] = set()

    for driver in extracted.drivers:
        abbr = (driver.abbreviation or "").strip().upper()
        if len(abbr) != 3:
            skipped_count += 1
            warnings.append(f"Skipped driver with invalid abbreviation: {driver.abbreviation!r}")
            continue

        if abbr in valid_abbreviations:
            raise NormalizationError(f"Duplicate driver abbreviation in session data: {abbr}")

        full_name = _resolve_full_name(driver.full_name, driver.first_name, driver.last_name, abbr)
        drivers.append(
            NormalizedDriver(
                driver_number=driver.driver_number,
                abbreviation=abbr,
                full_name=full_name,
                first_name=driver.first_name,
                last_name=driver.last_name,
                team_name=driver.team_name,
                grid_position=_positive_or_none(driver.grid_position),
                finish_position=_positive_or_none(driver.finish_position),
                result_status=driver.result_status,
            )
        )
        valid_abbreviations.add(abbr)

    laps: list[NormalizedLap] = []
    stint_laps: dict[tuple[str, int], list[NormalizedLap]] = defaultdict(list)

    for lap in extracted.laps:
        abbr_raw = (lap.driver_abbreviation or "").strip().upper()
        if not abbr_raw or len(abbr_raw) != 3 or abbr_raw not in valid_abbreviations:
            # Skip laps for unknown/invalid/skipped drivers
            if abbr_raw and len(abbr_raw) != 3:
                skipped_count += 1
                warnings.append(f"Skipped lap for invalid driver abbreviation: {abbr_raw!r}")
            elif abbr_raw and abbr_raw not in valid_abbreviations:
                skipped_count += 1
                warnings.append(f"Skipped lap for unknown/skipped driver: {abbr_raw}")
            else:
                skipped_count += 1
                warnings.append("Skipped lap with missing driver abbreviation")
            continue

        if lap.lap_number is None or lap.lap_number < 1:
            skipped_count += 1
            warnings.append(
                f"Skipped lap with invalid lap number for {abbr_raw}: {lap.lap_number!r}"
            )
            continue

        pit_in = lap.pit_in_time_ms
        pit_out = lap.pit_out_time_ms
        pit_duration: int | None = None
        if pit_in is not None and pit_out is not None and pit_out >= pit_in:
            pit_duration = pit_out - pit_in

        compound = lap.compound.strip().upper() if lap.compound else None

        sectors: list[NormalizedSector] = []
        for number, time_ms in (
            (1, lap.sector1_time_ms),
            (2, lap.sector2_time_ms),
            (3, lap.sector3_time_ms),
        ):
            if time_ms is not None:
                sectors.append(NormalizedSector(sector_number=number, sector_time_ms=time_ms))

        stint_number = _positive_or_none(lap.stint_number)
        normalized_lap = NormalizedLap(
            driver_abbreviation=abbr_raw,
            lap_number=lap.lap_number,
            lap_time_ms=lap.lap_time_ms,
            position=_positive_or_none(lap.position),
            compound=compound,
            tyre_age_laps=lap.tyre_age_laps
            if lap.tyre_age_laps is not None and lap.tyre_age_laps >= 0
            else None,
            stint_number=stint_number,
            is_deleted=lap.is_deleted,
            is_accurate=lap.is_accurate,
            lap_start_time_ms=lap.lap_start_time_ms,
            pit_in_time_ms=pit_in,
            pit_out_time_ms=pit_out,
            is_pit_in_lap=pit_in is not None,
            is_pit_out_lap=pit_out is not None,
            pit_duration_ms=pit_duration,
            lap_end_time_ms=lap.lap_end_time_ms,
            sectors=sectors,
        )
        laps.append(normalized_lap)

        if stint_number is not None:
            stint_laps[(abbr_raw, stint_number)].append(normalized_lap)
        else:
            drivers_with_laps_no_stint.add(abbr_raw)

    for abbr in sorted(drivers_with_laps_no_stint):
        # Only warn when the driver has laps but none with stint numbers
        has_any_stint = any(a == abbr for a, _ in stint_laps)
        if not has_any_stint:
            warnings.append(f"Driver {abbr} has laps but no stint numbers; stints not invented")

    stints: list[NormalizedStint] = []
    for (abbr, stint_number), group in sorted(stint_laps.items(), key=lambda x: (x[0][0], x[0][1])):
        group_sorted = sorted(group, key=lambda item: item.lap_number)
        compound = next((item.compound for item in group_sorted if item.compound), None)
        if compound is None:
            skipped_count += 1
            warnings.append(f"Skipped stint {stint_number} for {abbr}: no compound on any lap")
            continue
        earliest = group_sorted[0]
        stints.append(
            NormalizedStint(
                driver_abbreviation=abbr,
                stint_number=stint_number,
                compound=compound,
                start_lap=group_sorted[0].lap_number,
                end_lap=group_sorted[-1].lap_number,
                tyre_age_at_start=earliest.tyre_age_laps,
            )
        )

    track_statuses: list[NormalizedTrackStatus] = []
    timed_statuses = []
    for status in extracted.track_statuses:
        if status.race_time_ms is None:
            skipped_count += 1
            warnings.append("Skipped track status row with missing time")
            continue
        source_code = status.source_code if status.source_code else ""
        timed_statuses.append(
            (
                status.race_time_ms,
                source_code,
                status.message,
            )
        )

    timed_statuses.sort(key=lambda item: item[0])
    for sequence, (race_time_ms, source_code, message) in enumerate(timed_statuses):
        track_statuses.append(
            NormalizedTrackStatus(
                race_time_ms=race_time_ms,
                status=map_track_status_code(source_code) if source_code else TrackStatus.UNKNOWN,
                source_code=source_code or "",
                message=message,
                sequence=sequence,
            )
        )

    session_name = extracted.session_name or extracted.session_type.value

    return NormalizedSession(
        season=extracted.season,
        round=extracted.round,
        event_name=extracted.event_name,
        official_event_name=extracted.official_event_name,
        country=extracted.country,
        location=extracted.location,
        event_date=extracted.event_date,
        session_type=extracted.session_type,
        session_name=session_name,
        session_start=extracted.session_start,
        drivers=drivers,
        laps=laps,
        stints=stints,
        track_statuses=track_statuses,
        warnings=warnings[:_MAX_WARNINGS],
        skipped_count=skipped_count,
    )


def _resolve_full_name(
    full_name: str | None,
    first_name: str | None,
    last_name: str | None,
    abbreviation: str,
) -> str:
    if full_name:
        return full_name
    parts = [part for part in (first_name, last_name) if part]
    if parts:
        return " ".join(parts)
    return abbreviation


def _positive_or_none(value: int | None) -> int | None:
    if value is None or value < 1:
        return None
    return value
