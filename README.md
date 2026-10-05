# F1 Replay Backend

Backend for the F1 Historical Race Replay and Event Detection Engine. It imports
historical Formula 1 sessions via FastF1, stores normalized domain data in
PostgreSQL, and exposes FastAPI endpoints to read that data. Replay, event
detection, and streaming are not implemented yet.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker (optional, for local Postgres 16 and Redis 7)

## Setup

```bash
# Install dependencies
uv sync

# Configure environment
cp .env.example .env

# Start local Postgres and Redis
docker compose up -d

# Apply migrations
uv run alembic upgrade head

# Start the API
uv run fastapi dev
```

The API listens on `API_HOST`:`API_PORT` (defaults: `127.0.0.1:8000`).

Health endpoints:

- `GET /health` — liveness
- `GET /ready` — readiness (Postgres + Redis)

## Race import

`POST /races/import` loads a historical session through FastF1, normalizes it into
plain application records, and persists those records in PostgreSQL. After a
successful import, subsequent reads come only from the database — no FastF1 types
are involved in query or API layers.

Example:

```bash
curl -X POST http://127.0.0.1:8000/races/import \
  -H 'Content-Type: application/json' \
  -d '{"season": 2024, "round": 1, "session_type": "RACE"}'
```

Provide exactly one of `round` or `event_name`. Optional `replace: true` rebuilds
an already-imported session for that race and session type.

### What is persisted

- Race weekend metadata (`season`, `round`, `name`, `official_name`, `country`,
  `location`, `event_date`). `circuit_name` stays null — FastF1 `Location` is the
  locality, not a separate circuit name.
- Session metadata (`session_type`, name, start time).
- Drivers as session participation (abbreviation, names, team, grid position,
  classified finish position, result status).
- Laps with timing, lap-end classification position, compound, tyre age, stint
  number, deleted/accurate flags, session-relative lap start, lap end (completion) and pit
  timestamps.
- Sector times when present (missing sector times are omitted, not zeroed).
- Tyre stints derived from lap stint numbers.
- Track status periods (session-relative start times and mapped status).

Lap `position` is the lap-end classification from FastF1. Grid and finish
positions live on the driver record and are never copied from lap position.

### Missing data

- Optional fields become `null` when FastF1 has NaN/NaT/empty values.
- Invalid driver abbreviations and their laps are skipped (warning + skip count).
- Invalid lap numbers are skipped.
- Track-status rows without a time are skipped.
- Empty track-status datasets produce a warning and no rows.
- Warnings are capped at 50 strings; `skipped_count` keeps the full total.

### Duplicate imports

A second import of the same race session without `replace` returns
`already_imported` (HTTP 200) and leaves the stored session untouched.
`replace: true` deletes that session (cascading children) and inserts a fresh
copy. The race weekend row is reused, not duplicated.

### Known limitations

- Car telemetry is not imported.
- `pit_duration_ms` is set only when pit-in and pit-out timestamps share the same
  lap and pit-out ≥ pit-in; otherwise each timestamp is stored on its own lap and
  duration stays null.
- FastF1 does not provide a separate circuit name.
- Combined track-status codes use a fixed priority mapping (red flag > safety car
  > VSC > VSC ending > yellow > green > unknown).
- Deleted laps are stored with `is_deleted` rather than dropped.
- Sprint qualifying loads FastF1 identifier `SQ` first; if that session is not
  found, it retries once with `SS` (Sprint Shootout). Other session types use a
  single identifier. Generic download or load failures are not retried with `SS`.
- A repeat import by round number with `replace: false` returns the existing
  session without calling FastF1 again. Imports by event name still need FastF1
  to resolve the round, then short-circuit without writing duplicates.

## Historical race timeline

The timeline turns the normalized race data already in PostgreSQL into one
deterministic, chronologically ordered, persisted stream of structured events
(stored in `race_events`). A future replay engine can consume it without touching
FastF1. It is built only on explicit request; reads never generate it implicitly.

Supported sessions: `RACE` and `SPRINT`. Other session types return HTTP 422.

### Event types

| Type | Emitted when |
|------|--------------|
| `RACE_STARTED` | Exactly once, `sequence` 0, `race_time_ms` 0. Payload carries the epoch, driver count and starting grid. |
| `TRACK_STATUS_CHANGED` | The track status changes (consecutive identical statuses are collapsed). Statuses that begin before the race start collapse into one event at 0. Payload: `status`, `previous_status`, `source_code`. |
| `LAP_COMPLETED` | A driver completes a lap. Payload: lap time, position, tyre data, stint, deleted/accurate flags, pit flags, `track_status` in effect, `completion_time_source`. |
| `PIT_ENTRY` / `PIT_EXIT` | From the stored pit-in / pit-out timestamps. `PIT_EXIT` carries `pit_lane_duration_ms` when it can be paired with the same driver's pit-in (previous or same lap), otherwise null. |
| `POSITION_CHANGED` | A driver's lap-end position differs from their previous known position. Payload: `previous_position`, `new_position`, `previous_position_source` (`GRID` or `LAP`), `granularity` (`LAP_END`). |
| `FASTEST_LAP` | A non-deleted lap is strictly faster than the race fastest so far. Payload includes the previous holder and time. |

Payloads are structured data only; no display strings.

### Timing semantics

- `race_time_ms` is integer milliseconds since the race start and is always >= 0.
- Stored times are FastF1 session-relative. The epoch is the earliest lap 1
  `lap_start_time_ms` across drivers; every session time `t` becomes `t - epoch`.
  The epoch is stored on the timeline summary (`race_start_session_time_ms`) and in
  the `RACE_STARTED` payload. If no lap 1 start time exists, generation fails with
  HTTP 422 rather than guessing.
- A lap completes at `lap_end_time_ms`; if that is missing, at
  `lap_start_time_ms + lap_time_ms` (`completion_time_source` says which). Laps with
  neither are skipped and counted in the summary warnings.
- Pit-in / pit-out times before the race start are excluded with a counted warning
  (not clamped). A negative `LAP_COMPLETED` time, or any other event that would land
  before t=0, is never clamped: it fails validation and `POST` returns HTTP 422 with a
  `problems` list.

### Ordering

Events are sorted once by `(race_time_ms, priority, lap_number, driver abbreviation,
tie-break)` and then numbered `sequence` 0..n-1. Priority at identical timestamps:

`RACE_STARTED` 0, `TRACK_STATUS_CHANGED` 10, `PIT_ENTRY` 20, `LAP_COMPLETED` 30,
`POSITION_CHANGED` 40, `FASTEST_LAP` 50, `PIT_EXIT` 60 (25 is reserved for
`SECTOR_COMPLETED`). Missing lap numbers / drivers sort first; track status events
tie-break on their source sequence. Output does not depend on input row order, so the
same data always yields the same events.

Validation before persisting checks: one `RACE_STARTED` first at t=0, contiguous
sequences, non-negative non-decreasing times, known drivers, one `LAP_COMPLETED` per
existing driver/lap, and per-driver lap completion times non-decreasing with lap number.

### Persistence and regeneration

`POST /sessions/{id}/timeline` builds and stores the events plus a `session_timelines`
metadata row. A repeat call returns `already_generated` (HTTP 200) without rewriting.
With `{"regenerate": true}` the old events and metadata are deleted and the new ones
inserted in one transaction (a failed build leaves the old timeline in place).
Regeneration deletes **all** `race_events` for that session, because only the timeline
writes them today. Generation is serialized per session with a row lock on the session: concurrent
requests queue, a concurrent first generate then returns `already_generated`, and a
concurrent regenerate simply regenerates again after the previous one commits.
HTTP 409 is only a safety net if a unique-constraint conflict still occurs.

A summary's `is_current_schema_version` is false when the stored timeline was built
with an older schema version. `POST` without `regenerate` still returns
`already_generated` (and logs a warning); `{"regenerate": true}` upgrades it.

### Endpoints

```bash
# Generate (201 generated/regenerated, 200 already_generated)
curl -X POST http://127.0.0.1:8000/sessions/$SESSION_ID/timeline \
  -H 'Content-Type: application/json' -d '{"regenerate": false}'

# Events ordered by sequence; filters: driver, event_type (repeatable), lap_from,
# lap_to, limit (1-1000, default 500), offset
curl "http://127.0.0.1:8000/sessions/$SESSION_ID/timeline?driver=NOR&event_type=PIT_ENTRY&event_type=PIT_EXIT"

# Metadata, counts by type and warnings
curl http://127.0.0.1:8000/sessions/$SESSION_ID/timeline/summary
```

Reading a timeline that has not been generated returns HTTP 404 explaining how to
generate it. Events without a lap (`RACE_STARTED`, `TRACK_STATUS_CHANGED`) are
excluded when `lap_from` / `lap_to` is used.

### Known limitations

- Position changes are detected at lap-end granularity; the event time is when the
  lap completed, not the moment of the overtake. Laps without a completion time are
  ignored for position tracking.
- `SECTOR_COMPLETED` is deferred: only sector durations are stored, not sector
  completion timestamps, so accurate event times cannot be derived.
- Pit times before the race start (pre-race pit lane / grid exits on lap 1) are
  excluded and counted in the warnings. Missing pit times are never inferred.
- Laps without a stored `lap_end_time_ms` (for example imported before the column
  existed) complete at the driver's next lap start (`NEXT_LAP_START`), else at
  `lap_start_time_ms + lap_time_ms` (`LAP_START_PLUS_LAP_TIME`). The last fallback is
  slightly early on lap 1 (standing-start offset of roughly 250 ms). Re-import with
  `replace: true` to populate `lap_end_time_ms`.
- Track status periods are processed in chronological order (`session_time_ms`, then
  `sequence`), so out-of-order source rows still yield a consistent status chain.
- If the built timeline fails validation (inconsistent source data, e.g. a lap
  completing before the previous one), `POST` returns HTTP 422 with a `problems` list.
- No detection (battles, pace) or replay timing is part of the timeline.

## Read endpoints

- `GET /races`
- `GET /races/{race_id}`
- `GET /sessions/{session_id}`
- `GET /sessions/{session_id}/laps`
- `GET /sessions/{session_id}/stints`
- `GET /sessions/{session_id}/track-status`

## Tests

Default suite (no FastF1 downloads):

```bash
uv run pytest
```

Optional live FastF1 smoke test:

```bash
FASTF1_RUN_EXTERNAL=1 uv run pytest -m external
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `APP_ENV` | no | Environment name |
| `APP_NAME` | no | Application name |
| `LOG_LEVEL` | no | Logging level |
| `DATABASE_URL` | **yes** | Async SQLAlchemy URL (`postgresql+asyncpg://...`) |
| `REDIS_URL` | no | Redis connection URL |
| `API_HOST` | no | Bind host |
| `API_PORT` | no | Bind port |
| `FASTF1_CACHE_DIR` | no | FastF1 cache directory (default `.fastf1-cache`) |

Copy `.env.example` for local values. Do not commit `.env`.
