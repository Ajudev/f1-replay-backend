# F1 Replay Backend

Backend for the F1 Historical Race Replay and Event Detection Engine. It imports
historical Formula 1 sessions via FastF1, stores normalized domain data in
PostgreSQL, and exposes FastAPI endpoints to read that data. Replay, event
detection, and streaming are out of scope for the current import pipeline.

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
  number, deleted/accurate flags, session-relative lap start and pit timestamps.
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
