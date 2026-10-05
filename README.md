# F1 Replay Backend

Backend for the F1 Historical Race Replay and Event Detection Engine. It imports
historical Formula 1 sessions via FastF1, stores normalized domain data in
PostgreSQL, builds a deterministic event timeline, and replays it on a
controllable virtual race clock, and publishes the replayed events to Redis
Streams for independent consumers. Race-state processing, event detection and
WebSockets are not implemented yet.

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

## Race replay

A replay releases a session's stored historical timeline (`race_events`) in
`sequence` order as though the race were live. It never calls FastF1, never
builds or re-sorts the timeline, and does no race-state processing or detection.
Generate the timeline first (`POST /sessions/{id}/timeline`).

### Lifecycle

| Command | Allowed from | Result |
|---------|--------------|--------|
| create | — | `CREATED` |
| start | `CREATED` | `RUNNING` |
| pause | `RUNNING` | `PAUSED` |
| resume | `PAUSED` | `RUNNING` |
| stop | `RUNNING`, `PAUSED` | `STOPPED` |
| restart | `RUNNING`, `PAUSED`, `STOPPED`, `COMPLETED`, `FAILED` | `RUNNING` |
| (engine) complete | `RUNNING` | `COMPLETED` once the last event is emitted |
| (engine) fail | `RUNNING`, `PAUSED` | `FAILED` when emission raises (`PAUSED` only when a pause lands while a publish is in flight and that publish then raises) |

Any other command, including a repeat of the same command (start twice, pause
twice, stop twice), returns HTTP 409 with `current_status` and changes nothing.

- **Stop** terminates the replay. It cannot be resumed; use restart to run it again.
  An event already being published when stop arrives is allowed to finish (up to
  5 s; after that the worker is cancelled).
- **Restart** resets race time, event pointer, current lap, pause and end state,
  reloads the stored timeline (it never regenerates or duplicates it) and starts
  running immediately. Playback speed is kept.
- **Completion** freezes race time at the final event's time, records `ended_at`
  and ends the background task.
- **Failure** (the event publisher raised) records `FAILED` with `status_reason`.
  The failing event is not counted as emitted; restart to run again.
- **Backend restart**: execution is in-memory and does not survive a restart. On
  startup every persisted `RUNNING`/`PAUSED` replay is marked `STOPPED` with
  `status_reason` explaining it was interrupted; restart it explicitly. If the
  database is unreachable at startup, the same repair happens the next time that
  replay is read or commanded. On graceful shutdown live replays are stopped and
  persisted as `STOPPED`.

### Virtual clock and playback speed

Race time is `anchor_race_time + (real_elapsed × playback_speed)`. Pause, resume
and speed changes re-anchor at the current race time, so pausing freezes race time
exactly and a speed change (running or paused) continues from the same race time.
The engine sleeps until the next event is due on that clock (no fixed per-event or
per-lap delays, no drift) and emits every already-due event back to back, so high
speeds never reorder or delay events. A very large due batch yields to the event
loop every 100 events, so a pause or stop issued meanwhile takes effect mid-batch.

Supported speeds: `1`, `2`, `5`, `10`, `20`. Anything else (zero, negative,
fractional, extreme) returns HTTP 422. Speed can be changed in any status;
setting the current speed is a no-op.

### Progress fields

- `current_race_time_ms` — virtual race time (live while running).
- `current_sequence` — sequence of the last emitted event (`null` before the
  first); `emitted_event_count` = `current_sequence + 1`. Resume continues from
  the next event, so nothing is emitted twice within a run.
- `current_lap` — the race lap the leader is on: `1` once `RACE_STARTED` is
  emitted, `n + 1` after the first `LAP_COMPLETED` for lap `n`, capped at
  `total_laps` (highest completed lap in the timeline). Lapped cars never move it.

### Multiple replays and concurrency

Each replay has its own clock, event pointer, speed, status and background task,
so any number of replays (including several of the same session) run
independently. Every command for a replay is serialized by a per-replay lock, so
concurrent starts launch exactly one worker. While a replay's worker exists its
in-memory state is authoritative; PostgreSQL is written only at lifecycle
boundaries (create, start, pause, resume, speed change, stop, restart, completion,
failure), never per event or clock tick.

Execution and locks are per-process: run the API with a single worker.

### Database failures

Every database failure in the replay service (reads included) returns HTTP 503
`Replay storage unavailable`, or `Replay is <status> but the state is not saved
yet` when only the write failed. No database transaction is held while waiting for
a worker to stop.

A 503 from pause, resume, speed change or stop means the in-memory change
**applied** but is not yet durable. The in-memory state stays authoritative, so
`GET /replays/{id}` reports the true state (it does not fail just because the
deferred write failed), and repeating the command follows the normal rules (pause
on a paused replay is 409). The unsaved state is written again on the next
access to that replay (and on graceful shutdown). A stopped, completed or failed
replay is always unregistered from memory even if its save fails; its terminal
state is kept and reported, never replaced by the "interrupted" repair, and
persisted on the next access. Start and restart are atomic: if their save fails
nothing starts (503) and the replay is unchanged. A read that itself cannot reach
the database is a plain 503.

### Event emission

The engine publishes each event as a `ReplayEvent` (replay id, run id, session id,
`sequence`, type, race time, lap, driver, payload) to a `ReplayEventSink`
(`app/replay/events.py`), awaiting each publish in order. The engine does not know
what backs the sink. The API process wires a `RedisStreamPublisher` (see
[Event streaming](#event-streaming)); `LoggingEventSink` (DEBUG logging only) remains
available. A sink exception fails the replay and the event is not counted as
emitted. `run_id` is generated per start or restart, so consumers can tell a
restarted replay (which re-emits from sequence 0) from the original run.

### Endpoints

```bash
# Create (201); playback_speed defaults to 1
curl -X POST http://127.0.0.1:8000/replays \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "'$SESSION_ID'", "playback_speed": 5}'

curl http://127.0.0.1:8000/replays/$REPLAY_ID
curl -X POST http://127.0.0.1:8000/replays/$REPLAY_ID/start    # also pause, resume, stop, restart
curl -X PUT http://127.0.0.1:8000/replays/$REPLAY_ID/speed \
  -H 'Content-Type: application/json' -d '{"playback_speed": 10}'
```

Errors: 404 unknown replay or session; 409 invalid transition or no generated
timeline; 422 unsupported speed; 503 database unavailable or state not saved yet (see
"Database failures"; for pause/resume/speed/stop the in-memory change still
applies and is reported by later reads).

### Known limitations

- Seek is not implemented. The clock can be positioned (`VirtualRaceClock.reset`)
  and the event pointer is explicit, but seeking safely needs downstream state to
  be rebuilt, so it is deferred.
- Single process only (see above); no distributed locking.
- Regenerating a timeline does not affect a running replay (it keeps the copy it
  loaded); the new timeline is used on the next start/restart.
- If a worker is cancelled after the stop timeout, the event being published at
  that moment may or may not have reached the sink.

## Event streaming

```
Replay engine -> RedisStreamPublisher -> Redis Streams -> independent consumer groups
```

Producers and consumers are decoupled: the publisher only appends to a stream and
never waits for consumers. Each consumer group receives every message and keeps its
own progress, so a slow or failing group affects nobody else. Consumers are **not**
started inside the API process; they run as separate processes (for example
`python -m app.streaming.cli consume`).

### Streams

| Stream | Default name | Purpose |
|--------|--------------|---------|
| raw | `race.raw.events` | Historical timeline events released by replays (used now) |
| state | `race.state.events` | Reserved for derived race-state events |
| detected | `race.detected.events` | Reserved for detected analytical events |
| dead letter | `race.dead_letter.events` | Messages that could not be processed |

All replays share the one raw stream; events are separated by `replay_id` and
`run_id`. Per-replay streams would need dynamic group creation and cleanup and force
consumers to discover streams. Names and tuning come from settings (below).

Consumer group names live in `app/streaming/config.py`: `race-state-processors`,
`event-detectors`, `event-persisters`, `websocket-gateway` and the validation group
`raw-event-auditors`. Consumer names are unique per process
(`{prefix}-{hostname}-{pid}-{short uuid}`).

### Envelope

Each entry has small flat headers (`event_id`, `schema_version`, `event_type`,
`replay_id`, `run_id`, `sequence`) for inspection with `XRANGE`, plus `data`: the full
envelope as compact, key-sorted JSON (`event_id`, `schema_version`, `event_type`,
`replay_id`, `run_id`, `session_id`, `sequence`, `race_time_ms`, `lap_number`,
`driver_id`, `driver_abbreviation`, `published_at`, `payload`). `data` is the source
of truth. UUIDs are strings, datetimes ISO-8601 UTC, decimals strings; any other
payload type is rejected with a serialization error (no pickle).

### Versioning

`STREAM_EVENT_SCHEMA_VERSION` is currently `1`. Consumers declare the versions they
support (default `{1}`). Adding optional fields is non-breaking and needs no bump;
removing, renaming or retyping a field, or changing its meaning, requires a bump. A
message with an unsupported version is dead-lettered without retries.

### Identifiers

- `event_id`: deterministic `uuid5(run_id, str(sequence))`. A retried publish of the
  same logical event has the same id, so consumers can deduplicate.
- `sequence`: position in the replay timeline (contiguous from 0 within a run).
- `run_id`: one start or restart of a replay.
- Redis message id (for example `1700000000000-0`): assigned by Redis on XADD, not
  part of the envelope; consumers see it as `ReceivedMessage.message_id`.

### Publisher behaviour

`RedisStreamPublisher` uses the app's shared Redis connection pool. Connection and
timeout errors are retried a bounded number of times with a short backoff; then a
`StreamPublishError` is raised, which fails the replay (`FAILED`, `status_reason` set,
the event is not counted and not skipped). Other Redis errors fail immediately. App
startup does not require Redis. Delivery is at-least-once: if an XADD succeeds but
its reply is lost, the retry can append the same event twice with the same `event_id`.

### Consumers

`StreamConsumer` (`app/streaming/consumer.py`) provides:

- group creation with `XGROUP CREATE ... MKSTREAM` (idempotent). The default start id
  is `$` (only messages published after the group exists); create groups before
  replays start or set `group_start_id` to `0` to read the retained backlog.
- blocking `XREADGROUP` reads (no busy polling) and processing of the consumer's own
  pending list on startup.
- acknowledgement only after the handler succeeds. A failing handler leaves the
  message pending; later messages are not blocked.
- retry: messages idle longer than `STREAM_RECLAIM_IDLE_MS` are claimed with
  `XAUTOCLAIM` (by any consumer in the group) and handled again.
- dead letter: malformed messages, unsupported versions and messages delivered more
  than `STREAM_MAX_DELIVERIES` times are written to the dead-letter stream (original
  data, stream, message id, group, consumer, reason, delivery count, time) and
  acknowledged in one MULTI/EXEC transaction. For the delivery limit the handler ran
  `STREAM_MAX_DELIVERIES` times and the recorded `delivery_count` is one higher (the
  delivery that triggered dead-lettering).
- idempotency: an `IdempotencyStore` (`RedisIdempotencyStore`, key
  `stream:processed:{group}:{event_id}` with TTL) skips events already handled.

This is at-least-once delivery with idempotent consumers, not exactly-once: a crash
after a handler finished but before the event is marked processed causes a
redelivery, so handlers must be idempotent. `app/streaming/audit.py` is a minimal
validation handler (logs event id, replay id, run id and sequence only).

### Retention and backpressure

The raw stream is trimmed approximately (`XADD MAXLEN ~ STREAM_MAXLEN`, `0` disables
trimming). Trimming can drop messages a lagging group has not consumed yet, so size
the limit for the largest expected backlog. The publisher never slows down for
consumers; watch consumer lag and pending counts instead (CLI below). Running
consumers log their backlog periodically, not per event.

### Debugging

```bash
uv run python -m app.streaming.cli info                      # length, ids, groups, lag
uv run python -m app.streaming.cli tail --count 5            # decoded latest events
uv run python -m app.streaming.cli pending --group raw-event-auditors
uv run python -m app.streaming.cli dead-letters
uv run python -m app.streaming.cli consume                   # validation consumer

redis-cli XINFO STREAM race.raw.events
redis-cli XINFO GROUPS race.raw.events
redis-cli XPENDING race.raw.events raw-event-auditors
redis-cli XRANGE race.raw.events - + COUNT 5
redis-cli XRANGE race.dead_letter.events - + COUNT 5
```

No HTTP endpoint exposes Redis.

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

Streaming tests use fakeredis. To also run them against a real Redis, point
`REDIS_TEST_URL` at a disposable instance (never the application's Redis; use a
separate database number). Tests use uniquely named streams and delete them
afterwards; they also delete every `stream:processed:*` idempotency key in that
database, so it must hold nothing else you care about.

```bash
REDIS_TEST_URL=redis://localhost:6379/15 uv run pytest tests/streaming
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
| `STREAM_RAW_EVENTS` | no | Raw events stream name (default `race.raw.events`) |
| `STREAM_STATE_EVENTS` | no | Reserved state events stream (default `race.state.events`) |
| `STREAM_DETECTED_EVENTS` | no | Reserved detected events stream (default `race.detected.events`) |
| `STREAM_DEAD_LETTER` | no | Dead-letter stream (default `race.dead_letter.events`) |
| `STREAM_MAXLEN` | no | Approximate raw stream retention, `0` = unbounded (default `100000`) |
| `STREAM_DEAD_LETTER_MAXLEN` | no | Approximate dead-letter retention (default `10000`) |
| `STREAM_READ_COUNT` | no | Messages per consumer read (default `100`) |
| `STREAM_BLOCK_MS` | no | Consumer blocking read timeout in ms (default `5000`) |
| `STREAM_MAX_DELIVERIES` | no | Deliveries before dead-lettering (default `5`) |
| `STREAM_RECLAIM_IDLE_MS` | no | Idle time before a pending message is reclaimed (default `30000`) |
| `STREAM_PUBLISH_ATTEMPTS` | no | Publish attempts on connection errors (default `3`) |
| `STREAM_IDEMPOTENCY_TTL_SECONDS` | no | Processed-event marker TTL (default `86400`) |

Copy `.env.example` for local values. Do not commit `.env`.
