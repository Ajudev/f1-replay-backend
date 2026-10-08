# F1 Replay Backend

Backend for the F1 Historical Race Replay and Event Detection Engine. It imports
historical Formula 1 sessions via FastF1, stores normalized domain data in
PostgreSQL, builds a deterministic event timeline, and replays it on a
controllable virtual race clock, publishes the replayed events to Redis
Streams for independent consumers, and reduces them into a live race state served
over REST. Event detection and WebSockets are not implemented yet.

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

The API listens on `API_HOST`:`API_PORT` (defaults: `127.0.0.1:8000`). All application
endpoints live under `/api/v1` (see "HTTP API"); interactive OpenAPI docs are served at `/docs`.

Health endpoints:

- `GET /health` — liveness (unversioned)
- `GET /ready` — readiness (Postgres + Redis; unversioned)

## Race import

`POST /api/v1/races/import` loads a historical session through FastF1, normalizes it into
plain application records, and persists those records in PostgreSQL. After a
successful import, subsequent reads come only from the database — no FastF1 types
are involved in query or API layers.

Example:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/races/import \
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

`POST /api/v1/sessions/{id}/timeline` builds and stores the events plus a `session_timelines`
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
curl -X POST http://127.0.0.1:8000/api/v1/sessions/$SESSION_ID/timeline \
  -H 'Content-Type: application/json' -d '{"regenerate": false}'

# Events ordered by sequence; filters: driver, event_type (repeatable), lap_from,
# lap_to, limit (1-1000, default 500), offset
curl "http://127.0.0.1:8000/api/v1/sessions/$SESSION_ID/timeline?driver=NOR&event_type=PIT_ENTRY&event_type=PIT_EXIT"

# Metadata, counts by type and warnings
curl http://127.0.0.1:8000/api/v1/sessions/$SESSION_ID/timeline/summary
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
Generate the timeline first (`POST /api/v1/sessions/{id}/timeline`).

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
`GET /api/v1/replays/{id}` reports the true state (it does not fail just because the
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
curl -X POST http://127.0.0.1:8000/api/v1/replays \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "'$SESSION_ID'", "playback_speed": 5}'

curl http://127.0.0.1:8000/api/v1/replays/$REPLAY_ID
curl -X POST http://127.0.0.1:8000/api/v1/replays/$REPLAY_ID/start    # also pause, resume, stop, restart
curl -X PATCH http://127.0.0.1:8000/api/v1/replays/$REPLAY_ID/speed \
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
| state | `race.state.events` | Incremental race state events published by the race state worker (see [Race state](#race-state)) |
| detected | `race.detected.events` | Reserved for detected analytical events |
| dead letter | `race.dead_letter.events` | Messages that could not be processed |

All replays share the one raw stream; events are separated by `replay_id` and
`run_id`. Per-replay streams would need dynamic group creation and cleanup and force
consumers to discover streams. Names and tuning come from settings (below).

Consumer group names live in `app/streaming/config.py`: `race-state-processors`,
`race-event-detectors`, `event-persisters`, `websocket-gateway` and the validation group
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

## Race state

The race state engine consumes the raw replay events and deterministically reduces
them into the current state of the race: positions, laps, tyres, pit status, gaps,
track status and fastest lap. It does no detection (battles, degradation, pace
anomalies) and never touches FastF1.

```
race.raw.events -> StreamConsumer (group race-state-processors) -> RaceStateProcessor
                     |-> reducer (pure) -> Redis  race:{replay_id}:state  (hot state)
                     |                  -> race.state.events              (state events)
                     `-> PostgreSQL race_state_snapshots                  (recovery points)
```

The processor runs as its own process, never inside the API:

```bash
uv run python -m app.race_state.worker     # SIGINT / SIGTERM stop it gracefully
```

### State structure

`GET /api/v1/replays/{id}/state` returns (additional bookkeeping such as the gap crossing
window stays internal):

| Field | Meaning |
|-------|---------|
| `replay_id`, `run_id`, `session_id`, `race_id`, `season`, `round`, `session_type` | Identity. `run_id` changes on every start or restart. |
| `source` | `live` (Redis) or `snapshot` (PostgreSQL fallback). `replay_status` is the replay lifecycle status. |
| `phase` | `PRE_RACE`, `RUNNING`, `CHEQUERED` (the leader finished the final lap), `COMPLETED` (last timeline event applied). |
| `current_race_time_ms`, `last_sequence`, `last_event_id`, `total_events` | Race time and sequence of the last applied event. |
| `current_lap`, `total_laps`, `leader_laps_completed`, `leader_driver_id` / `_abbreviation` | See "Current lap" below. |
| `track_status` | `null` until the first `TRACK_STATUS_CHANGED`, then the normalized status. |
| `fastest_lap` | Race fastest lap: driver, lap number, lap time, race time. |
| `drivers` | Ordered by position (unknown positions last, then laps completed, earliest last crossing, abbreviation). |

Driver fields: identity (`driver_id`, `abbreviation`, `driver_number`, `full_name`,
`team_name`, `grid_position`), `position` / `previous_position`, `laps_completed`,
`current_lap` (`laps_completed + 1`, capped at `total_laps`; `null` before the start and after
finishing), `last_lap_time_ms`, `best_lap_time_ms` / `best_lap_number`, `gap_to_leader_ms`,
`interval_to_ahead_ms`, `gap_basis` (`LAP_END`), `laps_behind_leader`, tyres (`compound`,
`tyre_age_laps`, `stint_number`, `tyre_info_lap`), pit data (`pit_status` `UNKNOWN` /
`IN_PIT` / `ON_TRACK`, `pit_stop_count`, last entry/exit race time, last pit lane
duration), `race_status` (`NOT_STARTED`, `RUNNING`, `FINISHED`, `DID_NOT_FINISH`) and a
bounded `recent_laps` list (`RACE_STATE_LAP_HISTORY` entries). There are no sector fields:
`SECTOR_COMPLETED` is never emitted.

The reducer (`app/race_state/reducer.py`) is pure: no I/O, no wall clock, input never
mutated. The same events always produce the same state, whether they arrive from the
stream or are replayed from the stored timeline during a rebuild. The initial state is
seeded once per run from PostgreSQL (drivers with their *grid* positions; finishing
positions are never used because they would leak the future). Tyre fields stay `null` until
an event provides them, and tyre age is copied from the data, never incremented.

### Current lap

Same rule as the replay engine: the leader is the driver with the most completed laps, ties
broken by the earliest crossing of that lap; `current_lap = min(leader_laps_completed + 1,
total_laps)`, `1` after `RACE_STARTED`, and lapped cars never move it. `total_laps` is the
highest completed lap in the timeline. Note that the leader is derived from crossings, so it
can differ from the driver reported in position 1 when the data disagrees.

### Redis hot state and state events

- Key `race:{replay_id}:state` (prefix configurable): one JSON document holding the full
  state, with a TTL (default 7 days) refreshed on every write. One `GET` and one
  `WATCH`/`MULTI`/`EXEC` per processed event.
- In that single transaction the state is written **and** the state event is added to
  `race.state.events` (approximate `MAXLEN`, the same `STREAM_MAXLEN`). A transition is
  therefore never stored without being published, nor published twice. If another worker
  changed the document in between, the transaction aborts (`StateConflictError`) and the message is
  retried.
- State events reuse the stream envelope (decodable with `StreamEvent.from_fields`; `event_id`
  is `uuid5(run_id, "state:<sequence>")`, distinct from the raw event id; `sequence` is the
  source raw sequence; `race_time_ms` / `lap_number` come from the state). Types:
  `STATE_INITIALIZED` (full snapshot after `RACE_STARTED`), `STATE_UPDATED` (delta only),
  `STATE_REBUILT` (full snapshot after a rebuild), `STATE_COMPLETED` (full final snapshot; it
  replaces the final delta but still lists `kinds` and `changes`).
- Payload: `source_event_id`, `source_event_type`, `last_sequence`, `rebuilt`, `kinds`
  (`RACE_STARTED`, `LAP_COMPLETED`, `POSITION_CHANGED`, `PIT_STATUS_CHANGED`,
  `TRACK_STATUS_CHANGED`, `FASTEST_LAP_CHANGED`, `RACE_COMPLETED`), `changes.race` (changed
  race-level fields) and `changes.drivers` (full state of each changed driver); snapshot
  types also carry `state`. Events that change nothing (a `POSITION_CHANGED` already applied
  by its `LAP_COMPLETED`, sector or unknown events) publish nothing, although the stored
  sequence still advances.

### Ordering and idempotency

Per replay the state records `run_id`, `last_sequence` and the run's `published_at`:

| Situation | Behavior |
|-----------|----------|
| same run, `seq <= last_sequence` | duplicate: nothing changes, nothing published, ACK |
| same run, `seq == last_sequence + 1` | applied |
| same run, `seq > last_sequence + 1` | gap: the event waits in process up to `RACE_STATE_GAP_WAIT_MS` (re-reading every 100 ms, `WATCH` released in between) for the predecessor or a concurrent worker; if the gap persists, the missing events are reduced from the stored timeline onto the current state (same run, same `run_published_at`), the event is applied and one `STATE_REBUILT` snapshot is published. The late predecessor is then a duplicate and is ACKed |
| other run, `published_at` older than the state's run | stale straggler: ignored, ACK |
| other run or no state, `seq == 0` | new run: seeded from PostgreSQL, previous state replaced |
| other run or no state, `seq > 0` | rebuild (below) |

Sequence checks are the primary guard; the consumer's idempotency store is a secondary one.
Unknown event types and `SECTOR_COMPLETED` only advance the sequence (warning / debug log).
A lost compare-and-set (`StateConflictError`, another worker wrote first) is retried in process
up to 3 times, since the re-read event is usually a duplicate or the next one. Other processing
errors (database or Redis failures, an impossible rebuild) are left to the consumer's
pending/reclaim retry. A failed, dead-lettered or trimmed raw event therefore does not stall
later events: they wait at most `RACE_STATE_GAP_WAIT_MS`, then rebuild across the hole.

### Rebuild and recovery

When the state is missing (Redis lost data, TTL expired, a worker joined mid-run, a newer run's
first event was missed), the processor loads the stored timeline events `0 .. seq-1` from
PostgreSQL, reduces them from the seed, applies the received event and publishes **one**
full-snapshot `STATE_REBUILT` event. The final state equals that of an uninterrupted run.
The rebuild fails loudly (`RaceStateRebuildError`, retried) if the stored timeline cannot
supply those events; regenerating a timeline while a replay of that session is running breaks
rebuilds of that replay. A stored document with an unreadable body or another
`schema_version` is treated as missing and rebuilt.

### PostgreSQL snapshots

Table `race_state_snapshots` (migration `005`): replay, run, session, `sequence`,
`race_time_ms`, `current_lap`, `trigger`, `state_schema_version`, `payload` (JSONB), unique on
`(replay_id, run_id, sequence)` so a repeated write is a no-op. Triggers are deterministic
(state fields, not wall clock): `INITIAL` after `RACE_STARTED`, `PERIODIC` when the leader's
completed laps cross a multiple of `RACE_STATE_SNAPSHOT_EVERY_LAPS`, `FINAL` at completion,
and `REBUILT` after a rebuild. Never per event. Order: reduce, write snapshot, commit Redis,
ACK. A failed initial/periodic/rebuild snapshot is logged and skipped (the hot state is
rebuildable); a failed `FINAL` snapshot raises, nothing is committed to Redis, and the event is
retried.

### Gaps and intervals

`gap_basis` is `LAP_END`: when a driver completes lap N, `gap_to_leader_ms` is that crossing
minus the first crossing of lap N by any driver, and `interval_to_ahead_ms` is the crossing
minus that of the driver reported one position ahead on lap N. A bounded window of recent
crossings is kept; both values are `null` whenever they cannot be derived (outside the
window, no position, car ahead not on the same lap yet). Lapped cars have a gap relative to
the same lap number plus `laps_behind_leader`. Values update only when the driver crosses the
line. Crossing times can come from fallbacks (`completion_time_source`), which lowers accuracy.

### Endpoints

```bash
curl http://127.0.0.1:8000/api/v1/replays/$REPLAY_ID/state
curl http://127.0.0.1:8000/api/v1/replays/$REPLAY_ID/drivers/NOR     # abbreviation or driver UUID
```

404 unknown replay, unknown driver, or no state available (nothing processed yet or expired:
check the worker is running); 409 replay not started; 503 Redis (or database) unavailable.
With Redis down the endpoint does not fall back to an older snapshot, because presenting it
as current would mislead. If Redis simply has no state, the latest snapshot is served with
`source: "snapshot"`.

### Scaling

Run **one** worker. Several workers converge to the same state (compare-and-set on the state
document, in-process conflict retries, bounded gap wait then rebuild), but they split
consecutive sequences, so they repeatedly wait, conflict or rebuild the same ranges and
duplicate work; nothing partitions replays between them (that would need per-replay stream
partitioning, not implemented).

### Known limitations

- Gaps and intervals are `LAP_END` values and often `null` (see above); tyre information is
  only as complete as the lap/pit events carrying it (compound, age or stint can be `null`).
- No sector data.
- Retirements are not visible mid-race (there is no retirement event); non-finishers are only
  marked `DID_NOT_FINISH` when the race is finalized, and a car that finished its last lap
  before the leader's chequered flag cannot be told apart from one that retired.
- Positions have lap-end granularity, so two drivers may briefly share a position number.
- After a restart the previous run's state stays visible until the new run's first event is
  processed.
- A missing, failed or dead-lettered raw event no longer stalls the state: it is recovered by a
  rebuild from the stored timeline after the bounded wait (so the state can lag by up to
  `RACE_STATE_GAP_WAIT_MS` once). That recovery trusts the stored timeline.
- Regenerating a timeline during an active replay still breaks rebuild (and completion
  detection, which uses the event count loaded when the run began): a rebuild that finds
  non-contiguous or missing stored events fails with `RaceStateRebuildError` and is retried
  until the timeline is consistent again or the delivery limit dead-letters the event. The same
  happens when the session data needed for seeding is gone.
- Seeking is not supported.

## Event detection

The detection engine turns the stream of race state events into structured, evidence-backed
detections (battles, overtakes, pace changes, personal bests, new stints). It is deterministic
and statistical (no ML), explainable (every detection carries the numbers behind it), and it
never changes race state, controls replays or touches FastF1.

```
race.state.events -> StreamConsumer (group race-event-detectors) -> DetectionProcessor
                       |-> engine -> registered detectors (pure)
                       |-> Redis  race:{replay_id}:detection   (context: mirror + detector memory)
                       |-> race.detected.events                (detected events)
                       `-> PostgreSQL detected_events          (durable record, API)
```

```bash
uv run python -m app.detection.worker     # needs the race state worker to be running too
```

### Why it reads state events

The race state processor runs concurrently with the detectors. Reading "the current state"
while handling a raw event would usually return a state that is already ahead of that event, so
results would depend on timing. A state event pairs the source raw event (`source_event_id`,
`source_event_type`, sequence, driver, race time) with the state exactly after it, so the same
stream always yields the same detections. State events are only published for non-empty
deltas, so sequence holes are normal.

### Detection context and idempotency

Per replay one JSON document is kept in Redis (`race:{replay_id}:detection`, TTL
`DETECTION_CONTEXT_TTL_SECONDS`): the run, `last_sequence`, a mirror of the public race state
(deltas merged, snapshots replace it), a bounded track status change history and one memory per
detector (namespaced by detector name and version).

| Situation | Behavior |
|-----------|----------|
| same run, `seq <= last_sequence` | duplicate: nothing detected or published, history not double counted |
| same run, higher sequence | mirror advanced, detectors run |
| other run or no context, snapshot event | context reset from the full state, fresh detector memory |
| other run or no context, `STATE_UPDATED` | mirror bootstrapped from the race state store (same run, not behind the event), detector memory starts empty (windows refill); otherwise skipped with a warning |
| other run, published before the current run began | stale straggler, ignored |
| `STATE_REBUILT` | detectors get `rebuilt=true`; the overtake detector does not confirm swaps from it |

Order per event: detect, persist to PostgreSQL (insert-ignore on the deterministic primary key),
then commit context and `XADD` of the detected events in one `WATCH`/`MULTI`/`EXEC`
transaction (conflicts are retried a bounded number of times). A retry after a Redis failure
therefore never duplicates rows, and the consumer ACKs only after all of this succeeded.
One failing detector is logged and skipped; it does not block the others.

### Detected event contract (`schema_version` 1)

Published on `race.detected.events` in the usual stream envelope (`event_type` = detected type,
`sequence` = source sequence, `payload` = the document below) and stored in `detected_events`.

| Field | Meaning |
|-------|---------|
| `detected_event_id` | deterministic `uuid5(run_id, detector, type, source sequence, drivers, logical key)` |
| `event_type` | `BATTLE_FORMING`, `RAPIDLY_CLOSING`, `OVERTAKE`, `PACE_DEGRADATION`, `PACE_ANOMALY`, `PERSONAL_BEST`, `NEW_STINT` |
| `replay_id`, `run_id`, `session_id`, `race_id` | identity |
| `race_time_ms`, `lap_number` | when (race time of the source event; lap of the subject driver) |
| `primary_driver_*`, `secondary_driver_*` | id and abbreviation (secondary: defender / overtaken driver) |
| `severity` | `LOW`/`MEDIUM`/`HIGH` only where documented bands exist (degradation, anomaly), else `null` |
| `confidence` | `null`: no detector derives one objectively |
| `evidence` | machine-readable facts, numbers in milliseconds |
| `source_event_ids`, `source_sequence` | the raw event the detection was confirmed by |
| `detector_name`, `detector_version`, `detected_at` | provenance (`detected_at` is wall clock and excluded from determinism comparisons) |

### Detectors and rules

Shared pace rules: a lap is *clean* when it has a time, is not deleted, not lap 1, not an in or
out lap, its whole time window (checked against the status change history, not just the status at
lap end) saw no safety car, VSC, red flag or (configurable) yellow, it is not the restart lap
after such a period, and its track status is known. Detectors keep their own bounded per-driver
history keyed by stint; a new stint empties it.

| Type | Rule |
|------|------|
| `BATTLE_FORMING` | On the attacker's lap, defender = car one position ahead, gap = `interval_to_ahead_ms` (`LAP_END`). A full window (`DETECTION_BATTLE_WINDOW_LAPS`, default 4) of consecutive-lap gaps for the same pair, current gap <= battle gap, closing >= min rate per lap, at most one non-closing step, no pit in/out laps or pit lane, green track. Evidence: `gap_ms`, `gap_history`, `closing_rate_ms_per_lap`, `observed_laps`, `window_laps`, positions, `threshold_ms`, `basis`. |
| `RAPIDLY_CLOSING` | Same trend further away: gap above the battle gap and up to `DETECTION_RAPID_CLOSING_MAX_GAP_MS`, closing >= `DETECTION_RAPID_CLOSING_MIN_RATE_MS` per lap. Once per approach, never while the pair is already in a battle. Same evidence plus `max_gap_ms`, `min_closing_rate_ms`. |
| `OVERTAKE` | Adjacent swap confirmed when the later of the two drivers completes lap N (lap-end positions); both running, pit in/out flags on laps N and N-1 known and false (missing pit data is never read as on track), not in the pit lane, lap N green. Otherwise (pit cycles, unknown pit data, safety car, retirements, multi-position or ambiguous changes, lap 1, rebuilt state) nothing is emitted. Evidence: previous/new positions of both, `lap`, `classification` (`ON_TRACK_LIKELY`), `basis` (`LAP_END`), `confirmed_by`. |
| `PACE_DEGRADATION` | Same stint: `median(recent M clean laps) - median(baseline N clean laps before them) >= threshold` and at least M-1 recent laps slower than the baseline median. Once per stint, again only if the delta grew by a step. Severity by delta (1.0 s MEDIUM, 2.0 s HIGH). Evidence: medians, `delta_ms`, `slower_recent_laps`, windows, lap lists, `compound`, `tyre_age_laps`, `stint_number`. It states the slowdown, not its cause. |
| `PACE_ANOMALY` | Clean lap, full baseline of K clean laps in the stint: score `(actual - expected) / (1.4826 * max(MAD, floor))` >= min score and deviation >= min ms (slow side only). Anomalous laps stay out of the baseline; a run of K anomalous laps becomes the new baseline; a run is reported once. Severity by deviation (4 s MEDIUM, 10 s HIGH). Evidence: `lap_time_ms`, `expected_ms`, `deviation_ms`, `robust_score`, `mad_ms`, `mad_floor_ms`, `baseline_laps`, tyre fields. Sector anomalies are not implemented: the race state has no sector data. |
| `PERSONAL_BEST` | Eligible lap (time, not deleted, not lap 1, no pit lap, not neutralised) faster than the driver's best eligible lap; the first only sets the benchmark. An event needs an improvement of at least `DETECTION_PB_MIN_IMPROVEMENT_MS` over the *last reported* best (the true best is always tracked, so many tiny fuel-burn gains add up instead of emitting every lap). Evidence: `lap_time_ms`, `previous_best_ms` (last reported), `previous_true_best_ms`, `previous_best_lap`, `improvement_ms`, `min_improvement_ms`, tyre fields. Distinct from the race-wide fastest lap. |
| `NEW_STINT` | On `PIT_EXIT`, or a higher stint number seen on `LAP_COMPLETED` when the pit data is missing; once per driver and stint. The starting stint is not "new". Evidence: `stint_number`, `compound` (`null` if unknown), `previous_stint_number`, `previous_compound`, `compound_changed`, `starting_lap`, `tyre_age_laps`, `pit_lane_duration_ms`, `pit_stop_count`, `source_event_type`. |

Strategy detectors (for example `UNDERCUT_ATTEMPT`) are not implemented.

### Suppression

An active battle is not re-reported until the gap exceeds the release gap (hysteresis) or the pair
is broken (defender change, missing lap, pit lap, neutralisation, unknown gap) **and**
`DETECTION_BATTLE_COOLDOWN_LAPS` laps have passed. `RAPIDLY_CLOSING` is once per approach and
also respects the cooldown.

### Configuration

| Variable | Default |
|----------|---------|
| `DETECTION_KEY_PREFIX` | `race` |
| `DETECTION_CONTEXT_TTL_SECONDS` | `604800` |
| `DETECTION_DISABLED_DETECTORS` | empty (comma separated names: `battle`, `overtake`, `pace_degradation`, `pace_anomaly`, `personal_best`, `stint`) |
| `DETECTION_EXCLUDE_YELLOW` | `true` |
| `DETECTION_BATTLE_GAP_MS` / `_RELEASE_GAP_MS` | `1000` / `1500` |
| `DETECTION_BATTLE_WINDOW_LAPS` | `4` |
| `DETECTION_BATTLE_MIN_CLOSING_RATE_MS` | `200` per lap |
| `DETECTION_BATTLE_COOLDOWN_LAPS` | `3` |
| `DETECTION_RAPID_CLOSING_MAX_GAP_MS` / `_MIN_RATE_MS` | `5000` / `500` per lap |
| `DETECTION_PB_MIN_IMPROVEMENT_MS` | `300` |
| `DETECTION_DEGRADATION_BASELINE_LAPS` / `_RECENT_LAPS` | `5` / `5` |
| `DETECTION_DEGRADATION_THRESHOLD_MS` / `_REEMIT_STEP_MS` | `500` / `500` |
| `DETECTION_ANOMALY_BASELINE_LAPS` | `5` |
| `DETECTION_ANOMALY_MIN_SCORE` / `_MIN_DEVIATION_MS` | `4.0` / `2000` |

Invalid combinations (battle release gap below the battle gap, rapid-closing max gap not above
the battle gap, windows shorter than 2 laps) fail at startup with a `ValueError`.

### Endpoint

```bash
curl "http://127.0.0.1:8000/api/v1/replays/$REPLAY_ID/events?event_type=OVERTAKE&driver=NOR&lap_from=10&limit=50"
```

Filters: `event_type` (repeatable), `driver` (abbreviation or UUID, either side), `lap_from`,
`lap_to`, `run_id` (default: the run of the most recent detection), `limit` (1-1000, default 100),
`offset`. Ordered by source sequence, event type, id. 404 unknown replay; 503 database unavailable.
Table `detected_events` (migration `006`), primary key = the deterministic id.

### Recovery limitations and false positives

- Without a context (Redis loss, TTL, a worker joining mid-run) the mirror is bootstrapped from the
  race state, but detector memories start empty: battles, pace windows and personal bests refill
  and may be missed or reported late. A `STATE_REBUILT` skips overtake confirmation.
- Positions and gaps are lap-end values: an overtake is only known to have happened during a lap.
  Battles use lap-end gaps, so a pass followed by a re-pass inside one lap is invisible.
- Pace rules cannot separate tyre wear from fuel, traffic or pace management; lapped traffic is
  not modelled. Retirements are not visible mid-race, so a retiring car is simply never confirmed.
- The endpoint's default run is the run of the most recent detection, so right after a restart it
  may still show the previous run until the new run detects something. Pass `run_id` to be explicit.
- A detector that raises is logged and skipped for that event; the context still advances, so that
  detector's detections for the event are lost (and its memory is not updated for it).
- If the replay was deleted, detections cannot be stored (foreign key); the state event is logged
  with a warning, ACKed and not published. Other database errors are retried.
- Run a single detection worker.

### Adding a detector

1. Create `app/detection/detectors/<name>.py` with a `Detector` subclass: `name`, `version`,
   `triggers` (raw event types), a pydantic `memory_model` and a pure
   `evaluate(input, memory) -> (drafts, new_memory)` (no I/O, no wall clock).
2. Add one line to `build_default_registry` in `app/detection/registry.py`.
3. Add a value to `DetectedEventType` if it emits a new type, and thresholds to `DetectionConfig`.

## HTTP API

Everything the frontend needs is under one versioned prefix, `/api/v1` (defined once in
`app/api/router.py`). `/health` and `/ready` stay unversioned. The frontend never needs to
know about FastF1, PostgreSQL or Redis: routes are thin and call application services.
Interactive documentation: `/docs` (Swagger UI), `/redoc`, schema at `/openapi.json`
(endpoints are grouped by the tags Races, Replays, Race State, Events, Timing, Data
Management and Health).

### Endpoints

Races (imported historical data, read from PostgreSQL):

| Method and path | Purpose |
|-----------------|---------|
| `GET /api/v1/seasons` | Seasons with imported races and their race counts |
| `GET /api/v1/races` | Imported races; filters `season`, `round`, `event` (name/location text), `session_type` |
| `GET /api/v1/races/{race_id}` | Race metadata with its imported sessions |
| `GET /api/v1/races/{race_id}/drivers` | Participating drivers |
| `GET /api/v1/races/{race_id}/laps` | Historical laps, paginated (`limit` 1-500, default 100; `offset`; `driver`; `lap_from`; `lap_to`) |
| `GET /api/v1/sessions/{session_id}` | Session metadata |
| `GET /api/v1/sessions/{session_id}/laps`, `/stints`, `/track-status` | Laps (paginated as above), tyre stints, track status periods |

Replays (lifecycle is owned by the replay service, see "Race replay"):

| Method and path | Purpose |
|-----------------|---------|
| `POST /api/v1/replays` | Create (`{"session_id", "playback_speed"}`), 201 |
| `GET /api/v1/replays/{replay_id}` | Status, virtual clock, lap progress |
| `POST /api/v1/replays/{replay_id}/start`, `/pause`, `/resume`, `/stop`, `/restart` | Playback control |
| `PATCH /api/v1/replays/{replay_id}/speed` | `{"playback_speed": 5}` (supported speeds only) |
| `WS /api/v1/replays/{replay_id}/stream` | Live updates, see "WebSocket API" |

Race state, events and timing (scoped to a replay and to what it has released so far):

| Method and path | Purpose |
|-----------------|---------|
| `GET /api/v1/replays/{replay_id}/state` | Full race state snapshot (see "Race state") |
| `GET /api/v1/replays/{replay_id}/drivers/{driver}` | One driver's state; `driver` is an abbreviation or UUID |
| `GET /api/v1/replays/{replay_id}/events` | Detected events (see "Event detection") |
| `GET /api/v1/replays/{replay_id}/events/{event_id}` | One detected event |
| `GET /api/v1/replays/{replay_id}/timing` | Per-driver lap series for charts; `driver` (repeatable), `lap_from`, `lap_to` |
| `GET /api/v1/replays/{replay_id}/drivers/{driver}/timing` | Lap series of one driver |

Data management (operator endpoints, not needed by a viewer): `POST /api/v1/races/import`,
`POST` / `GET /api/v1/sessions/{session_id}/timeline` and `GET .../timeline/summary`.

### Timing series

`GET .../timing` returns, per driver (ordered by abbreviation), the laps the replay has
already released (`sequence <= replay.current_sequence`), so a chart never contains laps from
the replay's future. Each point has `lap_number`, `race_time_ms`, `lap_time_ms`, `position`
(lap-end classification as reported by the source), `gap_to_leader_ms`, tyre fields
(`compound`, `tyre_age_laps`, `stint_number`), `is_pit_in_lap`, `is_pit_out_lap`,
`is_deleted`, `track_status` and `sectors` (empty when the source has none; never invented).
`gap_to_leader_ms` has the race state's definition: the driver's crossing time of the lap
minus the earliest crossing of that lap by any driver. The last point of a driver therefore
equals the `gap_to_leader_ms` in `GET .../state`. The series is derived from the persisted
timeline, so unlike the state's bounded crossing window it covers every released lap. A replay
that has not started returns every driver with an empty `points` list.

### Error contract

Every non-2xx response has the same body (also used by the WebSocket `ERROR` message):

```json
{"code": "REPLAY_NOT_FOUND", "message": "Replay 0b0f... not found", "details": null}
```

`code` is stable and machine-readable (branch on it, not on `message`); `details` is an
object or `null`. Stack traces and database or Redis errors are never returned.

| Status | Codes |
|--------|-------|
| 404 | `RACE_NOT_FOUND`, `SESSION_NOT_FOUND`, `DRIVER_NOT_FOUND`, `REPLAY_NOT_FOUND`, `RACE_STATE_UNAVAILABLE`, `DETECTED_EVENT_NOT_FOUND`, `TIMELINE_NOT_GENERATED`, `HISTORICAL_EVENT_NOT_FOUND`, `HISTORICAL_SESSION_NOT_FOUND`, `NOT_FOUND` (unknown path) |
| 405 | `METHOD_NOT_ALLOWED` |
| 409 | `INVALID_REPLAY_TRANSITION` (`details`: `command`, `current_status`), `REPLAY_NOT_STARTED`, `REPLAY_TIMELINE_UNAVAILABLE`, `TIMELINE_CONFLICT` |
| 422 | `VALIDATION_ERROR` (`details.errors`: list of validation problems), `INVALID_PLAYBACK_SPEED`, `UNSUPPORTED_SESSION_TYPE`, `TIMELINE_BUILD_FAILED`, `TIMELINE_INVALID`, `HISTORICAL_DATA_INVALID` |
| 500 | `INTERNAL_ERROR`, `IMPORT_PERSISTENCE_FAILED` |
| 502 | `HISTORICAL_DATA_UNAVAILABLE` |
| 503 | `DATABASE_UNAVAILABLE`, `REDIS_UNAVAILABLE`, `RACE_STATE_STORE_UNAVAILABLE` |

Success codes: 200, and 201 for `POST /replays`, `POST /races/import` and a newly generated
timeline. Replay commands that are not valid in the current status (for example `start` on a
running replay, `pause` on a stopped one, `resume` on a completed one) are 409, never silently
accepted.

### Pagination and filters

List endpoints that can be large take `limit` / `offset` and answer with
`{"items": [...], "total", "limit", "offset"}` (laps: `limit` 1-500, default 100; detected events
and timeline events: `limit` 1-1000). Drivers are filtered by abbreviation (case-insensitive) or
UUID. `lap_from` / `lap_to` are inclusive, and `lap_from > lap_to` is a 422. Invalid values are a
422 `VALIDATION_ERROR`.

### CORS

Browsers may call the API from the origins in `CORS_ALLOWED_ORIGINS` (comma-separated). The
default allows local development servers only (`http://localhost:5173`,
`http://127.0.0.1:5173`, `http://localhost:3000`); set the real frontend origin in every deployed
environment. Allowed methods: `GET`, `POST`, `PATCH`, `OPTIONS`; credentials are not allowed.
A disallowed origin receives no `Access-Control-Allow-Origin` header. There is no
authentication; put the API behind a gateway if it must not be public.

## WebSocket API

`WS /api/v1/replays/{replay_id}/stream` pushes a replay's progress, race state changes and
detected events to a browser. It is a delivery layer: it recomputes nothing. The race state
comes from the Race State Engine, detections from the Detection Engine, lifecycle and clock from
the replay service. Messages are JSON text frames.

```js
const ws = new WebSocket(`ws://127.0.0.1:8000/api/v1/replays/${replayId}/stream`);
ws.onmessage = (e) => handle(JSON.parse(e.data));
```

### Envelope

Every server message has the same envelope:

| Field | Meaning |
|-------|---------|
| `type` | Message type (below) |
| `schema_version` | Message contract version (currently `1`; breaking changes bump it) |
| `replay_id` | The replay |
| `run_id` | Replay run (changes on every start or restart); `null` for lifecycle messages |
| `sequence` | Timeline sequence the message derives from; `null` for lifecycle messages |
| `race_time_ms`, `lap_number` | Race clock position and leader lap of the message |
| `emitted_at` | Server time the message was created (ISO 8601, UTC) |
| `payload` | Type-specific object |

### Message types

| Type | Payload |
|------|---------|
| `SNAPSHOT` | `replay` (same as `GET /replays/{id}`), `state` (same as `GET /replays/{id}/state`, or `null`), `state_error` (REST error body explaining a `null` state, for example `REPLAY_NOT_STARTED`). Envelope `run_id` / `sequence` are the state's `run_id` / `last_sequence`. |
| `PONG` | empty, answer to `PING` |
| `ERROR` | `{code, message, details}`: the REST error body (plus `UNSUPPORTED_CLIENT_MESSAGE` and `CLIENT_TOO_SLOW`) |
| `REPLAY_STATUS` | `replay`: the current replay status object. Sent after every lifecycle change (start, pause, resume, stop, restart, speed change, completion). |
| `REPLAY_CLOCK` | `status`, `current_race_time_ms`, `current_lap`, `total_laps`, `playback_speed`, `emitted_event_count`, `total_events` |
| `REPLAY_COMPLETED` | `replay`; sent right after the final `REPLAY_STATUS` |
| `RACE_STATE_SNAPSHOT` | `reason` (`STATE_INITIALIZED`, `STATE_REBUILT`, `STATE_COMPLETED`), `state` (full state, as in REST). Sent when the state engine publishes a full state (start, rebuild, completion). |
| `RACE_STATE_UPDATE` | `kinds`, `race` (changed race-level fields such as `current_lap`, `phase`, `leader_*`, `fastest_lap`, `track_status`), `last_sequence` |
| `DRIVER_UPDATE` | `driver`: the driver's full current state as in REST, without `recent_laps` |
| `LAP_COMPLETED` | `driver_id`, `abbreviation`, `lap` (a `recent_laps` entry), `laps_completed` |
| `POSITION_CHANGED` | `driver_id`, `abbreviation`, `position`, `previous_position` |
| `PIT_STATUS_CHANGED` | `driver_id`, `abbreviation`, `pit_status`, `pit_stop_count`, `last_pit_lane_duration_ms` |
| `TRACK_STATUS_CHANGED` | `track_status` |
| `DETECTED_EVENT` | `event`: identical to one item of `GET /replays/{id}/events` |

### Initial snapshot and buffering

On connect the server sends exactly one `SNAPSHOT` first, built by the same services as the
REST endpoints (`state` equals `GET .../state`; before the replay is started `state` is `null`
and `state_error.code` is `REPLAY_NOT_STARTED`). While the snapshot is being built the
connection is registered and messages published in the meantime are buffered. After the
snapshot is sent, buffered and live messages follow, except state messages (race state, driver,
lap, position, pit and track status) of the same `run_id` with `sequence <= snapshot.sequence`,
which the snapshot already contains. There is no gap and no double application. Messages of
other runs, lifecycle messages and detections are never filtered.

### Delta model

After the snapshot a client keeps its state current by applying deltas:

- `RACE_STATE_UPDATE.race`: shallow-merge into the race-level fields of the state.
- `DRIVER_UPDATE.driver`: merge into the driver entry with the same `driver_id` (keep its own
  `recent_laps`), then re-sort by `position`.
- `LAP_COMPLETED.lap`: upsert into that driver's `recent_laps` by `lap_number`. Upsert rather than
  append: the event that completes the race arrives both inside a `RACE_STATE_SNAPSHOT` and as
  `LAP_COMPLETED`.
- `RACE_STATE_SNAPSHOT.state`: replace the whole state.
- `POSITION_CHANGED`, `PIT_STATUS_CHANGED` and `TRACK_STATUS_CHANGED` are notifications about changes
  already contained in the updates (use them for animations, toasts or sound).

### Ordering

Within a run, `sequence` never decreases. One state event produces several messages that share its
`sequence` in a fixed order: `RACE_STATE_SNAPSHOT` (full-state events only), `RACE_STATE_UPDATE`,
`DRIVER_UPDATE` (one per changed driver), `LAP_COMPLETED`, `POSITION_CHANGED`, `PIT_STATUS_CHANGED`,
`TRACK_STATUS_CHANGED`: state first, notifications after. A detection arrives after the state event
that triggered it (the Detection Engine consumes the state stream) and carries that event's
`sequence`. `REPLAY_STATUS`, `REPLAY_CLOCK` and `REPLAY_COMPLETED` are lifecycle messages from the replay service,
not from the streams: they are **not ordered** relative to state or detection messages (a
`REPLAY_COMPLETED` can arrive before the last state updates). Do not derive race state from them:
treat the state as final on a `RACE_STATE_SNAPSHOT` with `reason` `STATE_COMPLETED` (state `phase`
`COMPLETED`), not on `REPLAY_COMPLETED`. Run-scoped messages (all state messages and `DETECTED_EVENT`) are only delivered if their `run_id` is the
replay's *current* run according to the replay service in the API process, checked when a message is queued for the client (and again when the snapshot is sent), so
entries of an older run that the stream tail had not read yet (after a restart, at connect or resync) are
dropped, and a new run is adopted however it is announced (`STATE_INITIALIZED`, `STATE_REBUILT` or a plain
delta). If the replay has not run in this process (for example a state served from a PostgreSQL snapshot),
the snapshot's own run is followed. No wall clocks are compared. A restart begins a new `run_id` from sequence 0: discard the state of an older run when you see a
new `run_id` (send `RESYNC` to get a fresh snapshot).

### Clock cadence

`REPLAY_CLOCK` is sent once per `WS_CLOCK_INTERVAL_MS` (default 1000 ms) for every replay in
`RUNNING` status that has subscribers. It is for smooth progress display and may be dropped for a slow
client (see below). Paused, stopped and completed replays send no clock messages.

### Completion

When a replay completes the server sends `REPLAY_STATUS` (status `COMPLETED`) and `REPLAY_COMPLETED`;
the socket stays open. The client can keep the connection, send `RESYNC`, or close it.

### Client messages

JSON text frames with a `type` (case-insensitive):

- `{"type": "PING"}` is answered with `PONG` (application-level keep-alive).
- `{"type": "RESYNC"}` is answered with a fresh `SNAPSHOT`, after which delivery resumes as after
  connect (use it after detecting a gap or a `run_id` change).

Anything else, including invalid JSON and binary frames, is answered with `ERROR` `UNSUPPORTED_CLIENT_MESSAGE`; the
connection stays open. Replies to client messages are bounded (a flood of `PING`s is coalesced and
excess replies are dropped).

### Close codes

| Code | Meaning |
|------|---------|
| 1008 | `replay_id` is not a UUID (rejected before the connection is accepted) |
| 4404 | Unknown replay; an `ERROR` `REPLAY_NOT_FOUND` is sent first |
| 4408 | Client too slow (send queue overflow or send timeout); an `ERROR` `CLIENT_TOO_SLOW` is sent first when possible |
| 1013 | Try again later: the snapshot could not be built (for example the database is unavailable); an `ERROR` is sent first |
| 1011 | Unexpected server error while building the snapshot; an `ERROR` `INTERNAL_ERROR` is sent first |
| 1001 | Server shutting down |

### Slow clients

Each client has its own bounded queue (`WS_CLIENT_QUEUE_SIZE` messages) drained by its own task, so a
slow client never delays the replay, the stream tail or other clients. When the queue is full,
`REPLAY_CLOCK` messages are dropped (the next one supersedes them) and queued clock messages are evicted
to make room for anything else. If a state, lifecycle or detection message still does not fit, the client
is sent `ERROR` `CLIENT_TOO_SLOW` and disconnected with 4408 instead of silently missing a state
transition. A single send that takes longer than `WS_SEND_TIMEOUT_SECONDS` also disconnects the client.
Failing sockets are removed. Replays run the same with no clients, one client or many.

### Reconnection strategy

Do not try to resume from a message id. On any disconnect (including 4408 and 1013), reconnect with
backoff and treat the new `SNAPSHOT` as authoritative: replace local state with it. Compare `run_id`
and `sequence` with what you hold: the same `run_id` and a higher `sequence` means you missed updates,
a different `run_id` means the replay was restarted. Messages published while disconnected are not
replayed to the browser.

### Scaling assumptions

- Every API process runs one gateway that tails `race.state.events` and `race.detected.events` with a
  plain `XREAD` from the stream tail at startup: broadcast, no consumer group, no acknowledgements. Every
  process therefore sees every message and serves its own clients; this is separate from the consumer
  groups of the Race State and Detection workers.
- A replay runs in the process that created or started it. `REPLAY_STATUS`, `REPLAY_CLOCK` and
  `REPLAY_COMPLETED` are produced from that process's in-memory replay state only, so WebSocket clients
  must reach that same process: run a single API process (the replay service is single-process by
  design). Stream-derived messages would work on any process.
- Connection state is in memory; restarting the API drops connections and the replays running in it.

### Frontend integration notes

1. List races (`GET /races`), create a replay (`POST /replays`), open the WebSocket, then call `start`.
   Opening the socket first lets you receive the very first state.
2. Render from the `SNAPSHOT`, apply deltas as above, use `REPLAY_CLOCK` for the progress display.
3. Fetch history with REST when needed (`/events` for the events list on load, `/timing` for charts);
   `DETECTED_EVENT` items have the same shape, so one type serves both.
4. Branch on `code`, not `message`. Ignore unknown message types and unknown fields: new types and
   optional fields are non-breaking.
5. Durations are milliseconds (`*_ms`); timestamps are ISO 8601; no response contains NaN or Infinity.

### Known limitations

- No authentication, authorization or rate limiting. CORS does not apply to WebSockets and the
  WebSocket route does not check the `Origin` header, so any page can open a stream until auth exists.
- No message replay after a reconnect; the snapshot is the recovery mechanism.
- Lifecycle and clock messages require the single-process deployment described above.
- Messages published while a gateway process was down or before it started are not delivered.
- State and detection updates need the Race State and Detection workers to be running; without them the
  socket still delivers lifecycle and clock messages.

## Tests

Default suite (no FastF1 downloads):

```bash
uv run pytest
```

Streaming and race state tests use fakeredis. To also run them against a real Redis, point
`REDIS_TEST_URL` at a disposable instance (never the application's Redis; use a
separate database number). Tests use uniquely named streams and delete them
afterwards; they also delete every `stream:processed:*` idempotency key in that
database, so it must hold nothing else you care about.

```bash
REDIS_TEST_URL=redis://localhost:6379/15 uv run pytest tests/streaming tests/race_state
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
| `STREAM_STATE_EVENTS` | no | Race state events stream (default `race.state.events`) |
| `STREAM_DETECTED_EVENTS` | no | Detected events stream (default `race.detected.events`) |
| `STREAM_DEAD_LETTER` | no | Dead-letter stream (default `race.dead_letter.events`) |
| `STREAM_MAXLEN` | no | Approximate raw stream retention, `0` = unbounded (default `100000`) |
| `STREAM_DEAD_LETTER_MAXLEN` | no | Approximate dead-letter retention (default `10000`) |
| `STREAM_READ_COUNT` | no | Messages per consumer read (default `100`) |
| `STREAM_BLOCK_MS` | no | Consumer blocking read timeout in ms (default `5000`) |
| `STREAM_MAX_DELIVERIES` | no | Deliveries before dead-lettering (default `5`) |
| `STREAM_RECLAIM_IDLE_MS` | no | Idle time before a pending message is reclaimed (default `30000`) |
| `STREAM_PUBLISH_ATTEMPTS` | no | Publish attempts on connection errors (default `3`) |
| `STREAM_IDEMPOTENCY_TTL_SECONDS` | no | Processed-event marker TTL (default `86400`) |
| `RACE_STATE_LAP_HISTORY` | no | Laps kept per driver in `recent_laps` and in the gap crossing window (default `10`) |
| `RACE_STATE_SNAPSHOT_EVERY_LAPS` | no | PostgreSQL snapshot every N leader laps, `0` disables (default `10`) |
| `RACE_STATE_TTL_SECONDS` | no | TTL of the Redis state document, refreshed on every write (default `604800`) |
| `RACE_STATE_GAP_WAIT_MS` | no | Max in-process wait for a missing predecessor before rebuilding from the timeline (default `1500`) |
| `RACE_STATE_KEY_PREFIX` | no | Redis key prefix of the state document (default `race`) |
| `CORS_ALLOWED_ORIGINS` | no | Comma-separated browser origins allowed by CORS (default: local dev servers `http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000`) |
| `WS_CLIENT_QUEUE_SIZE` | no | Outgoing messages buffered per WebSocket client before it is disconnected as too slow (default `512`, min `8`) |
| `WS_SEND_TIMEOUT_SECONDS` | no | A single WebSocket send slower than this disconnects the client (default `5`) |
| `WS_CLOCK_INTERVAL_MS` | no | `REPLAY_CLOCK` interval for running replays (default `1000`, min `100`) |
| `WS_STREAM_BLOCK_MS` | no | Blocking read timeout of the gateway's stream tail (default `1000`) |
| `DETECTION_*` | no | Detection thresholds, see "Event detection > Configuration" |

Copy `.env.example` for local values. Do not commit `.env`.
