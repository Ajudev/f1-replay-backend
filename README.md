# F1 Replay Backend

Backend for the F1 Historical Race Replay and Event Detection Engine. It loads
historical Formula 1 race data (later phases), stores normalized domain models in
PostgreSQL, and will replay races through a virtual clock with event detection
exposed via FastAPI.

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

## Tests

```bash
uv run pytest
```

Tests use SQLite and fakes; they do not require running Postgres or Redis.

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

Copy `.env.example` for local values. Do not commit `.env`.
