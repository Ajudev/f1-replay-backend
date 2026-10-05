"""Application settings loaded from environment variables."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application configuration.

    DATABASE_URL has no default; missing values fail validation at load time.
    Environment variables override values from the optional ``.env`` file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(alias="APP_ENV")
    app_name: str = Field(alias="APP_NAME")
    log_level: str = Field(alias="LOG_LEVEL")
    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")
    api_host: str = Field(alias="API_HOST")
    api_port: int = Field(alias="API_PORT")
    fastf1_cache_dir: str = Field(default=".fastf1-cache", alias="FASTF1_CACHE_DIR")

    # Redis Streams transport (see app/streaming/config.py for how these are used).
    stream_raw_events: str = Field(default="race.raw.events", alias="STREAM_RAW_EVENTS")
    stream_state_events: str = Field(default="race.state.events", alias="STREAM_STATE_EVENTS")
    stream_detected_events: str = Field(
        default="race.detected.events", alias="STREAM_DETECTED_EVENTS"
    )
    stream_dead_letter: str = Field(default="race.dead_letter.events", alias="STREAM_DEAD_LETTER")
    stream_maxlen: int = Field(default=100_000, ge=0, alias="STREAM_MAXLEN")
    stream_dead_letter_maxlen: int = Field(default=10_000, ge=0, alias="STREAM_DEAD_LETTER_MAXLEN")
    stream_read_count: int = Field(default=100, ge=1, alias="STREAM_READ_COUNT")
    stream_block_ms: int = Field(default=5000, ge=0, alias="STREAM_BLOCK_MS")
    stream_max_deliveries: int = Field(default=5, ge=1, alias="STREAM_MAX_DELIVERIES")
    stream_reclaim_idle_ms: int = Field(default=30_000, ge=0, alias="STREAM_RECLAIM_IDLE_MS")
    stream_publish_attempts: int = Field(default=3, ge=1, alias="STREAM_PUBLISH_ATTEMPTS")
    stream_idempotency_ttl_seconds: int = Field(
        default=86_400, ge=1, alias="STREAM_IDEMPOTENCY_TTL_SECONDS"
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings. Call ``get_settings.cache_clear()`` in tests."""
    return Settings()
