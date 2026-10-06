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

    # Race State Engine (see app/race_state/config.py).
    race_state_lap_history: int = Field(default=10, ge=1, alias="RACE_STATE_LAP_HISTORY")
    race_state_snapshot_every_laps: int = Field(
        default=10, ge=0, alias="RACE_STATE_SNAPSHOT_EVERY_LAPS"
    )
    race_state_ttl_seconds: int = Field(default=604_800, ge=1, alias="RACE_STATE_TTL_SECONDS")
    race_state_gap_wait_ms: int = Field(default=1_500, ge=0, alias="RACE_STATE_GAP_WAIT_MS")
    race_state_key_prefix: str = Field(default="race", min_length=1, alias="RACE_STATE_KEY_PREFIX")

    # Event Detection Engine (see app/detection/config.py).
    detection_key_prefix: str = Field(default="race", min_length=1, alias="DETECTION_KEY_PREFIX")
    detection_context_ttl_seconds: int = Field(
        default=604_800, ge=1, alias="DETECTION_CONTEXT_TTL_SECONDS"
    )
    detection_disabled_detectors: str = Field(default="", alias="DETECTION_DISABLED_DETECTORS")
    detection_exclude_yellow: bool = Field(default=True, alias="DETECTION_EXCLUDE_YELLOW")
    detection_battle_gap_ms: int = Field(default=1_000, ge=1, alias="DETECTION_BATTLE_GAP_MS")
    detection_battle_release_gap_ms: int = Field(
        default=1_500, ge=1, alias="DETECTION_BATTLE_RELEASE_GAP_MS"
    )
    detection_battle_window_laps: int = Field(default=4, ge=2, alias="DETECTION_BATTLE_WINDOW_LAPS")
    detection_battle_min_closing_rate_ms: int = Field(
        default=200, ge=1, alias="DETECTION_BATTLE_MIN_CLOSING_RATE_MS"
    )
    detection_battle_cooldown_laps: int = Field(
        default=3, ge=0, alias="DETECTION_BATTLE_COOLDOWN_LAPS"
    )
    detection_rapid_closing_max_gap_ms: int = Field(
        default=5_000, ge=1, alias="DETECTION_RAPID_CLOSING_MAX_GAP_MS"
    )
    detection_rapid_closing_min_rate_ms: int = Field(
        default=500, ge=1, alias="DETECTION_RAPID_CLOSING_MIN_RATE_MS"
    )
    detection_pb_min_improvement_ms: int = Field(
        default=300, ge=0, alias="DETECTION_PB_MIN_IMPROVEMENT_MS"
    )
    detection_degradation_baseline_laps: int = Field(
        default=5, ge=2, alias="DETECTION_DEGRADATION_BASELINE_LAPS"
    )
    detection_degradation_recent_laps: int = Field(
        default=5, ge=2, alias="DETECTION_DEGRADATION_RECENT_LAPS"
    )
    detection_degradation_threshold_ms: int = Field(
        default=500, ge=1, alias="DETECTION_DEGRADATION_THRESHOLD_MS"
    )
    detection_degradation_reemit_step_ms: int = Field(
        default=500, ge=1, alias="DETECTION_DEGRADATION_REEMIT_STEP_MS"
    )
    detection_anomaly_baseline_laps: int = Field(
        default=5, ge=3, alias="DETECTION_ANOMALY_BASELINE_LAPS"
    )
    detection_anomaly_min_score: float = Field(
        default=4.0, gt=0, alias="DETECTION_ANOMALY_MIN_SCORE"
    )
    detection_anomaly_min_deviation_ms: int = Field(
        default=2_000, ge=1, alias="DETECTION_ANOMALY_MIN_DEVIATION_MS"
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings. Call ``get_settings.cache_clear()`` in tests."""
    return Settings()
