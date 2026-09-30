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


@lru_cache
def get_settings() -> Settings:
    """Return cached settings. Call ``get_settings.cache_clear()`` in tests."""
    return Settings()
