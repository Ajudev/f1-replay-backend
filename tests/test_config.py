"""Configuration / settings tests."""

import pytest
from pydantic import ValidationError

from app.core.config import Settings, get_settings


def test_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_ENV", "staging")
    monkeypatch.setenv("APP_NAME", "custom-name")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://user:pass@db:5432/f1",
    )
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/2")
    monkeypatch.setenv("API_HOST", "0.0.0.0")
    monkeypatch.setenv("API_PORT", "9000")
    get_settings.cache_clear()

    settings = get_settings()

    assert settings.app_env == "staging"
    assert settings.app_name == "custom-name"
    assert settings.log_level == "DEBUG"
    assert settings.database_url == "postgresql+asyncpg://user:pass@db:5432/f1"
    assert settings.redis_url == "redis://cache:6379/2"
    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 9000


def test_overriding_env_var_after_cache_clear_changes_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://first:first@localhost:5432/first",
    )
    get_settings.cache_clear()
    first = get_settings()
    assert "first" in first.database_url

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://second:second@localhost:5432/second",
    )
    get_settings.cache_clear()
    second = get_settings()
    assert "second" in second.database_url
    assert first.database_url != second.database_url


def test_missing_database_url_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    # Ensure .env file cannot supply the value during this test.
    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    assert any(err["loc"] == ("DATABASE_URL",) for err in errors)
