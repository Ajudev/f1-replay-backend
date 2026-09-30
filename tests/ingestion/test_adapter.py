"""Unit tests for FastF1 identifier selection and SQ→SS fallback."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.domain.enums import SessionType
from app.ingestion.adapter import (
    FastF1SessionLoader,
    fastf1_identifiers_for,
    is_not_found_error,
)
from app.ingestion.errors import EventNotFoundError, SessionLoadError, SessionNotFoundError


def test_identifiers_sprint_qualifying_tries_sq_then_ss() -> None:
    assert fastf1_identifiers_for(SessionType.SPRINT_QUALIFYING) == ("SQ", "SS")
    assert fastf1_identifiers_for(SessionType.RACE) == ("R",)
    assert fastf1_identifiers_for(SessionType.QUALIFYING) == ("Q",)


def test_is_not_found_error_helpers() -> None:
    assert is_not_found_error(SessionNotFoundError("missing"))
    assert is_not_found_error(EventNotFoundError("missing"))
    assert not is_not_found_error(SessionLoadError("network"))


def _empty_session_object() -> SimpleNamespace:
    return SimpleNamespace(
        event={
            "RoundNumber": 5,
            "EventName": "Test GP",
            "OfficialEventName": None,
            "Country": None,
            "Location": None,
            "EventDate": None,
        },
        name="Sprint Shootout",
        date=None,
        results=None,
        laps=None,
        track_status=None,
    )


def test_sq_success_does_not_try_ss(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_get_session(season: int, event: int | str, identifier: str) -> MagicMock:
        calls.append(identifier)
        session = MagicMock()
        session.event = _empty_session_object().event
        session.name = "Sprint Qualifying"
        session.date = None
        session.results = None
        session.laps = None
        session.track_status = None
        session.load = MagicMock()
        return session

    monkeypatch.setattr("app.ingestion.adapter.fastf1.get_session", fake_get_session)
    monkeypatch.setattr("app.ingestion.adapter.fastf1.Cache.enable_cache", lambda *_a, **_k: None)

    loader = FastF1SessionLoader(cache_dir="/tmp/unused-fastf1-test-cache")
    result = loader.load(2024, 1, SessionType.SPRINT_QUALIFYING)

    assert calls == ["SQ"]
    assert result.session_type == SessionType.SPRINT_QUALIFYING
    assert result.event_name == "Test GP"


def test_sq_not_found_retries_ss(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_get_session(season: int, event: int | str, identifier: str) -> MagicMock:
        calls.append(identifier)
        if identifier == "SQ":
            raise ValueError("Session not found for this event")
        session = MagicMock()
        session.event = _empty_session_object().event
        session.name = "Sprint Shootout"
        session.date = None
        session.results = None
        session.laps = None
        session.track_status = None
        session.load = MagicMock()
        return session

    monkeypatch.setattr("app.ingestion.adapter.fastf1.get_session", fake_get_session)
    monkeypatch.setattr("app.ingestion.adapter.fastf1.Cache.enable_cache", lambda *_a, **_k: None)

    loader = FastF1SessionLoader(cache_dir="/tmp/unused-fastf1-test-cache")
    result = loader.load(2023, 1, SessionType.SPRINT_QUALIFYING)

    assert calls == ["SQ", "SS"]
    assert result.session_name == "Sprint Shootout"


def test_sq_load_error_does_not_retry_ss(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_get_session(season: int, event: int | str, identifier: str) -> MagicMock:
        calls.append(identifier)
        session = MagicMock()
        session.event = _empty_session_object().event
        session.name = "Sprint Qualifying"
        session.date = None
        session.results = None
        session.laps = None
        session.track_status = None

        def boom(*, telemetry: bool, weather: bool, messages: bool) -> None:
            raise RuntimeError("connection timed out downloading data")

        session.load = boom
        return session

    monkeypatch.setattr("app.ingestion.adapter.fastf1.get_session", fake_get_session)
    monkeypatch.setattr("app.ingestion.adapter.fastf1.Cache.enable_cache", lambda *_a, **_k: None)

    loader = FastF1SessionLoader(cache_dir="/tmp/unused-fastf1-test-cache")
    with pytest.raises(SessionLoadError):
        loader.load(2024, 1, SessionType.SPRINT_QUALIFYING)

    assert calls == ["SQ"]
