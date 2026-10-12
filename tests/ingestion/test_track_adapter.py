"""FastF1 track data extraction with fake sessions (no download)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from app.domain.enums import SessionType
from app.ingestion.adapter import FastF1SessionLoader


def _results() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "DriverNumber": ["1", "44", "4"],
            "Abbreviation": ["VER", "HAM", "NOR"],
            "TeamColor": ["3671C6", "#27F4D2", "nan-colour"],
        }
    )


def _pos() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "SessionTime": pd.to_timedelta([2.0, 1.0, 3.0, 4.0], unit="s"),
            "X": [20.4, 10.0, np.nan, 40.0],
            "Y": [-2.0, -1.0, -3.0, -4.6],
            "Status": ["OnTrack", "OnTrack", "OnTrack", "OffTrack"],
        }
    )


def _car() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "SessionTime": pd.to_timedelta([1.25, 2.25], unit="s"),
            "Speed": [250.0, 260.0],
            "RPM": [11000.0, np.nan],
            "nGear": [7, 8],
            "Throttle": [100.0, 104.0],
            "Brake": [False, True],
            "DRS": [12, 8],
        }
    )


class _Circuit:
    rotation = 92.0
    corners = pd.DataFrame(
        {
            "Number": [1, 2],
            "Letter": ["", "a"],
            "X": [1.0, np.nan],
            "Y": [2.0, 3.0],
            "Angle": [10.0, 0.0],
        }
    )


CIRCUIT = _Circuit()


def _session(*, circuit: Any = CIRCUIT, car_data: Any = None) -> SimpleNamespace:
    def get_circuit_info() -> Any:
        if isinstance(circuit, Exception):
            raise circuit
        return circuit

    session = SimpleNamespace(
        results=_results(),
        pos_data={"1": _pos()},
        car_data=car_data if car_data is not None else {"1": _car(), "44": _car()},
        get_circuit_info=get_circuit_info,
        loaded_with=None,
    )
    session.load = lambda **kwargs: setattr(session, "loaded_with", kwargs)
    return session


@pytest.fixture
def load(monkeypatch: pytest.MonkeyPatch):
    def run(session: SimpleNamespace):
        monkeypatch.setattr("app.ingestion.adapter.fastf1.get_session", lambda *_a: session)
        monkeypatch.setattr(
            "app.ingestion.adapter.fastf1.Cache.enable_cache", lambda *_a, **_k: None
        )
        return FastF1SessionLoader(cache_dir="/tmp/unused").load_track_data(
            2024, 1, SessionType.RACE
        )

    return run


def test_positions_telemetry_and_colours_are_extracted(load) -> None:
    session = _session()
    data = load(session)
    assert session.loaded_with == {
        "laps": True,
        "telemetry": True,
        "weather": False,
        "messages": False,
    }
    ver, ham, nor = data.drivers
    assert (ver.abbreviation, ver.team_color, ham.team_color, nor.team_color) == (
        "VER",
        "3671c6",
        "27f4d2",
        None,
    )
    assert ver.positions is not None
    assert ver.positions.session_time_ms == [1000, 2000, 4000]  # sorted, NaN X dropped
    assert ver.positions.x == [10, 20, 40]
    assert ver.positions.y == [-1, -2, -5]
    assert ver.positions.off_track == [False, False, True]
    assert ver.car is not None
    assert ver.car.session_time_ms == [1250, 2250]
    assert ver.car.rpm == [11000, None]
    assert ver.car.throttle_pct == [100, 104]  # kept as recorded
    assert ver.car.brake == [False, True]
    assert ver.car.drs_code == [12, 8]
    assert ham.positions is None and ham.car is not None
    assert nor.positions is None and nor.car is None


def test_circuit_info_is_extracted(load) -> None:
    circuit = load(_session()).circuit
    assert circuit is not None
    assert circuit.rotation_deg == 92.0
    assert [(c.number, c.letter, c.x, c.y, c.angle_deg) for c in circuit.corners] == [
        (1, "", 1.0, 2.0, 10.0)
    ]


def test_missing_circuit_info_is_not_fatal(load) -> None:
    assert load(_session(circuit=RuntimeError("no fastest lap"))).circuit is None


def test_unavailable_channels_yield_no_series(load) -> None:
    session = _session()
    del session.car_data
    data = load(session)
    assert all(d.car is None for d in data.drivers)
    assert data.drivers[0].positions is not None
