"""Optional live FastF1 smoke test (skipped by default)."""

from __future__ import annotations

import os

import pytest

from app.domain.enums import SessionType
from app.ingestion.adapter import FastF1SessionLoader
from app.ingestion.normalizer import normalize

pytestmark = pytest.mark.external


@pytest.mark.skipif(
    os.environ.get("FASTF1_RUN_EXTERNAL") != "1",
    reason="Set FASTF1_RUN_EXTERNAL=1 to run live FastF1 smoke test",
)
def test_fastf1_smoke_load_2024_round_1_race(tmp_path) -> None:  # type: ignore[no-untyped-def]
    cache_dir = tmp_path / "fastf1-cache"
    loader = FastF1SessionLoader(cache_dir=str(cache_dir))
    extracted = loader.load(2024, 1, SessionType.RACE)

    assert extracted.season == 2024
    assert extracted.round == 1
    assert extracted.event_name
    assert extracted.drivers
    assert extracted.laps

    normalized = normalize(extracted)
    assert normalized.drivers
    assert normalized.laps
    assert normalized.skipped_count >= 0
