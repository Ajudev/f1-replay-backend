"""Virtual race clock behavior against a controllable time source."""

from __future__ import annotations

import pytest

from app.replay.clock import VirtualRaceClock


class FakeTime:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_does_not_advance_before_start() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t)
    t.now += 10
    assert clock.now_ms() == 0
    assert not clock.running
    assert clock.real_seconds_until(5_000) is None


@pytest.mark.parametrize(("speed", "expected_ms"), [(1, 10_000), (5, 50_000), (20, 200_000)])
def test_speed_multiplier(speed: float, expected_ms: int) -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t, speed=speed)
    clock.start()
    t.now += 10
    assert clock.now_ms() == expected_ms


def test_pause_freezes_and_resume_continues_from_paused_time() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t, speed=2)
    clock.start()
    t.now += 5
    clock.pause()
    assert clock.now_ms() == 10_000
    t.now += 1_000
    assert clock.now_ms() == 10_000
    clock.resume()
    assert clock.now_ms() == 10_000
    t.now += 1
    assert clock.now_ms() == 12_000


def test_speed_change_while_running_preserves_position() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t, speed=5)
    clock.start()
    t.now += 306  # 00:25:30 of race time at 5x
    assert clock.now_ms() == 1_530_000
    clock.set_speed(10)
    assert clock.now_ms() == 1_530_000
    t.now += 1
    assert clock.now_ms() == 1_540_000
    clock.set_speed(2)
    t.now += 1
    assert clock.now_ms() == 1_542_000


def test_speed_change_while_paused_applies_on_resume() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t)
    clock.start()
    t.now += 3
    clock.pause()
    clock.set_speed(10)
    t.now += 50
    assert clock.now_ms() == 3_000
    clock.resume()
    t.now += 1
    assert clock.now_ms() == 13_000


def test_reset_returns_to_position_and_pauses() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t, speed=20)
    clock.start()
    t.now += 30
    clock.reset()
    assert not clock.running
    assert clock.now_ms() == 0
    clock.reset(42_000)
    t.now += 5
    assert clock.now_ms() == 42_000


def test_real_seconds_until_uses_speed_and_never_negative() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t, speed=10)
    clock.start()
    assert clock.real_seconds_until(50_000) == pytest.approx(5.0)
    t.now += 6
    assert clock.real_seconds_until(50_000) == 0.0


def test_no_drift_over_many_small_steps() -> None:
    t = FakeTime()
    clock = VirtualRaceClock(t, speed=20)
    clock.start()
    for _ in range(100_000):
        t.now += 0.001
    assert clock.now_ms() == 2_000_000


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan")])
def test_rejects_invalid_speed(bad: float) -> None:
    t = FakeTime()
    with pytest.raises(ValueError):
        VirtualRaceClock(t, speed=bad)
    clock = VirtualRaceClock(t)
    with pytest.raises(ValueError):
        clock.set_speed(bad)


def test_double_start_and_double_pause_rejected() -> None:
    clock = VirtualRaceClock(FakeTime())
    clock.start()
    with pytest.raises(RuntimeError):
        clock.start()
    clock.pause()
    with pytest.raises(RuntimeError):
        clock.pause()
