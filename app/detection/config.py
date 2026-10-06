"""Detection configuration.

``DetectionConfig`` is built from ``Settings`` so every component shares one source of
truth. Detectors read their thresholds from it (never from module constants), and
tests build variants with ``dataclasses.replace``. All durations are milliseconds,
all windows are counted in laps.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.core.config import Settings
from app.domain.enums import TrackStatus

#: Track statuses that always make a lap unrepresentative (neutralised or stopped).
NEUTRALISED_STATUSES: frozenset[TrackStatus] = frozenset(
    {
        TrackStatus.SAFETY_CAR,
        TrackStatus.VIRTUAL_SAFETY_CAR,
        TrackStatus.VIRTUAL_SAFETY_CAR_ENDING,
        TrackStatus.RED_FLAG,
    }
)


@dataclass(frozen=True, slots=True)
class PaceConfig:
    """Rules shared by every detector that reasons about lap times."""

    #: ``YELLOW`` also disqualifies a lap (local yellows slow laps without being a regime
    #: change). Neutralised statuses always do.
    exclude_yellow: bool = True
    #: Laps after a neutralised lap that are still excluded (restart effects).
    restart_laps: int = 1
    #: A baseline may span at most this many more laps than it has clean laps; larger
    #: holes (missing events, long neutralisations) restart the baseline.
    max_excluded_laps: int = 3
    #: Bound on stored track status changes.
    track_history_limit: int = 100

    @property
    def abnormal_statuses(self) -> frozenset[TrackStatus]:
        if self.exclude_yellow:
            return NEUTRALISED_STATUSES | {TrackStatus.YELLOW}
        return NEUTRALISED_STATUSES


@dataclass(frozen=True, slots=True)
class BattleConfig:
    #: Gap (to the car ahead) at or below which cars are in a battle.
    gap_ms: int = 1_000
    #: A battle only ends above this gap (hysteresis against flapping around ``gap_ms``).
    release_gap_ms: int = 1_500
    #: Consecutive lap observations needed (the window).
    window_laps: int = 4
    #: Minimum net closing per lap for ``BATTLE_FORMING``.
    min_closing_rate_ms: int = 200
    #: At most this many non-decreasing steps inside the window.
    max_non_closing_steps: int = 1
    #: Laps that must pass after a battle ended (or an approach was reported) before the
    #: same pair can be reported again.
    cooldown_laps: int = 3
    #: ``RAPIDLY_CLOSING`` zone: above ``gap_ms`` up to this gap.
    rapid_max_gap_ms: int = 5_000
    rapid_min_rate_ms: int = 500
    #: Bound on remembered per-defender cooldown entries.
    memory_pairs: int = 8


@dataclass(frozen=True, slots=True)
class OvertakeConfig:
    #: Bound on remembered reported swaps (guards against re-reporting).
    memory_swaps: int = 24


@dataclass(frozen=True, slots=True)
class PersonalBestConfig:
    #: Improvement over the last reported best needed to emit. The true best is always
    #: tracked, so a slow sequence of tiny gains (fuel burn) adds up until it counts.
    min_improvement_ms: int = 300


@dataclass(frozen=True, slots=True)
class DegradationConfig:
    baseline_laps: int = 5
    recent_laps: int = 5
    #: ``median(recent) - median(baseline)`` needed.
    threshold_ms: int = 500
    #: Re-emit within a stint only when the delta grew by this much.
    reemit_step_ms: int = 500
    #: Severity bands on the delta.
    medium_delta_ms: int = 1_000
    high_delta_ms: int = 2_000


@dataclass(frozen=True, slots=True)
class AnomalyConfig:
    baseline_laps: int = 5
    #: Robust score (``deviation / (1.4826 * MAD)``) needed.
    min_score: float = 4.0
    #: Absolute deviation needed as well, so tiny MADs cannot create noise.
    min_deviation_ms: int = 2_000
    #: Lower bound for the MAD used in the score.
    mad_floor_ms: int = 150
    #: Severity bands on the deviation.
    medium_deviation_ms: int = 4_000
    high_deviation_ms: int = 10_000


@dataclass(frozen=True, slots=True)
class DetectionConfig:
    #: Redis key prefix and TTL of the per-replay detection context.
    key_prefix: str = "race"
    ttl_seconds: int = 604_800
    #: Attempts per state event when another worker keeps changing the context.
    conflict_attempts: int = 3
    #: Detector names that are not registered.
    disabled_detectors: frozenset[str] = frozenset()
    pace: PaceConfig = field(default_factory=PaceConfig)
    battle: BattleConfig = field(default_factory=BattleConfig)
    overtake: OvertakeConfig = field(default_factory=OvertakeConfig)
    personal_best: PersonalBestConfig = field(default_factory=PersonalBestConfig)
    degradation: DegradationConfig = field(default_factory=DegradationConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)

    def __post_init__(self) -> None:
        b, d, a = self.battle, self.degradation, self.anomaly
        problems: list[str] = []
        if b.release_gap_ms < b.gap_ms:
            problems.append("battle release gap must be >= battle gap")
        if b.rapid_max_gap_ms <= b.gap_ms:
            problems.append("rapid closing max gap must be > battle gap")
        if b.window_laps < 2:
            problems.append("battle window must be >= 2 laps")
        if d.baseline_laps < 2 or d.recent_laps < 2:
            problems.append("degradation windows must be >= 2 laps")
        if a.baseline_laps < 2:
            problems.append("anomaly baseline must be >= 2 laps")
        if problems:
            raise ValueError("Invalid detection configuration: " + "; ".join(problems))

    @classmethod
    def from_settings(cls, settings: Settings) -> DetectionConfig:
        return cls(
            key_prefix=settings.detection_key_prefix,
            ttl_seconds=settings.detection_context_ttl_seconds,
            disabled_detectors=frozenset(
                name.strip()
                for name in settings.detection_disabled_detectors.split(",")
                if name.strip()
            ),
            pace=PaceConfig(exclude_yellow=settings.detection_exclude_yellow),
            battle=BattleConfig(
                gap_ms=settings.detection_battle_gap_ms,
                release_gap_ms=settings.detection_battle_release_gap_ms,
                window_laps=settings.detection_battle_window_laps,
                min_closing_rate_ms=settings.detection_battle_min_closing_rate_ms,
                cooldown_laps=settings.detection_battle_cooldown_laps,
                rapid_max_gap_ms=settings.detection_rapid_closing_max_gap_ms,
                rapid_min_rate_ms=settings.detection_rapid_closing_min_rate_ms,
            ),
            personal_best=PersonalBestConfig(
                min_improvement_ms=settings.detection_pb_min_improvement_ms
            ),
            degradation=DegradationConfig(
                baseline_laps=settings.detection_degradation_baseline_laps,
                recent_laps=settings.detection_degradation_recent_laps,
                threshold_ms=settings.detection_degradation_threshold_ms,
                reemit_step_ms=settings.detection_degradation_reemit_step_ms,
            ),
            anomaly=AnomalyConfig(
                baseline_laps=settings.detection_anomaly_baseline_laps,
                min_score=settings.detection_anomaly_min_score,
                min_deviation_ms=settings.detection_anomaly_min_deviation_ms,
            ),
        )
