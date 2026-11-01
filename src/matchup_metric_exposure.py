"""Explicit denominator provenance for CFBD advanced scouting statistics.

Game-level split metrics in CFBD /stats/game/advanced do not report a
rush/pass play count. Never use overall plays or a football box-score
attempt total as a substitute for garbage-time-excluded eligible plays.
No implicit weights or fabricated effective sample sizes.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricExposure:
    metric: str
    source_value: str
    denominator: str | None
    exposure_unit: str
    weighting: str
    aggregation: str
    warning: str | None = None


SPLIT_WARNING = (
    "CFBD game-advanced rushing/passing splits lack eligible play counts; "
    "one game is one observation until matching play-level data is verified"
)

METRIC_EXPOSURE: dict[str, MetricExposure] = {
    "success_rate": MetricExposure(
        "success_rate", "off_success_rate", "off_plays",
        "garbage_time_excluded_offensive_plays", "play_count",
        "play_weighted_mean_of_game_rates"),
    "ppa": MetricExposure(
        "ppa", "off_ppa", "off_plays",
        "garbage_time_excluded_offensive_plays", "play_count",
        "play_weighted_mean_of_game_rates"),
    "explosiveness": MetricExposure(
        "explosiveness", "off_explosiveness", None,
        "conditional_successful_plays", "game_equal",
        "equal_game_mean_provisional",
        "Explosiveness is conditional on successful plays; total offensive plays "
        "is not its valid denominator."),
    **{
        f"{side}_{kind}": MetricExposure(
            f"{side}_{kind}", f"off_{side}_{kind}", None,
            "successful_plays_of_type" if kind == "explosiveness"
            else f"eligible_{side}_plays",
            "game_equal", "equal_game_mean_provisional",
            SPLIT_WARNING + (
                "; explosiveness additionally requires successful plays of type"
                if kind == "explosiveness" else "")
        )
        for side in ("rush", "pass")
        for kind in ("success_rate", "ppa", "explosiveness")
    },
}


def exposure_policy(metric: str, available_columns: set[str]) -> dict:
    """Return an auditable policy; never infer missing split opportunities."""
    if metric not in METRIC_EXPOSURE:
        raise ValueError(f"Unrecognized metric: {metric}")
    policy = METRIC_EXPOSURE[metric]
    observed_denominator = (
        policy.denominator if policy.denominator in available_columns
        else None
    )
    if policy.weighting == "play_count" and not observed_denominator:
        return {
            "metric": metric, "source_value": policy.source_value,
            "denominator_field": None, "denominator_status": "unavailable",
            "weighting": "game_equal", "aggregation": "equal_game_mean_provisional",
            "exposure_unit": policy.exposure_unit,
            "warning": "Total offense play count column is absent; using equal games",
        }
    return {
        "metric": metric, "source_value": policy.source_value,
        "denominator_field": observed_denominator,
        "denominator_status": "verified_source_field" if observed_denominator
        else "not_supplied_by_source",
        "weighting": policy.weighting if observed_denominator else "game_equal",
        "aggregation": policy.aggregation if observed_denominator
        else "equal_game_mean_provisional",
        "exposure_unit": policy.exposure_unit,
        "warning": policy.warning,
    }
