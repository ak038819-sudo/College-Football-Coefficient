"""Pregame scouting presentation adapter for Elocast's ONE adjusted-stat engine.

Do not implement a second regression here. All fitted offensive, defensive,
and venue effects come from src/build_adjusted_stats.solve(), the existing
production-adjusted-stat implementation. For unit-weight game observations the
default shrinkage is a *provisional* per-game strength; play-weighted production
metrics use their existing calibrated metric-specific lambdas instead.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable

try:
    from src import build_adjusted_stats as adjusted
except ModuleNotFoundError:
    import build_adjusted_stats as adjusted


@dataclass(frozen=True)
class Observation:
    team: str
    opponent: str
    value: float
    weight: float = 1.0
    side: float = 0.0  # +.5 home, -.5 away, 0 neutral


@dataclass(frozen=True)
class TeamGrade:
    team: str
    offense: float
    defense_allowed: float
    offensive_percentile: float
    defensive_percentile: float
    games: int
    home_effect: float = 0.0
    distinct_opponents: int = 0
    reliability: str = "insufficient"


@dataclass(frozen=True)
class MatchupEstimate:
    offense: str
    defense: str
    projected_value: float
    national_average: float
    offense_adjustment: float
    defense_adjustment: float
    venue_adjustment: float = 0.0


def reliability_label(games: int, distinct_opponents: int) -> str:
    """Data-sufficiency tier, NOT a calibrated predictive confidence interval.

    Both volume and schedule diversity are necessary. The early weeks remain
    explicitly provisional even when the solver produces numeric effects.
    """
    if games < 3 or distinct_opponents < 3:
        return "insufficient"
    if games < 5 or distinct_opponents < 5:
        return "provisional"
    if games < 8 or distinct_opponents < 7:
        return "developing"
    return "established"


def _percentile(value: float, population: list[float]) -> float:
    below = sum(v < value for v in population)
    tied = sum(v == value for v in population)
    return 100 * (below + .5 * tied) / len(population)


def fit_profiles(
    observations: Iterable[Observation],
    *,
    higher_is_better: bool = True,
    shrinkage: float = 2.0,
    iterations: int = 80,
    min_games: int = 3,
) -> tuple[dict[str, TeamGrade], float]:
    """Convert a canonical adjusted_stats.solve fit into scouting percentiles.

    Iterations is retained for callers' compatibility; the shared solver owns
    its own convergence controls. Games counts require at least min_games on
    *each* side of the observed matchup graph.
    """
    if shrinkage < 0 or iterations < 1 or min_games < 1:
        raise ValueError("Invalid fitting parameters")
    rows = list(observations)
    if not rows:
        raise ValueError("No observations")
    offensive: dict[str, int] = {}
    defensive: dict[str, int] = {}
    opponents: dict[str, set[str]] = {}
    tuples = []
    for r in rows:
        if not r.team or not r.opponent or r.team == r.opponent:
            raise ValueError("Invalid team/opponent")
        if not all(map(isfinite, (r.value, r.weight, r.side))) or r.weight <= 0:
            raise ValueError("Nonfinite value/venue or nonpositive weight")
        if r.side not in (-.5, 0, .5):
            raise ValueError("Venue side must be +.5, -.5 or 0")
        opponents.setdefault(r.team, set()).add(r.opponent)
        opponents.setdefault(r.opponent, set()).add(r.team)
        offensive[r.team] = offensive.get(r.team, 0) + 1
        defensive[r.opponent] = defensive.get(r.opponent, 0) + 1
        tuples.append((r.team, r.opponent, r.value, r.weight, r.side))
    fit = adjusted.solve(tuples, shrinkage)
    qualified = sorted(t for t in fit["off"] if (
        offensive.get(t, 0) >= min_games and defensive.get(t, 0) >= min_games))
    if not qualified:
        return {}, fit["mu"]
    direction = 1 if higher_is_better else -1
    off_dist = [direction * fit["off"][t] for t in qualified]
    def_dist = [-direction * fit["def"][t] for t in qualified]
    grades = {
        t: TeamGrade(t, fit["off"][t], fit["def"][t],
                     _percentile(direction * fit["off"][t], off_dist),
                     _percentile(-direction * fit["def"][t], def_dist),
                     offensive[t], fit["home"], len(opponents.get(t, ())),
                     reliability_label(min(offensive[t], defensive[t]),
                                       len(opponents.get(t, ()))))
        for t in qualified
    }
    # A simple return signature is maintained for existing consumers.
    # Use project_matchup(..., venue_adjustment=...) when venue has to be applied.
    return grades, fit["mu"]


def project_matchup(
    profiles: dict[str, TeamGrade], league_average: float,
    offense: str, defense: str, *, venue_adjustment: float = 0.0
) -> MatchupEstimate:
    """Metric-level expectation, not a score or calibrated win probability."""
    if offense not in profiles or defense not in profiles:
        raise ValueError("Both teams need sufficient completed-game coverage")
    if not isfinite(league_average) or not isfinite(venue_adjustment):
        raise ValueError("Invalid baseline or venue adjustment")
    a, d = profiles[offense], profiles[defense]
    return MatchupEstimate(
        offense, defense,
        league_average + a.offense + d.defense_allowed + venue_adjustment,
        league_average, a.offense, d.defense_allowed, venue_adjustment)
