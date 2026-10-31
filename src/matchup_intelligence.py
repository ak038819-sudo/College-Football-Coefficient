"""Opponent-adjusted team scouting and matchup estimates.

Experimental standalone analytics: does not modify Elo, Merits, or deployed UI.
A row is an OFFENSIVE team-game observation of one metric against one opponent.
Callers MUST supply only games completed before the preview cutoff.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from statistics import mean
from typing import Iterable


@dataclass(frozen=True)
class Observation:
    team: str
    opponent: str
    value: float
    weight: float = 1.0


@dataclass(frozen=True)
class TeamGrade:
    team: str
    offense: float
    defense_allowed: float
    offensive_percentile: float
    defensive_percentile: float
    games: int


@dataclass(frozen=True)
class MatchupEstimate:
    offense: str
    defense: str
    projected_value: float
    national_average: float
    offense_adjustment: float
    defense_adjustment: float


def _percentile(value: float, population: list[float]) -> float:
    """Midrank empirical percentile. A higher grade is always favorable."""
    below = sum(v < value for v in population)
    tied = sum(v == value for v in population)
    return 100.0 * (below + 0.5 * tied) / len(population)


def fit_profiles(
    observations: Iterable[Observation],
    *,
    higher_is_better: bool = True,
    shrinkage: float = 8.0,
    iterations: int = 80,
    min_games: int = 3,
) -> tuple[dict[str, TeamGrade], float]:
    """Fit regularized two-way offensive/defensive effects.

    Value = league average + offensive effect + defensive allowance effect.
    Defensive grades reverse sign: allowing less is better for the defense
    when higher_is_better=True. For a negative-direction metric (e.g. turnovers),
    both percentiles reverse appropriately.

    Per-game observations should not be duplicated as separate defense rows.
    Incomplete or nonfinite samples are refused rather than silently zeroed.
    """
    if shrinkage < 0 or iterations < 1 or min_games < 1:
        raise ValueError("Invalid fitting parameters")
    rows = list(observations)
    if not rows:
        raise ValueError("No observations")
    counts: dict[str, int] = {}
    offenses: dict[str, list[Observation]] = {}
    defenses: dict[str, list[Observation]] = {}
    for r in rows:
        if not r.team or not r.opponent or r.team == r.opponent:
            raise ValueError("Invalid team/opponent")
        if not isfinite(r.value) or not isfinite(r.weight) or r.weight <= 0:
            raise ValueError("Nonfinite value or nonpositive weight")
        offenses.setdefault(r.team, []).append(r)
        defenses.setdefault(r.opponent, []).append(r)
        counts[r.team] = counts.get(r.team, 0) + 1
    teams = sorted(set(offenses) | set(defenses))
    total_weight = sum(r.weight for r in rows)
    average = sum(r.value * r.weight for r in rows) / total_weight
    attack = {t: 0.0 for t in teams}
    allowance = {t: 0.0 for t in teams}
    # Ridge-penalized coordinate descent. Ridge makes sparse teams regress
    # toward the national baseline instead of producing extreme estimates.
    for _ in range(iterations):
        for t, group in offenses.items():
            attack[t] = sum(
                r.weight * (r.value - average - allowance[r.opponent]) for r in group
            ) / (shrinkage + sum(r.weight for r in group))
        for t, group in defenses.items():
            allowance[t] = sum(
                r.weight * (r.value - average - attack[r.team]) for r in group
            ) / (shrinkage + sum(r.weight for r in group))
    eligible = [
        t for t in teams if len(offenses.get(t, [])) >= min_games
        and len(defenses.get(t, [])) >= min_games
    ]
    if not eligible:
        return {}, average
    direction = 1 if higher_is_better else -1
    off_dist = [direction * attack[t] for t in eligible]
    def_dist = [-direction * allowance[t] for t in eligible]
    return {
        t: TeamGrade(
            team=t,
            offense=attack[t],
            defense_allowed=allowance[t],
            offensive_percentile=_percentile(direction * attack[t], off_dist),
            defensive_percentile=_percentile(-direction * allowance[t], def_dist),
            games=counts.get(t, 0),
        )
        for t in eligible
    }, average


def project_matchup(
    profiles: dict[str, TeamGrade], league_average: float, offense: str, defense: str
) -> MatchupEstimate:
    """Estimate one offensive metric against a particular defense.

    This is a point estimate on the original metric scale, NOT a win probability,
    and has no venue adjustment or uncertainty interval yet.
    """
    if offense not in profiles or defense not in profiles:
        raise ValueError("Both teams need sufficient completed-game coverage")
    if not isfinite(league_average):
        raise ValueError("Invalid baseline")
    a, d = profiles[offense], profiles[defense]
    return MatchupEstimate(
        offense=offense,
        defense=defense,
        projected_value=league_average + a.offense + d.defense_allowed,
        national_average=league_average,
        offense_adjustment=a.offense,
        defense_adjustment=d.defense_allowed,
    )
