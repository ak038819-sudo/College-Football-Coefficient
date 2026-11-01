"""Leave-one-opponent-out robustness of descriptive FBS percentiles.

This is a descriptive sensitivity diagnostic, not a confidence interval.
Each refit removes both sides of the chosen opponent's games, preserving
schedule balance and using the *same* existing adjusted-statistics solver.
"""
from __future__ import annotations

from collections import defaultdict

try:
    from src.describe_matchup_metrics import describe_units
    from src.matchup_intelligence import Observation
except ModuleNotFoundError:
    from describe_matchup_metrics import describe_units
    from matchup_intelligence import Observation


def opponent_sensitivity(
    observations: list[Observation], *,
    shrinkage: float = 2., min_games: int = 3,
    higher_is_better: bool = True,
    min_competitors: int = 3,
) -> dict:
    if min_competitors < 1:
        raise ValueError("min_competitors must be positive")
    full = describe_units(observations, shrinkage=shrinkage,
                          min_games=min_games,
                          higher_is_better=higher_is_better)
    opponents = defaultdict(set)
    for row in observations:
        opponents[row.team].add(row.opponent)
        opponents[row.opponent].add(row.team)
    result = {}
    for team, baseline in full["teams"].items():
        if not baseline["rating_eligible"]:
            result[team] = {"status": "insufficient", "checks": 0}
            continue
        deltas_off, deltas_def = [], []
        for excluded in sorted(opponents[team]):
            subset = [
                r for r in observations
                if not ((r.team == team and r.opponent == excluded)
                        or (r.team == excluded and r.opponent == team))
            ]
            fit = describe_units(subset, shrinkage=shrinkage,
                                 min_games=min_games,
                                 higher_is_better=higher_is_better)
            revised = fit["teams"].get(team)
            # Do not claim robustness when the leave-one-out population is
            # too thin or the team loses its rating eligibility.
            if not revised or not revised["rating_eligible"] or (
                fit["rated_teams"] < min_competitors
            ):
                continue
            deltas_off.append(abs(
                revised["offensive_percentile"] -
                baseline["offensive_percentile"]))
            deltas_def.append(abs(
                revised["defensive_percentile"] -
                baseline["defensive_percentile"]))
        result[team] = {
            "status": "evaluated" if deltas_off else "insufficient",
            "checks": len(deltas_off),
            "opponents_available": len(opponents[team]),
            "max_offense_percentile_change": max(deltas_off) if deltas_off else None,
            "max_defense_percentile_change": max(deltas_def) if deltas_def else None,
            "mean_offense_percentile_change": (
                sum(deltas_off) / len(deltas_off) if deltas_off else None),
            "mean_defense_percentile_change": (
                sum(deltas_def) / len(deltas_def) if deltas_def else None),
            "note": "Sensitivity to removing one opponent; not a statistical confidence interval",
        }
    return result
