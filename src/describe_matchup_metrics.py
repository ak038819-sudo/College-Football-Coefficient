"""Descriptive unit analytics using the existing adjusted-statistics solver.

No future-game estimation: quantify raw performance, schedule opposition,
venue exposure and the penalized team effect for the observed FBS games.
The residual explicitly reconciles ridge shrinkage; do not falsely call
raw-minus-adjusted entirely a schedule effect.
"""
from __future__ import annotations

from collections import defaultdict
from math import isfinite

try:
    from src.build_adjusted_stats import solve
    from src.matchup_intelligence import Observation, reliability_label, _percentile
except ModuleNotFoundError:
    from build_adjusted_stats import solve
    from matchup_intelligence import Observation, reliability_label, _percentile


def describe_units(
    observations: list[Observation], *,
    shrinkage: float = 2.0, min_games: int = 3,
    higher_is_better: bool = True,
) -> dict:
    """Return season-to-date *descriptions*, not next-game predictions.

    Defense 'allowed' is lower-is-better for typical offense-positive metrics.
    Schedule difficulty is opponent fitted strength in metric units:
      offense faces difficulty = -mean(opponents' defense allowance)
      defense faces difficulty =  mean(opponents' offensive quality)
    These are exposure measures, not opponent-independent causal estimates.
    """
    if shrinkage < 0 or min_games < 1 or not isfinite(shrinkage):
        raise ValueError("Invalid shrinkage or minimum games")
    if not observations:
        return {"status": "no_data", "teams": {}, "rated_teams": 0}
    tuples = []
    for r in observations:
        if not r.team or not r.opponent or r.team == r.opponent:
            raise ValueError("Invalid offense/defense identity")
        if not all(isfinite(v) for v in (r.value, r.weight, r.side)) or r.weight <= 0:
            raise ValueError("Nonfinite observation or invalid weight")
        if r.side not in (-.5, 0, .5):
            raise ValueError("Invalid venue-side convention")
        tuples.append((r.team, r.opponent, r.value, r.weight, r.side))
    fit = solve(tuples, shrinkage)
    mu, venue = fit["mu"], fit["home"]
    off_rows, def_rows = defaultdict(list), defaultdict(list)
    opponents = defaultdict(set)
    for r in observations:
        off_rows[r.team].append(r)
        def_rows[r.opponent].append(r)
        opponents[r.team].add(r.opponent)
        opponents[r.opponent].add(r.team)
    # Each rating is conditioned on a qualified *team's* own sample size.
    # The underlying solver fits all available opponents, but only qualified
    # teams contribute to national percentiles.
    eligible = sorted(t for t in set(off_rows) | set(def_rows) if
                      len(off_rows[t]) >= min_games and
                      len(def_rows[t]) >= min_games and
                      len(opponents[t]) >= min_games)
    direction = 1 if higher_is_better else -1
    strength_off = [direction * fit["off"][t] for t in eligible]
    strength_def = [-direction * fit["def"][t] for t in eligible]
    out = {}
    for team in sorted(set(off_rows) | set(def_rows)):
        attack, facing = off_rows[team], def_rows[team]
        def summarize(rows, schedule, adjusted):
            if not rows:
                return None
            weight = sum(r.weight for r in rows)
            raw = sum(r.weight * r.value for r in rows) / weight
            opponent_effect = sum(r.weight * schedule(r) for r in rows) / weight
            venue_effect = sum(r.weight * venue * r.side for r in rows) / weight
            # Shrinkage introduces a residual. We expose it rather than
            # attributing all of raw-adjusted to difficulty of opponents.
            residual = raw - adjusted - opponent_effect - venue_effect
            return {
                "raw": raw, "opponent_adjusted": adjusted,
                "opponent_effect": opponent_effect,
                "venue_effect": venue_effect,
                "ridge_residual": residual, "games": len(rows),
                "total_weight": weight,
                "raw_minus_adjusted": raw - adjusted,
            }
        a = summarize(attack, lambda r: fit["def"][r.opponent],
                      mu + fit["off"][team])
        d = summarize(facing, lambda r: fit["off"][r.team],
                      mu + fit["def"][team])
        rated = team in eligible
        out[team] = {
            "offense": a, "defense": d,
            "opponents": len(opponents[team]),
            "reliability": reliability_label(
                min(len(attack), len(facing)), len(opponents[team])),
            "rating_eligible": rated,
            "offensive_percentile": (
                _percentile(direction * fit["off"][team], strength_off)
                if rated else None),
            "defensive_percentile": (
                _percentile(-direction * fit["def"][team], strength_def)
                if rated else None),
            # A larger number always means tougher opposing units, provided
            # higher_is_better describes the underlying offensive metric.
            "opposing_defense_difficulty": (
                -direction * a["opponent_effect"] if a else None),
            "opposing_offense_difficulty": (
                direction * d["opponent_effect"] if d else None),
        }
    for side, attr in (("opposing_defense_difficulty", "offensive_schedule_percentile"),
                       ("opposing_offense_difficulty", "defensive_schedule_percentile")):
        dist = [out[t][side] for t in eligible if out[t][side] is not None]
        for t in out:
            out[t][attr] = (
                _percentile(out[t][side], dist) if t in eligible and dist else None)
    return {
        "status": "available", "baseline": mu, "home_effect": venue,
        "shrinkage": shrinkage, "rated_teams": len(eligible),
        "schedule_method": "weighted_fitted_opponent_effects",
        "teams": out,
    }
