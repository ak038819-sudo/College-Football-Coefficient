#!/usr/bin/env python3
"""Research-only broad descriptive percentile stability across FBS schedules.

Measures (1) leave-one-opponent-out sensitivity on deterministic samples
spanning the full ordinal range of rated teams and (2) week-to-week rank
churn among the SAME eligible team cohort. Do not interpret chronological
rank change as measurement error: actual games happened between cutoffs.

No CFBD traffic, no prediction evaluation, no production writes.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from math import isfinite
from pathlib import Path

try:
    from src.build_matchup_intelligence import load_observations
    from src.describe_matchup_metrics import describe_units
    from src.audit_matchup_stability import opponent_sensitivity
except ModuleNotFoundError:
    from build_matchup_intelligence import load_observations
    from describe_matchup_metrics import describe_units
    from audit_matchup_stability import opponent_sensitivity


METRICS = (
    "rush_success_rate", "pass_success_rate", "rush_ppa", "pass_ppa",
    "rush_explosiveness", "pass_explosiveness",
)


def percentile(values: list[float], quantile: float) -> float | None:
    """Exact linear interpolation on a sorted list; no external packages."""
    if not values:
        return None
    values = sorted(values)
    idx = (len(values) - 1) * quantile
    low = int(idx)
    high = min(len(values) - 1, low + 1)
    return values[low] + (idx - low) * (values[high] - values[low])


def sampled_across_distribution(teams: dict, count: int) -> list[str]:
    """Include weak/strong teams, not just a low-ID sample or top performers."""
    if count < 1:
        raise ValueError("Sample count must be positive")
    rated = [
        team for team, row in teams.items()
        if row["rating_eligible"] and row["offensive_percentile"] is not None
    ]
    ordered = sorted(rated, key=lambda t: (teams[t]["offensive_percentile"], t))
    if len(ordered) <= count:
        return ordered
    indices = sorted({round(i * (len(ordered) - 1) / (count - 1))
                      for i in range(count)}) if count > 1 else [len(ordered) // 2]
    return [ordered[i] for i in indices]


def summarize_sensitivity(rows: dict) -> dict:
    evaluated = [r for r in rows.values() if r.get("status") == "evaluated"]
    result = {"evaluated": len(evaluated), "requested": len(rows)}
    for key in ("max_offense_percentile_change", "max_defense_percentile_change"):
        values = [float(r[key]) for r in evaluated if r[key] is not None]
        result[key] = {
            "median": percentile(values, .5),
            "p90": percentile(values, .9),
            "max": max(values) if values else None,
            "at_least_10_points": sum(v >= 10 for v in values),
            "at_least_20_points": sum(v >= 20 for v in values),
        }
    return result


def analyze(conn: sqlite3.Connection, *, season: int,
            dates: tuple[str, ...], sample_size: int = 16,
            shrinkage: float = 2.) -> dict:
    if not dates or list(dates) != sorted(set(dates)):
        raise ValueError("Provide chronological, unique dates")
    if sample_size < 1 or not isfinite(shrinkage) or shrinkage < 0:
        raise ValueError("Invalid research configuration")
    report = {"season": season, "dates": dates, "sample_size": sample_size,
              "method": "sampled_leave_one_opponent_out_and_matched_weekly_churn",
              "metric_reports": {}}
    for metric in METRICS:
        previous = None
        snapshots = []
        for day in dates:
            observations, coverage = load_observations(
                conn, season=season, as_of=day, metric=metric)
            if not observations:
                snapshots.append({"as_of": day, "status": "no_data",
                                  "team_games": 0, "rated_teams": 0})
                continue
            fitted = describe_units(observations, shrinkage=shrinkage)
            teams = fitted["teams"]
            selected = sampled_across_distribution(teams, sample_size)
            stability = opponent_sensitivity(
                observations, shrinkage=shrinkage,
                target_teams=tuple(selected)) if selected else {}
            churn = None
            if previous is not None:
                old = previous
                shared = [
                    t for t in old if t in teams and old[t]["rating_eligible"]
                    and teams[t]["rating_eligible"]
                ]
                churn = {
                    "shared_eligible_teams": len(shared),
                    "offense_absolute_changes": {
                        "median": percentile([
                            abs(teams[t]["offensive_percentile"] -
                                old[t]["offensive_percentile"]) for t in shared], .5),
                        "p90": percentile([
                            abs(teams[t]["offensive_percentile"] -
                                old[t]["offensive_percentile"]) for t in shared], .9),
                    },
                    "defense_absolute_changes": {
                        "median": percentile([
                            abs(teams[t]["defensive_percentile"] -
                                old[t]["defensive_percentile"]) for t in shared], .5),
                        "p90": percentile([
                            abs(teams[t]["defensive_percentile"] -
                                old[t]["defensive_percentile"]) for t in shared], .9),
                    },
                }
            snapshots.append({
                "as_of": day, "status": "evaluated",
                "team_games": len(observations), "rated_teams": fitted["rated_teams"],
                "established_teams": sum(t["reliability"] == "established"
                                         for t in teams.values()),
                "sampled_teams": selected, "sensitivity": summarize_sensitivity(stability),
                "weekly_churn": churn,
                "coverage": {"eligible_rows": coverage["eligible_rows"],
                             "invalid_metric_value": coverage["invalid_metric_value"]},
            })
            previous = teams
        report["metric_reports"][metric] = snapshots
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--as-of", action="append", required=True)
    p.add_argument("--sample-size", type=int, default=16)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    with sqlite3.connect(f"file:{Path(args.db).resolve().as_posix()}?mode=ro",
                         uri=True) as conn:
        result = analyze(conn, season=args.season, dates=tuple(args.as_of),
                         sample_size=args.sample_size)
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for metric, reports in result["metric_reports"].items():
        for r in reports:
            sens = r.get("sensitivity", {})
            print(metric, r["as_of"], "rated=", r["rated_teams"],
                  "sample_valid=", sens.get("evaluated"),
                  "max_offense_p90=", sens.get("max_offense_percentile_change", {}).get("p90"),
                  "max_defense_p90=", sens.get("max_defense_percentile_change", {}).get("p90"))


if __name__ == "__main__":
    main()
