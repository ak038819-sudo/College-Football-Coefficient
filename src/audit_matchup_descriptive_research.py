#!/usr/bin/env python3
"""Real-data integrity report for descriptive scouting (no predictions).

Runs against a disposable SQLite DB with verified CFBD split observations.
Samples stable team IDs deterministically, reports leave-one-opponent-out
sensitivity, and checks raw/adjusted decomposition for every eligible team.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path

try:
    from src.build_matchup_intelligence import load_observations
    from src.describe_matchup_metrics import describe_units
    from src.audit_matchup_stability import opponent_sensitivity
except ModuleNotFoundError:
    from build_matchup_intelligence import load_observations
    from describe_matchup_metrics import describe_units
    from audit_matchup_stability import opponent_sensitivity


METRICS = ("rush_success_rate", "pass_success_rate",
           "rush_ppa", "pass_ppa", "rush_explosiveness", "pass_explosiveness")


def audit(conn: sqlite3.Connection, *, season: int, as_of: str, sample_size: int = 6) -> dict:
    if sample_size < 1:
        raise ValueError("sample_size must be positive")
    result = {"season": season, "as_of": as_of, "sample_size": sample_size,
              "method": "descriptive_effects_and_opponent_leave_one_out",
              "metrics": {}}
    for metric in METRICS:
        rows, coverage = load_observations(conn, season=season, as_of=as_of, metric=metric)
        if not rows:
            result["metrics"][metric] = {"status": "no_data", "coverage": coverage}
            continue
        fitted = describe_units(rows, shrinkage=2)
        names = fitted["teams"]
        qualified = sorted(t for t in names if names[t]["rating_eligible"])
        # Prefer teams that have established sample histories, evenly spread
        # over sorted IDs; deterministic rather than cherry-picking leaders.
        established = [t for t in qualified if names[t]["reliability"] == "established"]
        pool = established or qualified
        selected = [
            pool[min(len(pool) - 1, (i * len(pool)) // min(sample_size, len(pool)))]
            for i in range(min(sample_size, len(pool)))
        ] if pool else []
        failures = []
        for t, data in names.items():
            for side in ("offense", "defense"):
                info = data[side]
                if info is None:
                    continue
                components = info["opponent_adjusted"] + info["opponent_effect"] + info["venue_effect"] + info["ridge_residual"]
                if not math.isclose(info["raw"], components, abs_tol=1e-10, rel_tol=1e-10):
                    failures.append({"team": t, "side": side, "reason": "decomposition"})
            for key in ("offensive_percentile", "defensive_percentile",
                        "offensive_schedule_percentile", "defensive_schedule_percentile"):
                value = data[key]
                if value is not None and (not math.isfinite(value) or not 0 <= value <= 100):
                    failures.append({"team": t, "field": key, "reason": "invalid_percentile"})
        sensitivity = opponent_sensitivity(
            rows, shrinkage=2, target_teams=tuple(selected)) if selected else {}
        result["metrics"][metric] = {
            "status": "pass" if not failures else "fail",
            "observation_sides": len(rows),
            "rated_teams": fitted["rated_teams"],
            "established_teams": len(established),
            "coverage": coverage,
            "failed_invariants": failures[:20],
            "sensitivity_sample": sensitivity,
            "sensitivity_evaluated": sum(x["status"] == "evaluated" for x in sensitivity.values()),
        }
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--as-of", required=True)
    p.add_argument("--sample-size", type=int, default=6)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    with sqlite3.connect(f"file:{Path(args.db).resolve().as_posix()}?mode=ro", uri=True) as conn:
        report = audit(conn, season=args.season, as_of=args.as_of,
                       sample_size=args.sample_size)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    for metric, r in report["metrics"].items():
        print(metric, r["status"], "teams=", r.get("rated_teams"),
              "established=", r.get("established_teams"),
              "leave-out evaluated=", r.get("sensitivity_evaluated"))
    if any(r["status"] != "pass" for r in report["metrics"].values()):
        raise SystemExit("Descriptive integrity check blocked")


if __name__ == "__main__":
    main()
