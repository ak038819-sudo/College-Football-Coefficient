#!/usr/bin/env python3
"""Fail-closed audit for the static descriptive UI handoff JSON.

Checks referential integrity, availability rules, finite numeric values,
metric-specific denominator honesty and absence of predictive fields.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

FORBIDDEN = ("win_probability", "predicted_score", "projected_score",
             "spread_prediction", "expected_margin")


def verify_contract(payload: dict) -> dict:
    errors = []
    if payload.get("schema_version") != "elocast-descriptive-v1":
        errors.append("unrecognized schema")
    if payload.get("data_kind") != "historical_descriptive_fbs_units":
        errors.append("not a descriptive FBS contract")
    try:
        from datetime import date
        date.fromisoformat(payload["as_of_exclusive"])
    except (KeyError, TypeError, ValueError):
        errors.append("invalid as-of calendar date")
    metric_defs = payload.get("metric_definitions", {})
    teams = payload.get("teams", {})
    if not metric_defs or not teams:
        errors.append("empty metrics or team collection")
    for name, defn in metric_defs.items():
        if defn.get("observation_weighting") not in ("game_equal", "play_count"):
            errors.append(f"{name}: invalid weight policy")
        if name.startswith(("rush_","pass_")) and defn.get("denominator_field") is not None:
            errors.append(f"{name}: fabricated split denominator")
        if name.endswith("explosiveness") and defn.get("denominator_field") is not None:
            errors.append(f"{name}: wrong successful-play denominator")
        if defn.get("eligible_population", 0) < defn.get("minimum_population", 0) and defn.get("status") == "available":
            errors.append(f"{name}: population gate bypass")
    for key, team in teams.items():
        if str(team.get("team_id")) != key:
            errors.append(f"{key}: inconsistent canonical team identifier")
        if set(team.get("metrics", {})) != set(metric_defs):
            errors.append(f"{key}: missing metric records")
        for metric, row in team.get("metrics", {}).items():
            available = row.get("status") == "rated"
            rated_fields = ("adjusted_offense","adjusted_defense_allowed",
                            "offense_percentile","defense_percentile",
                            "offensive_schedule_percentile","defensive_schedule_percentile")
            for col in rated_fields:
                if not available and row.get(col) is not None:
                    errors.append(f"{key}/{metric}/{col}: rating leaked despite unavailability")
            if available and metric_defs[metric].get("status") != "available":
                errors.append(f"{key}/{metric}: team rated in unavailable metric")
            for col, value in row.items():
                if isinstance(value, float) and not math.isfinite(value):
                    errors.append(f"{key}/{metric}/{col}: nonfinite")
                if col.endswith("percentile") and value is not None and not (0 <= value <= 100):
                    errors.append(f"{key}/{metric}/{col}: percentile out of range")
            if row.get("sample_games", 0) < 0 or row.get("distinct_opponents", 0) < 0:
                errors.append(f"{key}/{metric}: negative sample size")
    serialized=json.dumps(payload).lower()
    if any(field in serialized for field in FORBIDDEN):
        errors.append("predictive claims included in descriptive contract")
    return {
        "passed": not errors, "errors": errors[:100],
        "team_count": len(teams), "metric_count": len(metric_defs),
        "rated_team_metric_pairs": sum(
            row.get("status")=="rated"
            for team in teams.values() for row in team.get("metrics", {}).values()),
    }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("file")
    args=p.parse_args()
    result=verify_contract(json.loads(Path(args.file).read_text()))
    print(json.dumps(result,indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__=="__main__":
    main()
