#!/usr/bin/env python3
"""UI-design handoff data contract: descriptive statistics only.

No predictions, matchup point estimates, win probabilities, browser assets or
production data writes. Every metric includes provenance, as-of date and
separately gated percentiles. Comparisons align contrasting offense/defense
units, without claiming a future result.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

try:
    from src.build_matchup_intelligence import METRICS, build_snapshot
except ModuleNotFoundError:
    from build_matchup_intelligence import METRICS, build_snapshot

VERSION = "elocast-descriptive-v1"
DEFAULT_MIN_POPULATION = 30
UNIT_METRICS = (
    "success_rate", "ppa", "explosiveness",
    "rush_success_rate", "rush_ppa", "rush_explosiveness",
    "pass_success_rate", "pass_ppa", "pass_explosiveness",
)


def build_descriptive_contract(
    conn: sqlite3.Connection, *, season: int, as_of: str,
    team_ids: tuple[int, ...] | None = None,
    metrics: tuple[str, ...] = UNIT_METRICS,
    min_population: int = DEFAULT_MIN_POPULATION,
) -> dict:
    if min_population < 1:
        raise ValueError("min_population must be positive")
    if len(metrics) != len(set(metrics)) or not set(metrics).issubset(METRICS):
        raise ValueError("Metrics must be a unique subset of the official allowlist")
    if team_ids is not None and (not team_ids or len(team_ids) != len(set(team_ids))):
        raise ValueError("Team IDs must be unique and nonempty")
    all_names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    if team_ids is None:
        wanted = sorted(all_names)
    else:
        unknown = set(team_ids) - all_names.keys()
        if unknown:
            raise ValueError(f"Unrecognized team IDs: {sorted(unknown)}")
        wanted = list(team_ids)
    team_output = {str(t): {"team_id": t, "name": all_names[t], "metrics": {}}
                   for t in wanted}
    metric_output = {}
    for metric in metrics:
        snap = build_snapshot(conn, season=season, as_of=as_of, metric=metric)
        desc = snap.get("descriptive_units", {})
        population = desc.get("rated_teams", 0)
        valid = population >= min_population
        provenance = snap["coverage"]["exposure"]
        metric_output[metric] = {
            "status": "available" if valid else "insufficient_population",
            "eligible_population": population,
            "minimum_population": min_population,
            "fbs_eligible_team_game_rows": snap["coverage"]["eligible_rows"],
            "observation_weighting": provenance["weighting"],
            "denominator_status": provenance["denominator_status"],
            "denominator_field": provenance["denominator_field"],
            "exposure_unit": provenance["exposure_unit"],
            "provenance_warning": provenance["warning"],
            "metric_baseline": snap.get("baseline") if valid else None,
            "ridge_strength": snap.get("shrinkage"),
            "higher_percentile_means": "better offense or tougher opponent schedule",
        }
        profiles = desc.get("teams", {})
        for t in wanted:
            p = profiles.get(str(t))
            if not p:
                team_output[str(t)]["metrics"][metric] = {
                    "status": "no_observations", "sample_games": 0,
                    "distinct_opponents": 0, "reliability": "insufficient",
                    "raw_offense": None, "raw_defense_allowed": None,
                    "adjusted_offense": None, "adjusted_defense_allowed": None,
                    "offense_percentile": None, "defense_percentile": None,
                    "offensive_schedule_percentile": None,
                    "defensive_schedule_percentile": None,
                }
                continue
            attack, defense = p["offense"], p["defense"]
            rated = valid and p["rating_eligible"]
            team_output[str(t)]["metrics"][metric] = {
                "status": ("rated" if rated else
                           "insufficient_sample" if valid else "insufficient_population"),
                "sample_games": attack["games"] if attack else 0,
                "distinct_opponents": p["opponents"],
                "reliability": p["reliability"],
                "raw_offense": attack["raw"] if attack else None,
                "raw_defense_allowed": defense["raw"] if defense else None,
                "adjusted_offense": attack["opponent_adjusted"] if rated and attack else None,
                "adjusted_defense_allowed": defense["opponent_adjusted"] if rated and defense else None,
                "offense_percentile": p["offensive_percentile"] if rated else None,
                "defense_percentile": p["defensive_percentile"] if rated else None,
                "offensive_schedule_difficulty": (
                    p["opposing_defense_difficulty"] if rated else None),
                "defensive_schedule_difficulty": (
                    p["opposing_offense_difficulty"] if rated else None),
                "offensive_schedule_percentile": (
                    p["offensive_schedule_percentile"] if rated else None),
                "defensive_schedule_percentile": (
                    p["defensive_schedule_percentile"] if rated else None),
                "offense_venue_effect": attack["venue_effect"] if attack else None,
                "offense_ridge_residual": attack["ridge_residual"] if attack else None,
                "defense_venue_effect": defense["venue_effect"] if defense else None,
                "defense_ridge_residual": defense["ridge_residual"] if defense else None,
            }
    return {
        "schema_version": VERSION, "data_kind": "historical_descriptive_fbs_units",
        "season": season, "as_of_exclusive": as_of,
        "garbage_time": "excluded_by_CFBD_source",
        "classification": "explicit_fbs_both_sides",
        "metric_definitions": metric_output,
        "teams": team_output,
        "disclaimer": (
            "Ranks describe observed FBS games before the as-of calendar date. "
            "They are not forecasts or win probabilities. Individual games "
            "can move ratings substantially; split play denominators are unavailable."
        ),
    }


def compare_units(bundle: dict, *, offense_team_id: int, defense_team_id: int) -> dict:
    """Return descriptive side-by-side unit evidence, not a score estimate."""
    teams = bundle["teams"]
    offense, defense = str(offense_team_id), str(defense_team_id)
    if offense == defense or offense not in teams or defense not in teams:
        raise ValueError("Comparison requires distinct selected teams")
    compared = {}
    for metric in bundle["metric_definitions"]:
        a = teams[offense]["metrics"][metric]
        d = teams[defense]["metrics"][metric]
        compared[metric] = {
            "offense": {
                "team_id": offense_team_id, "raw": a["raw_offense"],
                "adjusted": a["adjusted_offense"],
                "national_percentile": a["offense_percentile"],
                "reliability": a["reliability"], "status": a["status"],
            },
            "defense": {
                "team_id": defense_team_id, "raw_allowed": d["raw_defense_allowed"],
                "adjusted_allowed": d["adjusted_defense_allowed"],
                "national_percentile": d["defense_percentile"],
                "reliability": d["reliability"], "status": d["status"],
            },
            "projection": None,
            "interpretation": "Observed opposing units only, not a forecast",
        }
    return {
        "season": bundle["season"], "as_of_exclusive": bundle["as_of_exclusive"],
        "offense_team": {"id": offense_team_id, "name": teams[offense]["name"]},
        "defense_team": {"id": defense_team_id, "name": teams[defense]["name"]},
        "metrics": compared,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--as-of", required=True)
    p.add_argument("--team-id", action="append", type=int)
    p.add_argument("--min-population", type=int, default=DEFAULT_MIN_POPULATION)
    p.add_argument("--out", required=True, help="Research JSON path, never ui/data")
    a = p.parse_args()
    with sqlite3.connect(f"file:{Path(a.db).resolve().as_posix()}?mode=ro",
                         uri=True) as db:
        data = build_descriptive_contract(
            db, season=a.season, as_of=a.as_of,
            team_ids=tuple(a.team_id) if a.team_id else None,
            min_population=a.min_population)
    path = Path(a.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(f"Research descriptive contract: {path} ({len(data['teams'])} teams)")


if __name__ == "__main__":
    main()
