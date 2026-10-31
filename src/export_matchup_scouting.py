#!/usr/bin/env python3
"""Produce UI-independent, versioned scouting snapshots and two-way matchups.

This is an *offline* developer export, not a dashboard integration. Output is
JSON to an explicitly chosen path and never modifies Elocast's production UI.
Unavailable metrics stay null; there are no fabricated national percentiles.
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


VERSION = "matchup-scouting-v1"
MIN_RATED_TEAMS = 30


def build_scouting_bundle(
    conn: sqlite3.Connection, *, season: int, as_of: str,
    home_team_id: int | None = None, away_team_id: int | None = None,
    metrics: tuple[str, ...] | None = None,
    min_rated_teams: int = MIN_RATED_TEAMS,
) -> dict:
    """Create a front-end-ready *data contract*, without choosing its design.

    Each metric remains explicitly unavailable if too few eligible FBS teams
    are rated. Estimated offense home/away effects use the common fitted venue
    coefficient, and the result carries a calibration warning.
    """
    if (home_team_id is None) != (away_team_id is None):
        raise ValueError("Both teams must be supplied for a matchup")
    if min_rated_teams < 1:
        raise ValueError("min_rated_teams must be >= 1")
    selected = tuple(METRICS) if metrics is None else tuple(metrics)
    if len(set(selected)) != len(selected):
        raise ValueError("Duplicate metric")
    invalid = set(selected) - METRICS.keys()
    if invalid:
        raise ValueError(f"Unknown metrics: {sorted(invalid)}")
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    if home_team_id is not None:
        if home_team_id == away_team_id or home_team_id not in names or away_team_id not in names:
            raise ValueError("Matchup requires two distinct existing teams")
    metrics_out = {}
    for metric in selected:
        snapshot = build_snapshot(conn, season=season, as_of=as_of, metric=metric)
        profiles = snapshot["profiles"]
        valid = len(profiles) >= min_rated_teams
        reason = ("insufficient_fbs_coverage" if not valid else None)
        record = {
            "status": "available" if valid else "unavailable",
            "reason": reason,
            "calibration": (
                "existing_play_weighted_settings"
                if metric in ("success_rate", "ppa") and snapshot.get("weighting") == "offensive_plays"
                else "provisional"
            ),
            "baseline": snapshot.get("baseline") if valid else None,
            "rated_teams": len(profiles),
            "coverage": snapshot["coverage"],
            "team_profiles": profiles if valid else {},
        }
        if home_team_id is not None:
            home, away = str(home_team_id), str(away_team_id)
            if valid and home in profiles and away in profiles:
                mean = snapshot["baseline"]
                h = profiles[home]
                a = profiles[away]
                # A single fitted home effect applies with opposing signs
                # (+.5 to home offense, -.5 to visitor offense).
                record["matchup"] = {
                    "home_offense_vs_away_defense": {
                        "estimate": mean + h["offense"] + a["defense_allowed"] + .5 * h["home_effect"],
                        "offense_effect": h["offense"],
                        "opponent_defense_allowance": a["defense_allowed"],
                        "venue_adjustment": .5 * h["home_effect"],
                    },
                    "away_offense_vs_home_defense": {
                        "estimate": mean + a["offense"] + h["defense_allowed"] - .5 * h["home_effect"],
                        "offense_effect": a["offense"],
                        "opponent_defense_allowance": h["defense_allowed"],
                        "venue_adjustment": -.5 * h["home_effect"],
                    },
                }
            else:
                record["matchup"] = None
        metrics_out[metric] = record
    return {
        "schema_version": VERSION,
        "season": season, "as_of": as_of,
        "data_kind": "experimental_pregame_efficiency_not_win_probability",
        "home_team": None if home_team_id is None else {"id": home_team_id, "name": names[home_team_id]},
        "away_team": None if away_team_id is None else {"id": away_team_id, "name": names[away_team_id]},
        "metrics": metrics_out,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="db/league.db")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--home-team-id", type=int)
    parser.add_argument("--away-team-id", type=int)
    parser.add_argument("--metric", action="append", choices=sorted(METRICS))
    parser.add_argument("--min-rated-teams", type=int, default=MIN_RATED_TEAMS)
    parser.add_argument("--out", required=True, help="Explicit research artifact path (not ui/data)")
    args = parser.parse_args()
    db = Path(args.db).resolve()
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as conn:
        result = build_scouting_bundle(
            conn, season=args.season, as_of=args.as_of, home_team_id=args.home_team_id,
            away_team_id=args.away_team_id, metrics=args.metric,
            min_rated_teams=args.min_rated_teams)
    target = Path(args.out)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote research scouting data: {target}")


if __name__ == "__main__":
    main()
