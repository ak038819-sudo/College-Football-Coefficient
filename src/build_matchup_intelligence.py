#!/usr/bin/env python3
"""Read-only, point-in-time CFBD game adapter for experimental scouting profiles.

Only dates *strictly before* the requested calendar date are eligible. This
conservatively excludes all same-day games because the DB does not store
reliable completion timestamps. No production table or file is modified.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date
from pathlib import Path

try:
    from src.matchup_intelligence import Observation, fit_profiles, project_matchup
except ModuleNotFoundError:
    # Support direct invocation: python src/build_matchup_intelligence.py ...
    from matchup_intelligence import Observation, fit_profiles, project_matchup

METRICS = {
    "success_rate": "off_success_rate",
    "ppa": "off_ppa",
    "explosiveness": "off_explosiveness",
    "rush_success_rate": "off_rush_success_rate",
    "rush_ppa": "off_rush_ppa",
    "rush_explosiveness": "off_rush_explosiveness",
    "pass_success_rate": "off_pass_success_rate",
    "pass_ppa": "off_pass_ppa",
    "pass_explosiveness": "off_pass_explosiveness",
}


def load_observations(
    conn: sqlite3.Connection, *, season: int, as_of: str, metric: str
) -> tuple[list[Observation], dict]:
    """Retrieve one offensive team-game observation for each eligible row."""
    if metric not in METRICS:
        raise ValueError(f"Unknown metric: {metric}")
    cutoff = date.fromisoformat(as_of)
    if season < 2001:
        raise ValueError("Per-game CFBD advanced coverage begins in 2001")
    # Explicit data-contract checks. Never silently run on incomplete schemas.
    required = ("games", "teams", "game_team_advanced", "team_membership_by_season")
    existing = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = set(required) - existing
    if missing:
        raise ValueError(f"Missing database tables: {sorted(missing)}")
    column = METRICS[metric]  # identifier comes from a closed allowlist, not user SQL
    available_columns = {r[1] for r in conn.execute(
        "PRAGMA table_info(game_team_advanced)")}
    if column not in available_columns:
        raise ValueError(f"Metric {metric} requires refreshed advanced-game schema")
    # Team identity is canonical team_id (not ambiguous names). Both teams must
    # have an explicit FBS flag in the season being evaluated.
    candidates = conn.execute(f"""
        SELECT g.game_id, g.home_team_id, g.away_team_id, a.team_id,
               a.{column} AS metric_value, a.season_year AS metric_season,
               g.game_date, g.season_year, g.neutral_site,
        FROM games AS g
        JOIN game_team_advanced AS a ON a.game_id = g.game_id
        JOIN team_membership_by_season AS mh
          ON mh.team_id = g.home_team_id AND mh.season_year = g.season_year
        JOIN team_membership_by_season AS ma
          ON ma.team_id = g.away_team_id AND ma.season_year = g.season_year
        WHERE g.season_year = ?
          AND mh.is_fbs = 1 AND ma.is_fbs = 1
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY g.game_date, g.game_id, a.team_id
    """, (season,)).fetchall()
    report = {"candidate_rows": len(candidates), "missing_value": 0,
              "missing_or_invalid_date": 0, "future_or_same_day": 0,
              "season_mismatch": 0, "invalid_team_side": 0,
              "duplicate_team_games": 0, "eligible_rows": 0}
    observations: list[Observation] = []
    seen: set[tuple[int, int]] = set()
    for game_id, home, away, team, value, metric_season, game_day, game_season, neutral in candidates:
        if metric_season != game_season:
            report["season_mismatch"] += 1
            continue
        if team not in (home, away):
            report["invalid_team_side"] += 1
            continue
        if not game_day:
            report["missing_or_invalid_date"] += 1
            continue
        try:
            # ISO timestamps may include time and offset; only the recorded
            # calendar DATE is used, never a guessed kickoff/completion time.
            recorded_day = date.fromisoformat(str(game_day)[:10])
        except ValueError:
            report["missing_or_invalid_date"] += 1
            continue
        if recorded_day >= cutoff:
            report["future_or_same_day"] += 1
            continue
        if value is None:
            report["missing_value"] += 1
            continue
        pair = (game_id, team)
        if pair in seen:
            report["duplicate_team_games"] += 1
            continue
        seen.add(pair)
        opponent = away if team == home else home
        observations.append(Observation(\n            str(team), str(opponent), float(value),\n            side=0.0 if neutral else (0.5 if team == home else -0.5)))
    report["eligible_rows"] = len(observations)
    report["eligible_games"] = len({game_id for game_id, _team_id in seen})
    return observations, report


def build_snapshot(
    conn: sqlite3.Connection, *, season: int, as_of: str, metric: str,
    offensive_team_id: int | None = None, defensive_team_id: int | None = None,
) -> dict:
    """Produce a JSON-ready read-only scouting snapshot or matchup estimate."""
    rows, coverage = load_observations(conn, season=season, as_of=as_of, metric=metric)
    if not rows:
        return {"season": season, "as_of": as_of, "metric": metric,
                "coverage": coverage, "status": "no_data", "profiles": {}}
    grades, baseline = fit_profiles(rows)
    result = {"season": season, "as_of": as_of, "metric": metric,
              "model": "opponent-adjusted-ridge-v1", "coverage": coverage,
              "baseline": baseline,
              "profiles": {key: vars(v) for key, v in sorted(grades.items())}}
    if (offensive_team_id is None) != (defensive_team_id is None):
        raise ValueError("Specify both matchup teams or neither")
    if offensive_team_id is not None:
        result["matchup"] = vars(project_matchup(
            grades, baseline, str(offensive_team_id), str(defensive_team_id)))
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--as-of", required=True, help="ISO calendar date YYYY-MM-DD")
    p.add_argument("--metric", choices=sorted(METRICS), default="success_rate")
    p.add_argument("--offense", type=int)
    p.add_argument("--defense", type=int)
    args = p.parse_args()
    # Read-only SQLite connection prevents accidental modifications.
    path = Path(args.db).resolve()
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as conn:
        result = build_snapshot(
            conn, season=args.season, as_of=args.as_of, metric=args.metric,
            offensive_team_id=args.offense, defensive_team_id=args.defense)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
