#!/usr/bin/env python3
"""Read-only, forward-only comparison of flat Elo and team HFA Elo.

Prerequisite: build the flat reference with src/build_elo.py, then run the
full src/build_hfa.py (without --current-only). Those HFA rows are frozen
entering each season and derived from the flat reference. This script never
replaces elo_game_history or any production predictions.

Usage: python src/compare_dynamic_hfa.py --from-year 2018
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path

from build_elo import fetch_games_chronological, load_config, load_game_success_rates, run_elo
from predict_upcoming import CONFIG_PATH, performance_layer
from dynamic_hfa import home_games_from_flat, pregame_bonus_provider


def entering_home_bonuses(conn: sqlite3.Connection, games: list, flat_bonus: float):
    """Return a pre-season-only provider and the count of bounded fallbacks."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='team_hfa_by_season'").fetchone():
        raise ValueError("Build the full flat-reference HFA table with src/build_hfa.py first")
    first_dates = {}
    for g in games:
        season = g["season_year"]
        first_dates[season] = min(first_dates.get(season, g["game_date"]), g["game_date"])
    if not first_dates:
        raise ValueError("No completed games to compare")
    first_season = min(first_dates)
    raw = conn.execute("SELECT season_year, team_id, as_of, elo_hfa_points, elo_points_at_bound "
                       "FROM team_hfa_by_season").fetchall()
    if not raw:
        raise ValueError("The full entering-season HFA table is empty; --current-only is insufficient")
    bonuses = {}
    for season, team, as_of, points, at_bound in raw:
        if season in first_dates and as_of > first_dates[season]:
            raise ValueError(f"HFA estimate for {team} in {season} was made after its first game")
        bonuses[season, team] = (points, bool(at_bound))
    bounded = set()

    def bonus(g):
        season, team = g["season_year"], g["home_team_id"]
        value = bonuses.get((season, team))
        if value is None:
            if season == first_season:  # no earlier national baseline exists
                return flat_bonus
            raise ValueError(f"Missing entering-season HFA for team {team} in {season}")
        points, at_bound = value
        if points is None or at_bound:
            bounded.add((season, team))
            return flat_bonus
        return points

    return bonus, bounded


def verify_flat_reference(conn: sqlite3.Connection, flat_rows: list) -> None:
    """Refuse a comparison if the persisted HFA source is not the flat replay."""
    stored = {(g, t): (pre, exp) for g, t, pre, exp in conn.execute(
        "SELECT game_id, team_id, pregame_elo, elo_expectation FROM elo_game_history")}
    if len(stored) != len(flat_rows) or any(
        (r[0], r[1]) not in stored or abs(stored[r[0], r[1]][0] - r[2]) > 1e-7 or
        abs(stored[r[0], r[1]][1] - r[4]) > 1e-9 for r in flat_rows
    ):
        raise ValueError("elo_game_history is not this flat reference; rebuild flat Elo then full HFA")


def compare(conn: sqlite3.Connection, cfg: dict, from_year: int,
            config_path: Path = CONFIG_PATH, in_season: bool = False) -> dict:
    prev = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        games = fetch_games_chronological(conn)
    finally:
        conn.row_factory = prev
    layer, success_rates = performance_layer(conn, cfg, path=config_path)
    flat_rows = run_elo(games, cfg, layer, success_rates)[0]
    verify_flat_reference(conn, flat_rows)
    if in_season:
        hfa_cfg = json.loads(Path(config_path).read_text())['hfa']
        reference = home_games_from_flat(games, flat_rows, cfg['scale'])
        bonus = pregame_bonus_provider(reference, hfa_cfg, cfg['scale'], cfg['home_field'])
        bounded = set()
    else:
        bonus, bounded = entering_home_bonuses(conn, games, cfg["home_field"])
    dynamic_rows = run_elo(games, cfg, layer, success_rates, home_bonus_for_game=bonus)[0]
    by_season = defaultdict(lambda: {"games": 0, "flat_brier": 0.0, "dynamic_brier": 0.0,
                                     "flat_log_loss": 0.0, "dynamic_log_loss": 0.0})
    for game, flat, dynamic in zip(games, flat_rows[::2], dynamic_rows[::2]):
        if game["season_year"] < from_year:
            continue
        actual = 1.0 if game["home_score"] > game["away_score"] else (
            0.0 if game["home_score"] < game["away_score"] else 0.5)
        rec = by_season[game["season_year"]]
        rec["games"] += 1
        for key, row in (("flat", flat), ("dynamic", dynamic)):
            p = min(1 - 1e-12, max(1e-12, row[4]))
            rec[key + "_brier"] += (p - actual) ** 2
            rec[key + "_log_loss"] -= actual * math.log(p) + (1 - actual) * math.log(1 - p)
    if not by_season:
        raise ValueError(f"No completed games from {from_year} onward")
    total = {key: sum(r[key] for r in by_season.values()) for key in
             ("games", "flat_brier", "dynamic_brier", "flat_log_loss", "dynamic_log_loss")}
    def summarize(rec):
        n = rec["games"]
        return {"games": n, **{k: round(rec[k] / n, 6) for k in
                ("flat_brier", "dynamic_brier", "flat_log_loss", "dynamic_log_loss")}}
    return {"from_year": from_year, "mode": "pregame in-season" if in_season else "entering-season",
            "note": "Diagnostic only; HFA hyperparameters are not calibrated for deployment",
            "bounded_or_missing_point_fallback_teams": len(bounded),
            "overall": summarize(total),
            "by_season": {str(y): summarize(r) for y, r in sorted(by_season.items())}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="db/league.db")
    parser.add_argument("--config", default="config/model_config.json")
    parser.add_argument("--from-year", type=int, default=2018)
    parser.add_argument("--in-season", action="store_true", help="Recalculate team HFA before each game date from flat reference")
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    try:
        result = compare(conn, load_config(args.config)["elo"], args.from_year, Path(args.config), args.in_season)
    finally:
        conn.close()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
