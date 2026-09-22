#!/usr/bin/env python3
"""
Standalone Elo rating engine -- CoE 2.0, Phase 1 (see project design doc,
"CoE 2.0" proposal).

DELIBERATELY ISOLATED from the existing CoE pipeline: reads only from the
games table, writes only to elo_game_history (sql/elo_tables.sql). Does
not touch build_coefficients.py, its tables, or any of its outputs.
Nothing about the live dashboard or playoff selection changes by running
this -- it's a parallel, standalone model until a later, explicit
integration step (build_hybrid_coefficients.py).

Formulas:
  Pregame effective rating: R* = R + H
    (H = home_field, added to the home team, only when the game is not
    at a neutral site; 0 otherwise)
  Expected result: E_A = 1 / (1 + 10^((R*_B - R*_A) / scale))
  Actual result S: 1 (win), 0.5 (tie), 0 (loss) -- an OT loss is NOT
    special here (S=0, same as a regulation loss). That distinction is
    reserved for the future CoE 2.0 layer (Game CoE), not Elo -- Elo
    only asks who won.
  Margin-of-victory multiplier:
    M = ln(|point_diff| + 1) * (mov_c / (mov_c + mov_d * winner_advantage))
    where winner_advantage = the WINNING team's effective rating minus
    the LOSING team's effective rating, before the game -- signed, so
    an upset win (negative advantage) produces a LARGER multiplier, not
    a smaller one. This mirrors the well-known 538 NFL Elo MOV formula.
  Rating change: delta = K * (S_A - E_A) * M, applied zero-sum (the
    winner's gain always exactly equals the loser's loss).
  Offseason regression, applied once per team before that team's first
    game of a new season:
    R_new = initial_rating + offseason_retention * (R_old - initial_rating)

All numeric constants live in config/model_config.json's "elo" section,
not hardcoded here -- see that file's _comment for calibration status
(none of these values are backtested yet; that's calibrate_elo.py, a
deliberately separate, later step).

Usage:
    python src/build_elo.py [--db db/league.db] [--config config/model_config.json]
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def effective_rating(rating: float, is_home: bool, neutral_site: bool, home_field: float) -> float:
    if neutral_site or not is_home:
        return rating
    return rating + home_field


def expected_result(eff_a: float, eff_b: float, scale: float) -> float:
    return 1.0 / (1.0 + 10 ** ((eff_b - eff_a) / scale))


def mov_multiplier(point_diff: int, winner_advantage: float, mov_c: float, mov_d: float) -> float:
    """
    point_diff: absolute point differential (0 for a tie).
    winner_advantage: winner's effective rating minus loser's effective
    rating, BEFORE the game -- signed, negative for an upset.

    NOTE: for a tie (point_diff=0), ln(0+1)=0, so M=0 and the rating
    change is zero regardless of K*(S-E). Ties have been structurally
    impossible in FBS since the 1996 overtime rule, so this never
    triggers on the current 2000-2026 dataset -- but would need
    explicit handling before this script is ever run against
    pre-1996 data.
    """
    return math.log(abs(point_diff) + 1) * (mov_c / (mov_c + mov_d * winner_advantage))


def ensure_schema(conn: sqlite3.Connection, schema_path: str) -> None:
    conn.executescript(Path(schema_path).read_text())


def fetch_games_chronological(conn: sqlite3.Connection):
    return conn.execute(
        """
        SELECT g.game_id, g.season_year, g.game_date, g.home_team_id, g.away_team_id,
               g.home_score, g.away_score, g.neutral_site, g.went_ot,
               ht.team_name AS home_name, at.team_name AS away_name
        FROM games g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY g.season_year, g.game_date, g.game_id
        """
    ).fetchall()


def run_elo(games, cfg: dict):
    """
    Pure function (no DB writes): given chronologically-ordered game rows
    and an elo config dict, returns (rows_to_insert, final_ratings,
    id_to_name). Separated from main() so tests can call this directly
    against a synthetic game list, without needing a real database.
    """
    initial_rating = cfg["initial_rating"]
    scale = cfg["scale"]
    k = cfg["k"]
    home_field = cfg["home_field"]
    retention = cfg["offseason_retention"]
    mov_c = cfg["mov_c"]
    mov_d = cfg["mov_d"]

    ratings: dict = {}
    current_season = None
    rows = []
    id_to_name = {}

    for g in games:
        season = g["season_year"]
        home_id, away_id = g["home_team_id"], g["away_team_id"]
        id_to_name[home_id] = g["home_name"]
        id_to_name[away_id] = g["away_name"]

        if current_season is None:
            current_season = season
        elif season != current_season:
            # Offseason regression toward the FIXED initial_rating (not a
            # dynamically computed population mean) -- every team ever
            # seen gets regressed once, before this new season's first game.
            for team_id in ratings:
                ratings[team_id] = initial_rating + retention * (ratings[team_id] - initial_rating)
            current_season = season

        ratings.setdefault(home_id, initial_rating)
        ratings.setdefault(away_id, initial_rating)

        r_home, r_away = ratings[home_id], ratings[away_id]
        neutral = bool(g["neutral_site"])

        eff_home = effective_rating(r_home, True, neutral, home_field)
        eff_away = effective_rating(r_away, False, neutral, home_field)

        e_home = expected_result(eff_home, eff_away, scale)
        e_away = 1.0 - e_home

        hs, aws = g["home_score"], g["away_score"]
        if hs > aws:
            s_home, s_away = 1.0, 0.0
        elif aws > hs:
            s_home, s_away = 0.0, 1.0
        else:
            s_home, s_away = 0.5, 0.5

        point_diff = abs(hs - aws)
        if hs == aws:
            winner_advantage = 0.0  # no winner, so "winner's advantage" is meaningless -- unused
        elif hs > aws:
            winner_advantage = eff_home - eff_away
        else:
            winner_advantage = eff_away - eff_home

        if hs == aws:
            # A tie has no margin at all (point_diff=0), so the margin
            # formula's ln(0+1)=0 isn't a real judgment that upsets don't
            # matter for ties -- it's an incidental artifact of a formula
            # built to scale a margin that doesn't exist here. Left as
            # M=0, a tie would ALWAYS produce zero rating change for
            # either team regardless of how surprising it was (e.g. a
            # heavy underdog tying a top team should still move ratings
            # somewhat). Using M=1.0 (no margin scaling, since there's
            # no margin to scale by) lets K*(S-E) drive the change
            # directly, same as any other result. Ties were structurally
            # impossible in the 2000-2026 dataset (post-1996 overtime
            # rule) so this never mattered before extending back further.
            m = 1.0
        else:
            m = mov_multiplier(point_diff, winner_advantage, mov_c, mov_d)

        delta_home = k * (s_home - e_home) * m
        # Zero-sum by construction: away's change is exactly -delta_home

        new_r_home = r_home + delta_home
        new_r_away = r_away - delta_home

        rows.append((g["game_id"], home_id, r_home, r_away, e_home, m, delta_home, new_r_home))
        rows.append((g["game_id"], away_id, r_away, r_home, e_away, m, -delta_home, new_r_away))

        ratings[home_id] = new_r_home
        ratings[away_id] = new_r_away

    return rows, ratings, id_to_name


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--config", default="config/model_config.json")
    p.add_argument("--schema", default="sql/elo_tables.sql")
    args = p.parse_args()

    cfg = load_config(args.config)["elo"]

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn, args.schema)
    conn.execute("DELETE FROM elo_game_history")  # idempotent full rebuild

    games = fetch_games_chronological(conn)
    rows, ratings, id_to_name = run_elo(games, cfg)

    conn.executemany(
        """
        INSERT INTO elo_game_history
            (game_id, team_id, pregame_elo, opponent_pregame_elo,
             elo_expectation, mov_multiplier, elo_change, postgame_elo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    print(f"Processed {len(games)} games, {len(ratings)} teams.")
    print("Final Elo top 10:")
    for team_id, rating in sorted(ratings.items(), key=lambda x: -x[1])[:10]:
        print(f"  {id_to_name.get(team_id, team_id):<20} {rating:.1f}")

    conn.close()


if __name__ == "__main__":
    main()
