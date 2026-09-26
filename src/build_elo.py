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
  Performance multiplier M: chosen by config/model_config.json's "performance"
    section and computed by src/srdiff.py, so the engine never knows which
    variant it is running. The default is the margin-of-victory multiplier:
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
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from srdiff import (XsrModel, actual_sr_diff, build_layer, expected_sr_diff,  # noqa: E402
                    load_performance_config, mov_multiplier, sr_plus)
# mov_multiplier is re-exported (it used to live here) so existing callers and
# tests keep importing it from build_elo; src/srdiff.py is now its one definition.
__all__ = ["run_elo", "effective_rating", "expected_result", "mov_multiplier", "GameContext"]


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def effective_rating(rating: float, is_home: bool, neutral_site: bool, home_field: float) -> float:
    if neutral_site or not is_home:
        return rating
    return rating + home_field


def expected_result(eff_a: float, eff_b: float, scale: float) -> float:
    return 1.0 / (1.0 + 10 ** ((eff_b - eff_a) / scale))


@dataclass(frozen=True)
class GameContext:
    """Everything a performance layer may look at, all of it PREGAME state plus
    the final score. Deliberately narrow: a layer cannot reach postgame Elo,
    later games, or anything the model did not know at kickoff."""
    season: int
    is_tie: bool
    point_diff: int
    winner_advantage: float              # winner's effective rating minus loser's, pregame
    winner_elo_diff_adjusted: float      # same quantity, named for the xSRDiff curve
    winner_sr: float | None              # winner's Success Rate in THIS game
    loser_sr: float | None


# Columns the xSRDiff layer added to elo_game_history (EXP-03). Parsed from the
# schema file rather than restated, so the two can never drift.
def _optional_columns(schema_sql: str) -> list[tuple[str, str]]:
    body = schema_sql.split("CREATE TABLE IF NOT EXISTS elo_game_history", 1)[-1]
    body = body.split(");", 1)[0]
    out = []
    for line in body.splitlines():
        line = line.split("--", 1)[0].strip().rstrip(",")
        m = re.match(r"^(\w+)\s+(REAL|TEXT|INTEGER)$", line)   # NOT NULL columns are original
        if m:
            out.append((m.group(1), m.group(2)))
    return out


def ensure_schema(conn: sqlite3.Connection, schema_path: str) -> None:
    """
    Create the table, then add any column an older database is missing.

    A plain CREATE TABLE IF NOT EXISTS silently leaves an existing table on its
    old shape, so a database built before the performance layer would keep
    failing the INSERT. ALTER TABLE ADD COLUMN is cheap and idempotent here
    because every added column is nullable.
    """
    sql = Path(schema_path).read_text()
    conn.executescript(sql)
    have = {r[1] for r in conn.execute("PRAGMA table_info(elo_game_history)")}
    for name, decl in _optional_columns(sql):
        if name not in have:
            conn.execute(f"ALTER TABLE elo_game_history ADD COLUMN {name} {decl}")
    conn.commit()


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


def load_game_success_rates(conn: sqlite3.Connection) -> dict:
    """
    {(game_id, team_id): offensive Success Rate for THAT game}.

    Per-game, never per-season: a season figure includes the game itself and
    every game after it, so using it here would leak the future into a rating
    the model is supposed to have formed before kickoff. Empty until
    src/load_game_advanced.py has run, which is the whole point of the
    fallback path.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='game_team_advanced'").fetchone():
        return {}
    return {(g, t): sr for g, t, sr in conn.execute(
        "SELECT game_id, team_id, off_success_rate FROM game_team_advanced WHERE off_success_rate IS NOT NULL")}


def run_elo(games, cfg: dict, layer=None, success_rates: dict | None = None):
    """
    Pure function (no DB writes): given chronologically-ordered game rows
    and an elo config dict, returns (rows_to_insert, final_ratings,
    id_to_name). Separated from main() so tests can call this directly
    against a synthetic game list, without needing a real database.

    `layer` is the performance layer that supplies M (src/srdiff.py). Omitted,
    it is the margin-of-victory layer -- so every existing caller and test
    keeps the exact behavior it had before the performance layer existed.
    `success_rates` is {(game_id, team_id): Success Rate} for the xSRDiff and
    raw-SRDiff variants; the MOV and result-only layers never read it.
    """
    initial_rating = cfg["initial_rating"]
    scale = cfg["scale"]
    k = cfg["k"]
    home_field = cfg["home_field"]
    retention = cfg["offseason_retention"]
    if layer is None:
        layer = build_layer({"modifier": "mov", "fallback": "mov", "beta": 1.0,
                             "m_min": 0.5, "m_max": 1.5, "model_path": None}, cfg)
    sr_of = success_rates or {}

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

        # One M per GAME, from the winner's point of view. It multiplies a
        # zero-sum delta, so a per-team M would break the zero-sum property
        # (see src/srdiff.py for why the winner's SR+ is the right side).
        is_tie = hs == aws
        home_won = hs > aws
        winner_id, loser_id = (home_id, away_id) if home_won else (away_id, home_id)
        gid = g["game_id"]
        ctx = GameContext(
            season=season, is_tie=is_tie, point_diff=point_diff,
            winner_advantage=winner_advantage,
            # Venue-adjusted pregame Elo difference, winner minus loser.
            winner_elo_diff_adjusted=winner_advantage,
            winner_sr=None if is_tie else sr_of.get((gid, winner_id)),
            loser_sr=None if is_tie else sr_of.get((gid, loser_id)),
        )
        m, model_used = layer.multiplier(ctx)

        delta_home = k * (s_home - e_home) * m
        # Zero-sum by construction: away's change is exactly -delta_home

        new_r_home = r_home + delta_home
        new_r_away = r_away - delta_home

        # Per-team analytics. SRDiff/xSRDiff/SR+ are antisymmetric, so each team
        # stores its own signed view of the same game; M is shared.
        home_sr, away_sr = sr_of.get((gid, home_id)), sr_of.get((gid, away_id))
        x_model = getattr(layer, "model", None)
        home_elo_diff = eff_home - eff_away
        per_team = {}
        for tid, own_sr, opp_sr, elo_diff in ((home_id, home_sr, away_sr, home_elo_diff),
                                              (away_id, away_sr, home_sr, -home_elo_diff)):
            d = actual_sr_diff(own_sr, opp_sr)
            x = expected_sr_diff(elo_diff, x_model, season)
            per_team[tid] = (own_sr, opp_sr, elo_diff, d, x, sr_plus(d, x))

        for tid, opp_id, pre, opp_pre, exp, delta, post in (
                (home_id, away_id, r_home, r_away, e_home, delta_home, new_r_home),
                (away_id, home_id, r_away, r_home, e_away, -delta_home, new_r_away)):
            own_sr, opp_sr, elo_diff, d, x, plus = per_team[tid]
            rows.append((gid, tid, pre, opp_pre, exp, m, delta, post,
                         elo_diff, own_sr, opp_sr, d, x, plus, model_used))

        ratings[home_id] = new_r_home
        ratings[away_id] = new_r_away

    return rows, ratings, id_to_name


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--config", default="config/model_config.json")
    p.add_argument("--schema", default="sql/elo_tables.sql")
    args = p.parse_args()

    raw = load_config(args.config)
    cfg = raw["elo"]
    perf = load_performance_config(raw.get("performance"))

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn, args.schema)
    conn.execute("DELETE FROM elo_game_history")  # idempotent full rebuild

    model = XsrModel.load(perf["model_path"]) if perf["model_path"] else None
    success_rates = load_game_success_rates(conn)
    layer = build_layer(perf, cfg, model)

    games = fetch_games_chronological(conn)
    rows, ratings, id_to_name = run_elo(games, cfg, layer, success_rates)

    conn.executemany(
        """
        INSERT INTO elo_game_history
            (game_id, team_id, pregame_elo, opponent_pregame_elo,
             elo_expectation, mov_multiplier, elo_change, postgame_elo,
             elo_diff_adjusted, success_rate_team, success_rate_opp,
             sr_diff, xsr_diff, sr_plus, performance_model)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    used = {}
    for r in rows:
        used[r[14]] = used.get(r[14], 0) + 1
    print(f"Processed {len(games)} games, {len(ratings)} teams.")
    print(f"Performance layer: {perf['modifier']} (fallback {perf['fallback']}); "
          f"per-game Success Rate for {len(success_rates):,} team-games; "
          f"xSRDiff curve: {model.version if model else 'not fitted yet'}")
    for name, n in sorted(used.items(), key=lambda x: -x[1]):
        print(f"  {name:<14} {n // 2:,} games")
    print("Final Elo top 10:")
    for team_id, rating in sorted(ratings.items(), key=lambda x: -x[1])[:10]:
        print(f"  {id_to_name.get(team_id, team_id):<20} {rating:.1f}")

    conn.close()


if __name__ == "__main__":
    main()
