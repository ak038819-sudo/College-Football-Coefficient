#!/usr/bin/env python3
"""
Hybrid Rating + Game CoE 2.0 engine (CoE 2.0, Phase 4).

Bridges the standalone Elo engine (build_elo.py, elo_game_history) and
the existing CoE v1 per-season ratings (build_coefficients.py,
team_ratings_by_season.csv) into:

  1. Frozen entering-season 5yr CoE per team (team_coe_5yr_by_season) --
     season Y uses ONLY completed seasons Y-5..Y-1, never Y itself.
  2. Hybrid Opponent Strength / Hybrid Rating / Hybrid expectation, per
     game, per team (hybrid_game_ratings).
  3. Game CoE 2.0 (BaseResult + DifficultyBonus), written ALONGSIDE the
     existing CoE v1 pipeline. v1's build_coefficients.py is completely
     untouched -- still runs, still feeds the live dashboard. Nothing
     here overwrites anything v1 produces.

Anti-circularity (spec section 17), enforced structurally, not by
convention:
  - Frozen 5yr CoE for season Y is computed ONLY from
    team_ratings_by_season.csv rows where season_year < Y. There is no
    code path by which season Y's own results can enter its own frozen
    5yr CoE.
  - Elo Z uses PREGAME elo only (elo_game_history.pregame_elo /
    opponent_pregame_elo), never postgame_elo -- a game's own result
    cannot influence the opponent-strength rating used to score that
    same game.

DELIBERATE simplification (documented, not silent): Elo Z-scores
standardize a game's pregame Elo against the population of ALL pregame
Elo values recorded for games in the SAME SEASON, not a continuously
updated week-by-week population. This still captures most of the
intended "current strength" signal (Elo itself updates every game)
while keeping standardization simple and auditable. A finer-grained
(weekly) population is a possible later refinement, not applied here.

Bootstrap era (spec section 27): a season with fewer than 5 completed
prior seasons in the dataset has no frozen 5yr CoE yet, so Game CoE 2.0
is skipped for it (Elo itself is built for every season regardless --
this only affects the CoE/hybrid layer). On the current 2000-2026
dataset that means 2000-2004 are bootstrap-only; 2005 onward gets the
full hybrid treatment.

Ties (spec section 14): game_coe is written as NULL for a tie -- ties
have been structurally impossible in FBS since the 1996 overtime rule,
so this is a documented non-issue for 2000-2026, not a real gap.

Usage:
    python src/build_hybrid_coefficients.py [--db db/league.db] [--config config/model_config.json]
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def load_team_ratings_by_season(csv_path: str) -> dict:
    """Returns {(season_year, team_name): rating} from the existing CoE v1 output."""
    ratings = {}
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            ratings[(int(row["season_year"]), row["team_name"])] = float(row["rating"])
    return ratings


def compute_frozen_5yr_coe(team_ratings: dict, years: list, decay_base: float, rolling_years: int = 5) -> dict:
    """
    Returns {(season_year, team_name): frozen_5yr_coe}, using ONLY
    seasons strictly before season_year (Y-rolling_years .. Y-1). Same
    decay-weighting shape as CoE v1's rolling window (freshest prior
    season weighted highest) -- anchored at Y-1 (the most recent
    COMPLETED season) rather than Y itself, since Y is excluded here.
    """
    result = {}
    teams_by_year = defaultdict(set)
    for (yr, team) in team_ratings:
        teams_by_year[yr].add(team)

    for y in years:
        window_years = [yy for yy in range(y - rolling_years, y) if yy in teams_by_year]
        if len(window_years) < rolling_years:
            continue  # bootstrap era -- not enough prior history yet

        anchor = y - 1
        teams = set()
        for wy in window_years:
            teams |= teams_by_year[wy]

        for team in teams:
            total = 0.0
            for wy in window_years:
                age = anchor - wy
                total += team_ratings.get((wy, team), 0.0) * (decay_base ** age)
            result[(y, team)] = total

    return result


def zscore_stats(values: list) -> tuple:
    if len(values) < 2:
        return 0.0, 1.0
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    return mean, (stdev if stdev != 0 else 1.0)


def tie_game_coe(p_this: float, tie_delta: float) -> float:
    """
    Spec section 14's own suggested starting formula for a tie: symmetric
    around 1.0 (halfway between a loss's 0 and roughly the low end of a
    win's range), using SIGNED deviation from 0.5 since a tie has no
    winner to disproportionately reward -- an underdog (p_this < 0.5)
    tying a favorite gets a BONUS (> 1); a favorite (p_this > 0.5)
    settling for a tie gets a mild PENALTY (< 1). tie_delta is a
    genuinely separate tunable from alpha (the spec uses a distinct
    symbol) -- not yet independently calibrated.
    """
    return 1.0 + tie_delta * (0.5 - p_this)


def compute_hybrid_rows(elo_rows, frozen_5yr: dict, team_id_to_name: dict, elo_home_field: float,
                         hybrid_cfg: dict, coe_cfg: dict):
    """
    Pure function (no DB writes): given elo_game_history rows (joined
    with games), the frozen 5yr CoE map, and config, returns the list of
    hybrid_rows tuples plus a skipped-bootstrap count. Shared by
    build_hybrid_coefficients.py (writes these to the DB) and
    calibrate_hybrid_weight.py (scores them against actual results) --
    kept as ONE function so a future fix (like the home-field asymmetry
    bug already caught once) can't silently diverge between two
    unsynced copies of the same logic.
    """
    elo_by_season = defaultdict(list)
    for r in elo_rows:
        elo_by_season[r["season_year"]].append(r["pregame_elo"])
    elo_pop_stats = {season: zscore_stats(vals) for season, vals in elo_by_season.items()}

    coe_by_season = defaultdict(list)
    for (y, team), val in frozen_5yr.items():
        coe_by_season[y].append(val)
    coe_pop_stats = {season: zscore_stats(vals) for season, vals in coe_by_season.items()}

    elo_weight = hybrid_cfg["elo_weight"]
    coe_weight = hybrid_cfg["coe_weight"]
    rating_scale = hybrid_cfg["rating_scale"]
    win_base = coe_cfg["win_base"]
    ot_loss_coe = coe_cfg["ot_loss"]
    reg_loss_coe = coe_cfg["regulation_loss"]
    alpha = coe_cfg["difficulty_alpha"]
    tie_delta = coe_cfg.get("tie_delta", alpha)  # spec section 14's own suggested starting point: reuse alpha's value

    hybrid_rows = []
    skipped_bootstrap = 0

    for r in elo_rows:
        game_id, team_id = r["game_id"], r["team_id"]
        season = r["season_year"]
        team_name = team_id_to_name.get(team_id)
        is_home = (team_id == r["home_team_id"])
        opp_id = r["away_team_id"] if is_home else r["home_team_id"]
        opp_name = team_id_to_name.get(opp_id)

        frozen_coe = frozen_5yr.get((season, team_name))
        opp_frozen_coe = frozen_5yr.get((season, opp_name))
        if frozen_coe is None or opp_frozen_coe is None:
            skipped_bootstrap += 1
            continue

        elo_mean, elo_sd = elo_pop_stats[season]
        coe_mean, coe_sd = coe_pop_stats[season]

        elo_z = (r["pregame_elo"] - elo_mean) / elo_sd
        coe_z = (frozen_coe - coe_mean) / coe_sd
        os_strength = elo_weight * elo_z + coe_weight * coe_z
        hybrid_rating = 1500 + rating_scale * os_strength

        opp_elo_z = (r["opponent_pregame_elo"] - elo_mean) / elo_sd
        opp_coe_z = (opp_frozen_coe - coe_mean) / coe_sd
        opp_os = elo_weight * opp_elo_z + coe_weight * opp_coe_z
        opp_hybrid_rating = 1500 + rating_scale * opp_os

        # Apply the home-field boost to EFFECTIVE ratings first (to
        # whichever side is actually home), THEN run the plain logistic
        # with no further adjustment -- mirrors build_elo.py's
        # effective_rating() pattern exactly. A first version added the
        # boost inside the exponent only for the home team's own row
        # while treating the away team's row as if h=0, which is NOT
        # equivalent (the away team's own expectation must reflect that
        # the OPPONENT, not itself, receives the boost) -- that bug was
        # caught by test_hybrid_expectations_sum_to_one_per_game, which
        # found 7576 of 8231 games violating P_home + P_away = 1 before
        # this fix.
        neutral = bool(r["neutral_site"])
        if neutral:
            eff_rating, eff_opp_rating = hybrid_rating, opp_hybrid_rating
        elif is_home:
            eff_rating, eff_opp_rating = hybrid_rating + elo_home_field, opp_hybrid_rating
        else:
            eff_rating, eff_opp_rating = hybrid_rating, opp_hybrid_rating + elo_home_field

        p_this = 1.0 / (1.0 + 10 ** ((eff_opp_rating - eff_rating) / 400))

        hs, aws = r["home_score"], r["away_score"]
        team_score = hs if is_home else aws
        opp_score = aws if is_home else hs
        # KNOWN DATA GAP (documented, not fixed in this pass): went_ot is
        # currently 0 for every game in the games table. Root cause:
        # CFBD's basic /games endpoint doesn't appear to expose an
        # overtime field directly -- fetch_cfbd_games.py has been
        # searching for one ("overtime"/"overtimes"/"overTime") that
        # likely doesn't exist on that endpoint. Determining real OT
        # status would need either a different endpoint (period-by-period
        # line scores) or parsing free-text game notes -- deliberately
        # deferred, not solved here. Practical effect: OT_LOSS never
        # actually triggers yet, so every loss currently scores as a
        # plain LOSS (game_coe=0) even ones that were real OT losses.
        went_ot = bool(r["went_ot"])

        if team_score > opp_score:
            result_type = "OT_WIN" if went_ot else "WIN"
            game_coe = win_base + alpha * (1 - p_this)
        elif team_score < opp_score:
            if went_ot:
                result_type, game_coe = "OT_LOSS", ot_loss_coe
            else:
                result_type, game_coe = "LOSS", reg_loss_coe
        else:
            result_type = "TIE"
            game_coe = tie_game_coe(p_this, tie_delta)

        hybrid_rows.append((game_id, team_id, elo_z, coe_z, os_strength, hybrid_rating, p_this, result_type, game_coe))

    return hybrid_rows, skipped_bootstrap


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--config", default="config/model_config.json")
    p.add_argument("--schema", default="sql/hybrid_tables.sql")
    p.add_argument("--team-ratings-csv", default="data/processed/team_ratings_by_season.csv")
    args = p.parse_args()

    cfg = load_config(args.config)
    elo_home_field = cfg["elo"]["home_field"]
    hybrid_cfg = cfg["hybrid"]
    coe_cfg = cfg["coe"]
    decay_base = 0.92  # matches CoE v1's WITHIN_WINDOW_DECAY_BASE, kept consistent intentionally

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.executescript(Path(args.schema).read_text())
    conn.execute("DELETE FROM team_coe_5yr_by_season")
    conn.execute("DELETE FROM hybrid_game_ratings")

    team_ratings = load_team_ratings_by_season(args.team_ratings_csv)
    years = sorted(set(yr for (yr, _) in team_ratings))

    frozen_5yr = compute_frozen_5yr_coe(team_ratings, years, decay_base)

    team_name_to_id = {row["team_name"]: row["team_id"] for row in conn.execute("SELECT team_id, team_name FROM teams")}
    team_id_to_name = {v: k for k, v in team_name_to_id.items()}

    frozen_rows = [
        (team_name_to_id[team], y, val)
        for (y, team), val in frozen_5yr.items()
        if team in team_name_to_id
    ]
    conn.executemany(
        "INSERT INTO team_coe_5yr_by_season (team_id, season_year, coe_5yr) VALUES (?, ?, ?)",
        frozen_rows,
    )
    conn.commit()
    seasons_covered = sorted(set(y for y, _ in frozen_5yr)) if frozen_5yr else []
    if seasons_covered:
        print(f"Wrote {len(frozen_rows)} frozen entering-season 5yr CoE rows "
              f"(seasons {seasons_covered[0]}-{seasons_covered[-1]})")
    else:
        print("No frozen 5yr CoE computed")

    elo_rows = conn.execute(
        """
        SELECT e.game_id, e.team_id, e.pregame_elo, e.opponent_pregame_elo,
               g.season_year, g.home_team_id, g.away_team_id,
               g.home_score, g.away_score, g.neutral_site, g.went_ot
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        ORDER BY g.season_year, g.game_date, g.game_id
        """
    ).fetchall()

    hybrid_rows, skipped_bootstrap = compute_hybrid_rows(
        elo_rows, frozen_5yr, team_id_to_name, elo_home_field, hybrid_cfg, coe_cfg
    )

    conn.executemany(
        """
        INSERT INTO hybrid_game_ratings
            (game_id, team_id, elo_z, coe_z, hybrid_strength, hybrid_rating,
             hybrid_expectation, result_type, game_coe)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        hybrid_rows,
    )
    conn.commit()

    print(f"Wrote {len(hybrid_rows)} hybrid game-rating rows "
          f"({skipped_bootstrap} team-game rows skipped -- bootstrap era, no frozen 5yr CoE yet)")

    win_base = coe_cfg["win_base"]
    alpha = coe_cfg["difficulty_alpha"]
    win_coes = [row[8] for row in hybrid_rows if row[8] is not None and row[7] in ("WIN", "OT_WIN")]
    if win_coes:
        print(f"Win CoE range this run: min={min(win_coes):.3f} max={max(win_coes):.3f} "
              f"(spec requires >= {win_base} always, < {win_base + 2 * alpha} asymptotically)")

    conn.close()
    print("\ncompare_models.py (a separate, later step) builds the season/conference CoE 2.0")
    print("rollup and the side-by-side comparison against CoE v1.")


if __name__ == "__main__":
    main()
