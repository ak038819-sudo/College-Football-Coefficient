#!/usr/bin/env python3
"""
Qualitative alpha comparison tool -- deliberately NOT a grid search for
"best" Brier score. Per the design doc's own philosophy (section 32):
CoE measures achievement, not prediction, so difficulty_alpha should be
judged by qualitative criteria (ranking stability, resistance to
cupcake-farming, rewards for elite wins), not optimized against
out-of-sample accuracy the way K/HFA/elo_weight were. This tool just
computes Game CoE 2.0 under each candidate alpha and lays out concrete
things to look at -- it does not recommend a "winner".

Three views, using the design doc's own suggested test grid (section 31:
alpha in {0.5, 1, 1.5, 2, 2.5, 3}):

  1. Win CoE distribution per alpha -- how much does raising alpha
     stretch the gap between an expected win (near the floor) and a
     stunning upset (near the ceiling)?
  2. Season CoE 2.0 ranking reordering for one complete season -- how
     much does the TOP of the standings actually reshuffle as alpha
     changes? (Ranking stability.)
  3. The single biggest upset in the dataset -- a concrete, inspectable
     number: does THIS Game CoE value, for THIS specific historical
     upset, feel right at each alpha?

Does NOT modify config/model_config.json -- purely descriptive output
for you to react to.

Usage:
    python src/compare_alpha.py [--db db/league.db] [--config config/model_config.json] [--year 2025]
"""
from __future__ import annotations

import argparse
import sqlite3
import statistics
from collections import defaultdict

from build_elo import load_config
from build_hybrid_coefficients import load_team_ratings_by_season, compute_frozen_5yr_coe, compute_hybrid_rows

ALPHA_GRID = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--config", default="config/model_config.json")
    p.add_argument("--team-ratings-csv", default="data/processed/team_ratings_by_season.csv")
    p.add_argument("--year", type=int, default=None, help="Season to show ranking reordering for (default: latest complete season with hybrid data)")
    p.add_argument("--top-n", type=int, default=15)
    args = p.parse_args()

    cfg = load_config(args.config)
    elo_home_field = cfg["elo"]["home_field"]
    hybrid_cfg = cfg["hybrid"]
    base_coe_cfg = cfg["coe"]

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    team_ratings = load_team_ratings_by_season(args.team_ratings_csv)
    years = sorted(set(yr for (yr, _) in team_ratings))
    frozen_5yr = compute_frozen_5yr_coe(team_ratings, years, decay_base=0.92)
    team_id_to_name = {row["team_id"]: row["team_name"] for row in conn.execute("SELECT team_id, team_name FROM teams")}

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
    conn.close()

    frozen_years = sorted(set(y for y, _ in frozen_5yr))

    if args.year:
        target_year = args.year
    else:
        # Default to the latest season that looks genuinely complete
        # (max games played across teams >= CONFIDENCE_GAMES), not just
        # the latest year with frozen 5yr CoE data -- an in-progress
        # current season would otherwise silently make View 2 look like
        # alpha "doesn't matter" (every team has 1-2 games, so there's
        # barely any achievement accumulated yet to differentiate by),
        # which is exactly the early-season-noise trap this project has
        # hit multiple times before.
        games_played = defaultdict(int)
        for g in elo_rows:
            games_played[(g["season_year"], g["team_id"])] += 1
        CONFIDENCE_GAMES = 8
        complete_years = [
            y for y in frozen_years
            if max((n for (yr, _), n in games_played.items() if yr == y), default=0) >= CONFIDENCE_GAMES
        ]
        target_year = complete_years[-1] if complete_years else frozen_years[-1]

    # --- View 1: Win CoE distribution per alpha ---
    print("=" * 70)
    print("VIEW 1: Win CoE distribution across the full dataset, per alpha")
    print("=" * 70)
    results_by_alpha = {}
    for alpha in ALPHA_GRID:
        coe_cfg = dict(base_coe_cfg)
        coe_cfg["difficulty_alpha"] = alpha
        hybrid_rows, _ = compute_hybrid_rows(elo_rows, frozen_5yr, team_id_to_name, elo_home_field, hybrid_cfg, coe_cfg)
        results_by_alpha[alpha] = hybrid_rows

        win_coes = [r[8] for r in hybrid_rows if r[7] in ("WIN", "OT_WIN")]
        print(f"  alpha={alpha:<4} min={min(win_coes):.3f}  max={max(win_coes):.3f}  "
              f"mean={statistics.mean(win_coes):.3f}  stdev={statistics.pstdev(win_coes):.3f}")

    print("\n  Higher alpha widens the gap between an expected win (near the floor)")
    print("  and a stunning upset (near the ceiling). A wider spread means a single")
    print("  great win can matter as much as several routine ones; a narrower spread")
    print("  means volume of wins dominates achievement more than their quality.")

    # --- View 2: Season CoE 2.0 ranking reordering ---
    print(f"\n{'=' * 70}")
    print(f"VIEW 2: Top {args.top_n} by Season CoE 2.0 in {target_year}, per alpha")
    print("=" * 70)

    game_year_lookup = {g["game_id"]: g["season_year"] for g in elo_rows}
    for alpha in ALPHA_GRID:
        hybrid_rows = results_by_alpha[alpha]
        season_coe = defaultdict(float)
        for r in hybrid_rows:
            game_id, team_id = r[0], r[1]
            game_coe = r[8]
            if game_coe is None or game_year_lookup.get(game_id) != target_year:
                continue
            season_coe[team_id] += game_coe

        top = sorted(season_coe.items(), key=lambda x: -x[1])[:args.top_n]
        names = [team_id_to_name.get(tid, str(tid)) for tid, _ in top]
        print(f"\n  alpha={alpha}:")
        print("  " + ", ".join(f"{i+1}.{n}" for i, n in enumerate(names)))

    # --- View 3: The single biggest upset ---
    print(f"\n{'=' * 70}")
    print("VIEW 3: Game CoE 2.0 for the single biggest upset in the dataset, per alpha")
    print("=" * 70)

    baseline_rows = results_by_alpha[ALPHA_GRID[len(ALPHA_GRID) // 2]]  # any alpha works to FIND the game; alpha only changes its game_coe, not which game is the biggest upset
    win_rows = [r for r in baseline_rows if r[7] in ("WIN", "OT_WIN")]
    biggest_upset = min(win_rows, key=lambda r: r[6])  # lowest hybrid_expectation among winners = biggest upset
    upset_game_id, upset_team_id = biggest_upset[0], biggest_upset[1]
    upset_p = biggest_upset[6]

    game_lookup = {g["game_id"]: g for g in elo_rows}
    g = game_lookup[upset_game_id]
    winner_name = team_id_to_name.get(upset_team_id, str(upset_team_id))
    loser_id = g["away_team_id"] if upset_team_id == g["home_team_id"] else g["home_team_id"]
    loser_name = team_id_to_name.get(loser_id, str(loser_id))

    print(f"\n  {winner_name} over {loser_name} ({g['season_year']}) -- win probability was only {upset_p:.1%}")
    for alpha in ALPHA_GRID:
        hybrid_rows = results_by_alpha[alpha]
        this_row = next(r for r in hybrid_rows if r[0] == upset_game_id and r[1] == upset_team_id)
        print(f"    alpha={alpha:<4} Game CoE 2.0 = {this_row[8]:.3f}")

    print("\n  Does this feel like the right amount of credit for the biggest upset")
    print("  in the dataset, at each alpha? That judgment call is the actual point")
    print("  of this tool -- there's no 'best' answer computed here.")


if __name__ == "__main__":
    main()
