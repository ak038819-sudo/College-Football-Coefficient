#!/usr/bin/env python3
"""
Backtests the Elo/CoE hybrid weight (elo_weight, with coe_weight =
1 - elo_weight, per spec section 8's w_E + w_C = 1 constraint) against
out-of-sample prediction accuracy -- step 9 of the CoE 2.0 rollout plan.

Scope, per the design doc's OWN stated philosophy (section 32): this
tunes ONLY elo_weight/coe_weight, which legitimately feed
hybrid_expectation (a genuine game predictor) -- NOT difficulty_alpha.
Alpha shapes Game CoE (the ACHIEVEMENT measure), and the spec explicitly
says not to optimize CoE for prediction accuracy ("that's Elo's job").
Alpha tuning needs the qualitative criteria section 32 lists instead
(ranking stability, resistance to cupcake farming, sensible disagreements
via compare_models.py, etc.) -- a human-judgment process, not a grid
search for lowest Brier score. Deliberately not attempted here.

Methodology: identical to calibrate_elo.py -- hybrid_expectation only
ever uses PREGAME information (frozen prior-season CoE, pregame Elo), so
every game's hybrid_expectation is already an authentic walk-forward
out-of-sample prediction. Scores using each game's HOME team perspective
only (home/away carry identical information -- scoring both would double
count), via Brier score (lower is better).

Grid matches the design doc's own suggested test values (section 8):
75/25, 60/40, 50/50, 40/60, 25/75.

Does NOT modify config/model_config.json automatically -- prints a
recommendation only, same as calibrate_elo.py.

Usage:
    python src/calibrate_hybrid_weight.py [--db db/league.db] [--config config/model_config.json] [--out data/processed/hybrid_weight_calibration_results.csv]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3

from build_elo import load_config
from build_hybrid_coefficients import load_team_ratings_by_season, compute_frozen_5yr_coe, compute_hybrid_rows
from validation import brier_score as brier_of_pairs

ELO_WEIGHT_GRID = [0.25, 0.40, 0.50, 0.60, 0.75, 0.80, 0.85, 0.90, 0.95, 1.0]


def brier_score_for_weight(elo_rows, frozen_5yr, team_id_to_name, elo_home_field, hybrid_cfg, coe_cfg) -> float:
    hybrid_rows, _ = compute_hybrid_rows(elo_rows, frozen_5yr, team_id_to_name, elo_home_field, hybrid_cfg, coe_cfg)
    game_lookup = {r["game_id"]: r for r in elo_rows}

    pairs = []
    seen = set()
    for row in hybrid_rows:
        game_id, team_id, elo_z, coe_z, os_strength, hybrid_rating, p_this, result_type, game_coe = row
        if game_id in seen:
            continue
        g = game_lookup[game_id]
        if team_id != g["home_team_id"]:
            continue
        seen.add(game_id)

        if result_type in ("WIN", "OT_WIN"):
            s_home = 1.0
        elif result_type in ("LOSS", "OT_LOSS"):
            s_home = 0.0
        else:
            s_home = 0.5  # TIE -- structurally absent from 2000-2026, kept for completeness

        pairs.append((p_this, s_home))

    # The metric lives in src/validation.py (MODEL-07) so every backtest in the
    # repository computes Brier the same way. None (no games scored) becomes nan
    # here, which is what this script's callers already sort and print.
    return brier_of_pairs(pairs) if pairs else float("nan")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--config", default="config/model_config.json")
    p.add_argument("--team-ratings-csv", default="data/processed/team_ratings_by_season.csv")
    p.add_argument("--out", default="data/processed/hybrid_weight_calibration_results.csv")
    args = p.parse_args()

    cfg = load_config(args.config)
    elo_home_field = cfg["elo"]["home_field"]
    base_hybrid_cfg = cfg["hybrid"]
    coe_cfg = cfg["coe"]

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

    print(f"Backtesting {len(ELO_WEIGHT_GRID)} elo_weight values against {len(elo_rows) // 2} games...")

    results = []
    for elo_weight in ELO_WEIGHT_GRID:
        hybrid_cfg = dict(base_hybrid_cfg)
        hybrid_cfg["elo_weight"] = elo_weight
        hybrid_cfg["coe_weight"] = round(1.0 - elo_weight, 10)
        score = brier_score_for_weight(elo_rows, frozen_5yr, team_id_to_name, elo_home_field, hybrid_cfg, coe_cfg)
        results.append({"elo_weight": elo_weight, "coe_weight": hybrid_cfg["coe_weight"], "brier_score": score})

    results.sort(key=lambda r: r["brier_score"])

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["elo_weight", "coe_weight", "brier_score"])
        w.writeheader()
        w.writerows(results)
    print(f"Wrote full results to {args.out}\n")

    print("All configs, ranked (lower Brier is better):")
    for r in results:
        print(f"  elo_weight={r['elo_weight']:.2f}  coe_weight={r['coe_weight']:.2f}  Brier={r['brier_score']:.5f}")

    current_score = brier_score_for_weight(elo_rows, frozen_5yr, team_id_to_name, elo_home_field, base_hybrid_cfg, coe_cfg)
    print(f"\nCurrent config (elo_weight={base_hybrid_cfg['elo_weight']}, "
          f"coe_weight={base_hybrid_cfg['coe_weight']}): Brier={current_score:.5f}")

    best = results[0]
    print(f"\nBest found: elo_weight={best['elo_weight']}, coe_weight={best['coe_weight']} (Brier={best['brier_score']:.5f})")
    print("This is a recommendation only -- config/model_config.json is NOT modified automatically.")
    print("\nNote: difficulty_alpha is deliberately NOT tuned here -- see script docstring for why.")


if __name__ == "__main__":
    main()
