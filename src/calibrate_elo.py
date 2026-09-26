#!/usr/bin/env python3
"""
Backtests Elo config parameters (K, home_field, offseason_retention)
against out-of-sample prediction accuracy, per the CoE 2.0 rollout plan's
calibration step. Deliberately separate from build_elo.py -- doesn't
change it, doesn't change config/model_config.json automatically (prints
a recommendation; you decide whether to adopt it).

Methodology: Elo only ever uses PREGAME ratings to form its expectation
for a game, so every game's elo_expectation, computed during a normal
chronological run, is already an authentic walk-forward out-of-sample
prediction -- the model has never seen that game's own result when it
made that prediction. No separate train/test split is needed; we just
score every game's home-team expectation against what actually happened,
using Brier score (mean squared error between predicted probability and
actual result: 0 = perfect, 0.25 = random-guess baseline for a coin-flip
game, lower is better).

Scope: this grid only varies k, home_field, and offseason_retention,
holding mov_c, mov_d, initial_rating, and scale fixed at their current
config/model_config.json values. MOV-constant tuning and hybrid-layer
tuning (elo_weight, difficulty_alpha) are separate, later steps -- not
mixed in here, per the project's one-thing-at-a-time discipline.

Caveat worth knowing: this scores every game in the dataset, including
the earliest ones where every team still starts at a flat, uninformative
1500 (a "burn-in" period). Home-field advantage still has a real,
immediate effect even then, but K and offseason_retention have less
signal to work with in that stretch. Left as all-games scoring here
for simplicity and transparency; excluding an early burn-in window is a
reasonable future refinement, not applied automatically.

Usage:
    python src/calibrate_elo.py [--db db/league.db] [--config config/model_config.json] [--out data/processed/elo_calibration_results.csv]
"""
from __future__ import annotations

import argparse
import csv
import itertools
import sqlite3

from build_elo import run_elo, fetch_games_chronological, load_config
from validation import brier_score as brier_of_pairs

K_GRID = [10, 15, 20, 25, 30, 35, 40]
HFA_GRID = [0, 25, 50, 75, 100]
RETENTION_GRID = [0.50, 0.60, 0.70, 0.80, 0.90, 1.0]


def brier_score(games: list, cfg: dict) -> float:
    """
    Lower is better. Scores using each game's HOME team perspective only
    (the away perspective carries identical information -- E_away = 1 -
    E_home, S_away = 1 - S_home -- scoring both would just double-count).

    The metric itself lives in src/validation.py (MODEL-07), shared with the
    other backtests and the validation harness, so "Brier" means one thing
    across the repository. This function's job is only to run the engine for a
    candidate config and pair each prediction with what happened.
    """
    rows, _, _ = run_elo(games, cfg)
    game_lookup = {g["game_id"]: g for g in games}

    pairs = []
    for row in rows:
        game_id, team_id, _, _, e_this, *_ = row
        g = game_lookup[game_id]
        if team_id != g["home_team_id"]:
            continue  # only score the home-team row per game, once each

        hs, aws = g["home_score"], g["away_score"]
        if hs > aws:
            s_home = 1.0
        elif aws > hs:
            s_home = 0.0
        else:
            s_home = 0.5

        pairs.append((e_this, s_home))

    return brier_of_pairs(pairs)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--config", default="config/model_config.json")
    p.add_argument("--out", default="data/processed/elo_calibration_results.csv")
    args = p.parse_args()

    base_cfg = load_config(args.config)["elo"]

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    games = fetch_games_chronological(conn)
    conn.close()

    print(f"Backtesting {len(K_GRID)} x {len(HFA_GRID)} x {len(RETENTION_GRID)} = "
          f"{len(K_GRID) * len(HFA_GRID) * len(RETENTION_GRID)} configs against {len(games)} games...")

    results = []
    for k, hfa, retention in itertools.product(K_GRID, HFA_GRID, RETENTION_GRID):
        cfg = dict(base_cfg)
        cfg["k"] = k
        cfg["home_field"] = hfa
        cfg["offseason_retention"] = retention
        score = brier_score(games, cfg)
        results.append({"k": k, "home_field": hfa, "offseason_retention": retention, "brier_score": score})

    results.sort(key=lambda r: r["brier_score"])

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["k", "home_field", "offseason_retention", "brier_score"])
        w.writeheader()
        w.writerows(results)
    print(f"Wrote full results ({len(results)} rows) to {args.out}")

    print("\nTop 10 configs by Brier score (lower is better; current config's score shown for comparison):")
    for r in results[:10]:
        print(f"  K={r['k']:<3} HFA={r['home_field']:<4} retention={r['offseason_retention']:<4} "
              f"Brier={r['brier_score']:.5f}")

    current_score = brier_score(games, base_cfg)
    print(f"\nCurrent config/model_config.json (K={base_cfg['k']}, HFA={base_cfg['home_field']}, "
          f"retention={base_cfg['offseason_retention']}): Brier={current_score:.5f}")

    best = results[0]
    print(f"\nBest found: K={best['k']}, HFA={best['home_field']}, retention={best['offseason_retention']} "
          f"(Brier={best['brier_score']:.5f})")
    print("This is a recommendation only -- config/model_config.json is NOT modified automatically.")


if __name__ == "__main__":
    main()
