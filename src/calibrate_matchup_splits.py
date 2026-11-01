#!/usr/bin/env python3
"""Calibrate split-stat shrinkage on 2025 ONLY, evaluate final 2026 holdout ONCE.

Never select parameters based on 2026. Uses an identical matched sample for
model-vs-baseline comparisons. Fails closed when calibration is inconclusive.
No data is fetched or committed by this script.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

try:
    from src.validate_matchup_intelligence import evaluate_walk_forward
except ModuleNotFoundError:
    from validate_matchup_intelligence import evaluate_walk_forward

SPLITS = (
    "rush_success_rate", "rush_ppa", "rush_explosiveness",
    "pass_success_rate", "pass_ppa", "pass_explosiveness",
)
CANDIDATES = (0.5, 2.0, 8.0, 24.0)
MIN_ELIGIBLE = 0.50
MIN_IMPROVEMENT = 0.002


def select_on_training(
    reports: list[dict], *, min_coverage: float = MIN_ELIGIBLE,
    min_improvement: float = MIN_IMPROVEMENT
) -> dict:
    """Choose best 2025 model, or explicit non-adjusted baseline.

    Penalize models which exclude too many matchups; require minimum relative
    improvement instead of blindly promoting small numerical differences.
    """
    choices = []
    for r in reports:
        adj = r["opponent_adjusted"]
        baseline = r["matched_team_offense_baseline"]
        if not adj or not baseline or adj["n"] != baseline["n"]:
            continue
        if r["model_coverage"] < min_coverage or baseline["mae"] <= 0:
            continue
        improvement = (baseline["mae"] - adj["mae"]) / baseline["mae"]
        choices.append({
            "lambda": r["shrinkage"],
            "min_team_games": r["min_team_games"],
            "mae": adj["mae"], "baseline_mae": baseline["mae"],
            "relative_improvement": improvement,
            "coverage": r["model_coverage"],
            "n": adj["n"],
        })
    if not choices:
        return {"policy": "baseline", "reason": "insufficient_training_coverage", "candidates": []}
    best = min(choices, key=lambda x: (-x["relative_improvement"], -x["coverage"],
                                      x["lambda"], x["min_team_games"]))
    policy = "adjusted" if best["relative_improvement"] >= min_improvement else "baseline"
    return {"policy": policy,
            "reason": "training_gain_met" if policy == "adjusted" else "no_verified_training_gain",
            "selected": best, "candidates": choices}


def calibrate(
    conn: sqlite3.Connection,
    *,
    training_season: int = 2025,
    holdout_season: int = 2026,
    lambdas: tuple[float, ...] = CANDIDATES,
    min_games_options: tuple[int, ...] = (3,),
) -> dict:
    if training_season >= holdout_season:
        raise ValueError("Training must precede the untouched holdout season")
    if not lambdas or any(x < 0 for x in lambdas):
        raise ValueError("Invalid calibration grid")
    result = {"training_season": training_season, "holdout_season": holdout_season,
              "data_kind": "chronological_split_metric_validation",
              "model": "shared_adjusted_stats_solver",
              "selection_uses_holdout": False, "metrics": {}}
    for metric in SPLITS:
        training = [
            evaluate_walk_forward(conn, season=training_season, metric=metric,
                                  shrinkage=lam, min_team_games=n)
            for n in min_games_options for lam in lambdas
        ]
        decision = select_on_training(training)
        if decision["candidates"]:
            chosen = decision["selected"]
            holdout = evaluate_walk_forward(
                conn, season=holdout_season, metric=metric,
                shrinkage=chosen["lambda"], min_team_games=chosen["min_team_games"])
        else:
            # Even when no candidate has adequate coverage, score the
            # original benchmark so the holdout is documented, not tuned.
            holdout = evaluate_walk_forward(
                conn, season=holdout_season, metric=metric, shrinkage=2.0)
        model = holdout["opponent_adjusted"]
        base = holdout["matched_team_offense_baseline"]
        result["metrics"][metric] = {
            "selection": decision,
            "holdout": {
                "adjusted": model, "prior_offense": base,
                "coverage": holdout["model_coverage"],
                "model_improves_baseline": (
                    model is not None and base is not None and
                    model["mae"] < base["mae"]
                ),
            },
            "release_recommendation": (
                "research_only" if decision["policy"] != "adjusted"
                or model is None or base is None or model["mae"] >= base["mae"]
                else "eligible_for_further_validation"
            ),
        }
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", required=True)
    p.add_argument("--training", type=int, default=2025)
    p.add_argument("--holdout", type=int, default=2026)
    args = p.parse_args()
    db = Path(args.db).resolve()
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as conn:
        report = calibrate(conn, training_season=args.training,
                           holdout_season=args.holdout)
    dest = Path(args.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for metric, row in report["metrics"].items():
        s = row["selection"]
        h = row["holdout"]
        a, b = h["adjusted"], h["prior_offense"]
        print(metric, "2025_policy=", s["policy"],
              "lambda=", s.get("selected", {}).get("lambda"),
              "2026_n=", a["n"] if a else 0,
              "2026_MAE=", round(a["mae"], 5) if a else None,
              "2026_baseline_MAE=", round(b["mae"], 5) if b else None,
              "verdict=", row["release_recommendation"])


if __name__ == "__main__":
    main()
