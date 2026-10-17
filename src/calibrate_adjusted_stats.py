#!/usr/bin/env python3
"""
How hard to shrink the opponent adjustment: measures build_adjusted_stats.LAMBDA
instead of assuming it.

The ridge strength is the one free parameter in that module, and it is a real
trade-off, not a formality. Too little and a team's first opponent defines its
season; too much and every team is pulled to the league mean and the whole
table says nothing. The answer is not a matter of taste: the fitted values make
a prediction -- what each offence will do against each defence -- so the right
lambda is the one whose predictions are the most accurate on games the fit
never saw.

Method: for each season, split that season's observations into FOLDS parts at
random (a fixed seed, so a rerun gives the same answer), fit on the rest and
score the held-out part, for every candidate lambda. Reported as weighted mean
absolute error and RMSE, pooled over every season and fold, so no single
season's scale decides it.

The baseline row is the league mean alone -- no offence, no defence, no home
effect. A lambda that cannot beat it is not measuring anything.

Usage:
    python src/calibrate_adjusted_stats.py                       # every metric
    python src/calibrate_adjusted_stats.py --metric points --first-season 2010
"""
from __future__ import annotations

import argparse
import random
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_adjusted_stats as adj  # noqa: E402

FOLDS = 5
SEED = 20261017
# The grid that settled build_adjusted_stats.LAMBDA: coarse enough to show the
# shape of the curve, fine enough around the floor to show how flat it is.
FINE_PLAYS = [160.0, 180.0, 200.0, 220.0, 250.0, 300.0]
CANDIDATES = {
    "points": [0.0, 0.5, 1.0, 1.5, 1.75, 2.0, 2.25, 2.5, 3.0, 5.0, 8.0, 15.0, 30.0, 60.0],
    "ppa": [0.0, 25.0, 50.0, 100.0] + FINE_PLAYS + [350.0, 700.0, 1500.0, 3000.0],
    "success_rate": [0.0, 25.0, 50.0, 100.0] + FINE_PLAYS + [350.0, 700.0, 1500.0, 3000.0],
}


def score(obs, fit) -> tuple[float, float, float]:
    """Weighted absolute and squared error of a fit's predictions on `obs`."""
    w_total = abs_total = sq_total = 0.0
    for team, opp, v, w, s in obs:
        pred = fit["mu"] + fit["off"].get(team, 0.0) + fit["def"].get(opp, 0.0) + fit["home"] * s
        w_total += w
        abs_total += w * abs(v - pred)
        sq_total += w * (v - pred) ** 2
    return w_total, abs_total, sq_total


def folds_of(obs, rng) -> list[list]:
    shuffled = list(obs)
    rng.shuffle(shuffled)
    return [shuffled[i::FOLDS] for i in range(FOLDS)]


def calibrate(conn, metric: str, seasons: list[int]) -> list[dict]:
    rng = random.Random(SEED)
    totals = {lam: [0.0, 0.0, 0.0] for lam in CANDIDATES[metric]}
    mean_only = [0.0, 0.0, 0.0]
    used = []
    for season in seasons:
        obs = adj.observations(conn, season, metric)
        if len(obs) < adj.MIN_OBSERVATIONS:
            continue
        used.append(season)
        parts = folds_of(obs, rng)
        for k in range(FOLDS):
            held = parts[k]
            train = [o for i, p in enumerate(parts) if i != k for o in p]
            if not held or not train:
                continue
            mu = sum(v * w for _, _, v, w, _ in train) / sum(w for _, _, _, w, _ in train)
            flat = {"mu": mu, "home": 0.0, "off": {}, "def": {}}
            for i, part in enumerate(score(held, flat)):
                mean_only[i] += part
            for lam in CANDIDATES[metric]:
                fit = adj.solve(train, lam)
                for i, part in enumerate(score(held, fit)):
                    totals[lam][i] += part
    rows = []
    if mean_only[0]:
        rows.append({"lambda": None, "mae": mean_only[1] / mean_only[0],
                     "rmse": (mean_only[2] / mean_only[0]) ** 0.5, "seasons": len(used)})
    for lam, (w, a, s) in totals.items():
        if w:
            rows.append({"lambda": lam, "mae": a / w, "rmse": (s / w) ** 0.5,
                         "seasons": len(used)})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="db/league.db")
    ap.add_argument("--metric", choices=sorted(CANDIDATES), action="append")
    ap.add_argument("--first-season", type=int, default=2005)
    ap.add_argument("--last-season", type=int)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    last = args.last_season or adj.latest_season(conn)
    seasons = list(range(args.first_season, last + 1))
    for metric in (args.metric or sorted(CANDIDATES)):
        rows = calibrate(conn, metric, seasons)
        if not rows:
            print(f"{metric}: nothing to fit in {args.first_season}-{last}.")
            continue
        fitted = [r for r in rows if r["lambda"] is not None]
        best = min(fitted, key=lambda r: r["mae"])
        print(f"\n{adj.METRIC_LABELS[metric]} ({metric}) -- {rows[0]['seasons']} seasons, "
              f"{FOLDS}-fold, {args.first_season}-{last}")
        print(f"  {'lambda':>8}  {'MAE':>10}  {'RMSE':>10}")
        for r in rows:
            name = "mean only" if r["lambda"] is None else f"{r['lambda']:g}"
            mark = "  <- best MAE" if r is best else ""
            print(f"  {name:>8}  {r['mae']:>10.5f}  {r['rmse']:>10.5f}{mark}")
        print(f"  shipped: {adj.LAMBDA[metric]:g}")


if __name__ == "__main__":
    main()
