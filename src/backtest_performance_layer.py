#!/usr/bin/env python3
"""
Backtest matrix for the Elo performance layer (EXP-03, guide §9).

Runs the whole Elo engine once per variant over the same games and reports how
well each one's PREGAME expectations predicted the games that followed:

    current production MOV   baseline
    result only (M = 1)      does any performance modifier help at all?
    raw SRDiff               Success Rate without the strength adjustment
    xSRDiff / SR+            the candidate
    xSRDiff sensitivity      beta and bound sweeps, so parameters are chosen
                             from the whole record rather than a few memorable games

Every expectation is genuinely out-of-sample. The engine walks games in
chronological order and each game's expectation is formed from ratings built
only from earlier games, so a single pass is already walk-forward -- the same
argument calibrate_elo.py makes. The xSRDiff curve is point-in-time for the
same reason: each season uses a fold fitted only on seasons before it.

Metrics
    Brier score   mean squared error of the pregame win probability (lower better)
    log loss      penalises confident mistakes harder (lower better)
    calibration   |predicted - observed| averaged over probability deciles
    mean |dElo|   rating volatility, for judging whether a variant just turns K up
    mean M        the multiplier's scale -- MOV sits well above 1 and xSRDiff near 1,
                  so a variant switch almost certainly needs k recalibrated with it

The k confound, and --fit-k
    k reaches the record only through k * M, so a row scored at somebody else's k
    is measuring k, not the layer. Every row here shares the configured k by
    default, which means the default table flatters whichever layer that k was
    fitted for. --fit-k gives each variant its own best k from a sweep before
    scoring it, which is the only comparison between layers worth reading. The
    default is kept because it answers a different, also useful question: what
    each variant would do if dropped into the engine as configured today.

Usage:
    python src/backtest_performance_layer.py --db db/league.db
    python src/backtest_performance_layer.py --db db/league.db --from-season 2001
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_elo import fetch_games_chronological, load_config, run_elo  # noqa: E402
from srdiff import MOV, RAW_SRDIFF, RESULT_ONLY, XSRDIFF, XsrModel, build_layer  # noqa: E402
# Brier, log loss and the calibration summary used to be computed privately in
# this file. They now come from the shared harness (MODEL-07), which returns the
# same numbers plus accuracy and skill, so a variant's score here is directly
# comparable to run_validation.py's report.
from validation import score  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_PATH = REPO / "data" / "processed" / "performance_layer_backtest.csv"


def evaluate(games, rows, from_season: int | None) -> dict:
    """
    Read the engine's own output: each row carries the expectation the model
    made before that game, so nothing is recomputed here and nothing can peek
    at a rating formed later.
    """
    result_of, season_of, home_of = {}, {}, {}
    for g in games:
        hs, aws = g["home_score"], g["away_score"]
        result_of[(g["game_id"], g["home_team_id"])] = 1.0 if hs > aws else 0.0 if aws > hs else 0.5
        result_of[(g["game_id"], g["away_team_id"])] = 1.0 if aws > hs else 0.0 if hs > aws else 0.5
        season_of[g["game_id"]] = g["season_year"]
        home_of[g["game_id"]] = g["home_team_id"]

    pairs, deltas, mults, by_model = [], [], [], defaultdict(int)
    seen_games = set()
    for r in rows:
        gid, tid, expectation, m, delta = r[0], r[1], r[4], r[5], r[6]
        if from_season is not None and season_of[gid] < from_season:
            continue
        # One prediction per game, from the home side, matching run_validation.py.
        # Both rows of a game carry mirrored information, so scoring both leaves
        # Brier and log loss untouched -- but it does NOT leave the binned
        # calibration error untouched, because a bin holding the favourites is
        # not the mirror of the bin holding the underdogs. This file used to
        # score both, so its calibration column was not comparable with the
        # harness's. |dElo| is unaffected either way: an Elo update is zero-sum,
        # so the two rows of a game differ only in sign.
        if tid == home_of[gid]:
            pairs.append((expectation, result_of[(gid, tid)]))
        deltas.append(abs(delta))
        if gid not in seen_games:          # M is per game, not per team
            seen_games.add(gid)
            mults.append(m)
            by_model[r[14]] += 1
    out = score(pairs)
    out["games"] = len(seen_games)
    out["mean_abs_delta"] = (sum(deltas) / len(deltas)) if deltas else None
    out["mean_multiplier"] = (sum(mults) / len(mults)) if mults else None
    out["paths"] = dict(by_model)
    return out


def variants(elo_cfg: dict, perf_cfg: dict, model: XsrModel | None) -> list[tuple[str, dict]]:
    """The matrix. Sensitivity rows sweep beta and the bounds around the configured values."""
    base = dict(perf_cfg)
    out = [("production_mov", {**base, "modifier": MOV}),
           ("result_only", {**base, "modifier": RESULT_ONLY}),
           ("raw_srdiff", {**base, "modifier": RAW_SRDIFF}),
           ("xsrdiff", {**base, "modifier": XSRDIFF})]
    for beta in (0.5, 1.0, 2.0, 4.0):
        out.append((f"xsrdiff_beta{beta}", {**base, "modifier": XSRDIFF, "beta": beta}))
    for lo, hi in ((0.75, 1.25), (0.5, 1.5), (0.25, 2.0)):
        out.append((f"xsrdiff_bounds{lo}_{hi}", {**base, "modifier": XSRDIFF, "m_min": lo, "m_max": hi}))
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--config", default=str(REPO / "config" / "model_config.json"))
    p.add_argument("--out", default=str(OUT_PATH))
    p.add_argument("--from-season", type=int, default=None,
                   help="score only games from this season on (e.g. 2001, where Success Rate coverage starts)")
    p.add_argument("--fit-k", action="store_true",
                   help="give each variant its own best k before scoring it, so the table compares "
                        "layers rather than k * mean(M) -- see the module docstring")
    p.add_argument("--k-grid", default="20,25,30,35,40,45,50,55,60,70,80",
                   help="the k values --fit-k searches (comma separated)")
    args = p.parse_args()

    raw = load_config(args.config)
    elo_cfg = raw["elo"]
    from srdiff import load_performance_config
    perf_cfg = load_performance_config(raw.get("performance"))
    model = XsrModel.load(perf_cfg["model_path"]) if perf_cfg["model_path"] else None

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    games = fetch_games_chronological(conn)
    has_sr = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='game_team_advanced'").fetchone()
    success_rates = {}
    if has_sr:
        success_rates = {(g, t): sr for g, t, sr in conn.execute(
            "SELECT game_id, team_id, off_success_rate FROM game_team_advanced "
            "WHERE off_success_rate IS NOT NULL")}
    conn.close()

    print(f"{len(games):,} games; per-game Success Rate for {len(success_rates):,} team-games; "
          f"xSRDiff curve: {model.version if model else 'not fitted'}")
    if not success_rates:
        print("WARNING: with no per-game Success Rate every SR variant falls back, so the rows "
              "below will be identical. Fetch and load it first (see src/fit_xsrdiff.py).")

    k_grid = [int(v) for v in args.k_grid.split(",")] if args.fit_k else [elo_cfg["k"]]
    if args.fit_k:
        print(f"--fit-k: each variant gets its own best k from {k_grid}")

    results = []
    for name, cfg in variants(elo_cfg, perf_cfg, model):
        best = None
        for k in k_grid:
            ecfg = {**elo_cfg, "k": k}
            rows, _, _ = run_elo(games, ecfg, build_layer(cfg, ecfg, model), success_rates)
            r = evaluate(games, rows, args.from_season)
            r["k"] = k
            # A variant with nothing to score keeps its first row rather than
            # dropping out of the table, so a broken row stays visible.
            better = best is None or (r["brier"] is not None and
                                      (best["brier"] is None or r["brier"] < best["brier"]))
            if better:
                best = r
        r = best
        r["variant"] = name
        r["beta"], r["m_min"], r["m_max"] = cfg["beta"], cfg["m_min"], cfg["m_max"]
        results.append(r)

    fields = ["variant", "k", "beta", "m_min", "m_max", "games", "n", "brier", "log_loss",
              "accuracy", "base_rate", "brier_skill", "calibration_error",
              "mean_abs_delta", "mean_multiplier", "paths"]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({**{k: r.get(k) for k in fields}, "paths": json.dumps(r["paths"], sort_keys=True)})

    scope = f" (scored from {args.from_season})" if args.from_season else ""
    print(f"\nWrote {out}{scope}\n")
    print(f"{'variant':<24}{'k':>4}{'Brier':>10}{'log loss':>11}{'calib':>9}{'mean|dElo|':>12}{'mean M':>9}")
    for r in sorted(results, key=lambda x: (x["brier"] is None, x["brier"])):
        print(f"{r['variant']:<24}{r['k']:>4}{r['brier']:>10.5f}{r['log_loss']:>11.5f}"
              f"{r['calibration_error']:>9.5f}{r['mean_abs_delta']:>12.3f}{r['mean_multiplier']:>9.3f}")
    print("\nLower Brier/log loss/calibration is better.")
    if args.fit_k:
        print("Each row is at its own best k, so the Brier column compares layers.")
    else:
        print(f"Every row is at the configured k={elo_cfg['k']}, which was fitted for "
              f"'{perf_cfg['modifier']}'. k acts only through k * mean(M), and the mean M column "
              "shows how far apart these layers are on that scale -- so this table does NOT "
              "settle which layer predicts better. Re-run with --fit-k for that.")


if __name__ == "__main__":
    main()
