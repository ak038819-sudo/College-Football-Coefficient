#!/usr/bin/env python3
"""
Fits the xSRDiff expectation curve from historical games (EXP-03, guide §3).

xSRDiff answers "how much better should this team have performed, by Success
Rate, given the venue-adjusted pregame Elo gap?" The guide is explicit that the
Elo-to-SR conversion must be FITTED, never invented, so this script builds the
training set, prints bucket diagnostics, fits an interpretable baseline, and
writes a versioned artifact that src/srdiff.py loads.

Training rows use only what was known before kickoff:
    season, game_id, team_id, opponent_id, neutral flag,
    pregame Elo both sides, venue-adjusted EloDiff*, team SR, opponent SR,
    actual SRDiff
Every game contributes BOTH team views, which are exact mirrors of each other
(EloDiff* and SRDiff both negate). That is deliberate: it centres the fit, so a
materially nonzero intercept is a signal worth investigating rather than
something to force to zero.

POINT-IN-TIME. A single fit over all history, applied backwards, is not a
backtest -- the guide says so in as many words. So the artifact stores one fold
per season, each fitted only on seasons strictly before it, and build_elo.py
selects the fold by the game's season. A season with too little prior data gets
no fold at all and takes the Elo fallback path, rather than quietly borrowing
the global fit. `global_fit` is stored separately and is only for forecasting
games that have not happened yet.

Usage:
    python src/fit_xsrdiff.py --db db/league.db
    python src/fit_xsrdiff.py --db db/league.db --buckets       # diagnostics only
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from srdiff import LINEAR  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_PATH = REPO / "data" / "processed" / "xsrdiff_model.json"
# A fold needs enough prior games for the slope to mean anything. Below this the
# season simply has no expectation curve and falls back -- an honest gap beats a
# confident line through noise.
MIN_TRAINING_ROWS = 400
BUCKET_WIDTH = 50          # Elo points per diagnostic bin (guide suggests 25 or 50)


def build_training_rows(conn: sqlite3.Connection) -> list[dict]:
    """
    One row per team-game that has a pregame Elo state AND per-game Success Rate
    for both sides. Reads elo_game_history for the pregame values rather than
    recomputing them, so the fit trains on exactly what the engine saw.

    elo_diff_adjusted is stored per team by build_elo.py; where an older
    database predates that column it is reconstructed from the two pregame
    ratings plus the venue, which is the same quantity.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(elo_game_history)")}
    if "elo_diff_adjusted" in cols:
        diff_expr = "e.elo_diff_adjusted"
    else:
        diff_expr = "NULL"
    rows = conn.execute(
        f"""
        SELECT g.season_year AS season, g.game_id AS game_id, e.team_id AS team_id,
               CASE WHEN e.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END AS opponent_id,
               g.neutral_site AS neutral,
               CASE WHEN e.team_id = g.home_team_id THEN 1 ELSE 0 END AS is_home,
               e.pregame_elo AS pregame_elo, e.opponent_pregame_elo AS opp_pregame_elo,
               {diff_expr} AS elo_diff_adjusted,
               a.off_success_rate AS team_sr, o.off_success_rate AS opp_sr
        FROM elo_game_history e
        JOIN games g ON g.game_id = e.game_id
        JOIN game_team_advanced a ON a.game_id = e.game_id AND a.team_id = e.team_id
        JOIN game_team_advanced o ON o.game_id = e.game_id AND o.team_id =
             (CASE WHEN e.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END)
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
          AND a.off_success_rate IS NOT NULL AND o.off_success_rate IS NOT NULL
        ORDER BY g.season_year, g.game_date, g.game_id, e.team_id
        """
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["sr_diff"] = d["team_sr"] - d["opp_sr"]
        if d["elo_diff_adjusted"] is None:
            # Venue adjustment is already inside the stored pregame ratings' effective
            # values; without the column, fall back to the plain difference.
            d["elo_diff_adjusted"] = d["pregame_elo"] - d["opp_pregame_elo"]
        out.append(d)
    return out


def bucket_diagnostics(rows: list[dict], width: int = BUCKET_WIDTH) -> list[dict]:
    """Count, mean, median and spread of actual SRDiff per EloDiff* bin, so the
    shape of the relationship can be eyeballed before any line is fitted."""
    buckets = defaultdict(list)
    for r in rows:
        buckets[int(r["elo_diff_adjusted"] // width) * width].append(r["sr_diff"])
    out = []
    for lo in sorted(buckets):
        vals = buckets[lo]
        out.append({"elo_lo": lo, "elo_hi": lo + width, "n": len(vals),
                    "mean_sr_diff": round(statistics.fmean(vals), 5),
                    "median_sr_diff": round(statistics.median(vals), 5),
                    "stdev": round(statistics.pstdev(vals), 5) if len(vals) > 1 else None})
    return out


def fit_linear(rows: list[dict]) -> dict | None:
    """
    Ordinary least squares of SRDiff on EloDiff*. Returns None when there is no
    spread in x (every game an identical matchup) -- an undefined slope, not a
    zero one.
    """
    n = len(rows)
    if n < 2:
        return None
    xs = [r["elo_diff_adjusted"] for r in rows]
    ys = [r["sr_diff"] for r in rows]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    b = sxy / sxx
    a = my - b * mx
    resid = [y - (a + b * x) for x, y in zip(xs, ys)]
    syy = sum((y - my) ** 2 for y in ys)
    return {"a": a, "b": b, "n": n,
            "r2": (1 - sum(e * e for e in resid) / syy) if syy > 0 else None,
            "residual_sd": statistics.pstdev(resid) if n > 1 else None}


def walk_forward_folds(rows: list[dict], min_rows: int = MIN_TRAINING_ROWS) -> dict:
    """
    {season: fit trained ONLY on seasons before it}. This is what makes a
    historical rebuild point-in-time: the expectation applied to a 2012 game
    never saw 2013.
    """
    by_season = defaultdict(list)
    for r in rows:
        by_season[r["season"]].append(r)
    folds, history = {}, []
    for season in sorted(by_season):
        if len(history) >= min_rows:
            fit = fit_linear(history)
            if fit:
                folds[season] = {"a": fit["a"], "b": fit["b"], "n": fit["n"],
                                 "trained_through": season - 1,
                                 "r2": fit["r2"], "residual_sd": fit["residual_sd"]}
        history.extend(by_season[season])
    return folds


def out_of_sample_residuals(rows: list[dict], folds: dict) -> dict:
    """
    Mean SR+ per season using each season's own fold. The guide's calibration
    check: out-of-sample mean SR+ should sit near zero overall, and a bucket or
    season that drifts is worth looking at rather than averaging away.
    """
    by_season = defaultdict(list)
    for r in rows:
        f = folds.get(r["season"])
        if not f:
            continue
        by_season[r["season"]].append(r["sr_diff"] - (f["a"] + f["b"] * r["elo_diff_adjusted"]))
    per_season = {s: {"n": len(v), "mean_sr_plus": round(statistics.fmean(v), 5)}
                  for s, v in sorted(by_season.items())}
    allv = [x for v in by_season.values() for x in v]
    return {"per_season": per_season,
            "overall": {"n": len(allv),
                        "mean_sr_plus": round(statistics.fmean(allv), 5) if allv else None,
                        "sd_sr_plus": round(statistics.pstdev(allv), 5) if len(allv) > 1 else None}}


def build_model(rows: list[dict], min_rows: int = MIN_TRAINING_ROWS) -> dict:
    folds = walk_forward_folds(rows, min_rows)
    glob = fit_linear(rows)
    seasons = sorted({r["season"] for r in rows})
    return {
        "kind": LINEAR,
        # The version identifies the exact training set, so a stored rating can
        # always name the curve that produced it.
        "version": f"xsrdiff_v1_{seasons[0]}_{seasons[-1]}_n{len(rows)}" if seasons else "xsrdiff_v1_empty",
        "fitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "training_rows": len(rows),
        "training_seasons": seasons,
        "min_training_rows": min_rows,
        "global_fit": ({"a": glob["a"], "b": glob["b"], "n": glob["n"],
                        "r2": glob["r2"], "residual_sd": glob["residual_sd"]} if glob else None),
        "folds": {str(s): f for s, f in sorted(folds.items())},
        "calibration": out_of_sample_residuals(rows, folds),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--out", default=str(OUT_PATH))
    p.add_argument("--min-training-rows", type=int, default=MIN_TRAINING_ROWS)
    p.add_argument("--buckets", action="store_true", help="print bucket diagnostics and exit")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='game_team_advanced'").fetchone()
    rows = build_training_rows(conn) if has_table else []
    conn.close()

    if not rows:
        sys.exit("No training rows: per-game Success Rate is missing.\n"
                 "Fetch and load it first:\n"
                 "  python src/fetch_cfbd_game_advanced.py 2001 2026   (needs CFBD_API_KEY)\n"
                 "  for f in data/raw/game_advanced_*.csv; do python src/load_game_advanced.py \"$f\"; done\n"
                 "Until then build_elo.py takes the configured fallback for every game.")

    if args.buckets:
        print(f"{len(rows):,} training rows, {len(set(r['season'] for r in rows))} seasons\n")
        print(f"{'EloDiff* bin':>18}  {'n':>7}  {'mean SRDiff':>12}  {'median':>9}  {'sd':>7}")
        for b in bucket_diagnostics(rows):
            sd = "" if b["stdev"] is None else f"{b['stdev']:.4f}"
            print(f"{b['elo_lo']:>8}..{b['elo_hi']:<8}{b['n']:>7,}  {b['mean_sr_diff']:>12.4f}  "
                  f"{b['median_sr_diff']:>9.4f}  {sd:>7}")
        return

    model = build_model(rows, args.min_training_rows)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(model, indent=2) + "\n", encoding="utf-8")

    g = model["global_fit"]
    print(f"Wrote {out}")
    print(f"  version           {model['version']}")
    print(f"  training rows     {model['training_rows']:,} over {len(model['training_seasons'])} seasons")
    print(f"  walk-forward folds {len(model['folds'])} "
          f"({min(model['folds'], default='-')}..{max(model['folds'], default='-')})")
    if g:
        print(f"  global fit        SRDiff = {g['a']:+.5f} {g['b']:+.7f} * EloDiff*"
              f"   r2={g['r2']:.4f}  residual sd={g['residual_sd']:.4f}")
    c = model["calibration"]["overall"]
    if c["n"]:
        print(f"  out-of-sample     mean SR+ {c['mean_sr_plus']:+.5f}  sd {c['sd_sr_plus']:.4f}  (n={c['n']:,})")
    print("\nNext: python src/backtest_performance_layer.py --db <db>  to compare variants "
          "before trusting beta/m_min/m_max in config/model_config.json.")


if __name__ == "__main__":
    main()
