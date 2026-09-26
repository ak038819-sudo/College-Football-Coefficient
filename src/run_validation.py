#!/usr/bin/env python3
"""
Model validation harness (MODEL-07).

Produces one versioned, machine-readable report per configuration so experiments
can be compared instead of argued about:

    data/processed/validation/<config_hash>.json

The report has two sections that are never combined into a single score:

  predictive    Walk-forward Brier score, log loss, calibration and sharpness for
                every model that states a pregame probability -- Elo and the
                hybrid expectation -- reported per fold and in aggregate.
  achievement   CoE diagnostics (src/validation/achievement.py). CoE is an
                achievement system; tuning it to maximise prediction would turn
                it into a worse copy of Elo, which the spec explicitly forbids.

Reproducibility: the filename is a hash of the model configuration that produced
it, and the report embeds that configuration. Re-running with the same config
overwrites the same file with the same numbers; changing any parameter writes a
new one, so two reports can always be diffed.

Chronology: folds only ever train on strictly earlier seasons, and the harness
asserts that before writing. Elo's own expectations are out-of-sample by
construction -- the engine walks games in order, so a game's probability was
formed from ratings built only from earlier games -- which is why the predictive
section can score a single chronological pass.

Usage:
    python src/run_validation.py --db db/league.db
    python src/run_validation.py --db db/league.db --compare <other_hash>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validation import assert_chronological, score_predictions, season_folds  # noqa: E402
from validation.achievement import run_all as run_achievement  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "data" / "processed" / "validation"
MIN_TRAIN_SEASONS = 5


def config_fingerprint(cfg: dict) -> tuple[str, dict]:
    """
    (hash, the configuration it hashes). Comment keys are stripped first so
    editing a comment does not invent a new experiment.
    """
    clean = {k: {kk: vv for kk, vv in v.items() if not kk.startswith("_")}
             for k, v in cfg.items() if isinstance(v, dict)}
    blob = json.dumps(clean, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:12], clean


def predictive_observations(conn: sqlite3.Connection) -> dict:
    """
    {model: {season: [(probability, outcome), ...]}} for every model that stated
    a pregame probability. Both are read from stored values -- the expectation
    the model actually made before that game -- so nothing is recomputed here
    and no later rating can leak in.
    """
    out: dict = defaultdict(lambda: defaultdict(list))
    for season, exp, result in conn.execute(
            """SELECT g.season_year, e.elo_expectation,
                      CASE WHEN (e.team_id = g.home_team_id AND g.home_score > g.away_score)
                             OR (e.team_id = g.away_team_id AND g.away_score > g.home_score) THEN 1.0
                           WHEN g.home_score = g.away_score THEN 0.5 ELSE 0.0 END
               FROM elo_game_history e JOIN games g ON g.game_id = e.game_id
               WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL"""):
        out["elo"][season].append((exp, result))
    has_hybrid = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='hybrid_game_ratings'").fetchone()
    if has_hybrid:
        for season, exp, result in conn.execute(
                """SELECT g.season_year, h.hybrid_expectation,
                          CASE WHEN (h.team_id = g.home_team_id AND g.home_score > g.away_score)
                                 OR (h.team_id = g.away_team_id AND g.away_score > g.home_score) THEN 1.0
                               WHEN g.home_score = g.away_score THEN 0.5 ELSE 0.0 END
                   FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
                   WHERE h.hybrid_expectation IS NOT NULL
                     AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL"""):
            out["hybrid"][season].append((exp, result))
    return {m: dict(v) for m, v in out.items()}


def evaluate_predictive(observations: dict, min_train: int = MIN_TRAIN_SEASONS) -> dict:
    """
    Per model: a row per walk-forward fold plus the aggregate over all folds.

    The aggregate pools the fold OBSERVATIONS rather than averaging fold scores,
    so a short season cannot weigh as much as a full one.
    """
    report = {}
    for model, by_season in observations.items():
        folds = season_folds(by_season.keys(), min_train_seasons=min_train)
        assert_chronological([(f.test_season, f.trained_through) for f in folds])
        rows, pooled = [], []
        for f in folds:
            pairs = by_season.get(f.test_season, [])
            if not pairs:
                continue
            rows.append({**f.as_dict(), **score_predictions(pairs)})
            pooled.extend(pairs)
        report[model] = {
            "folds": [{k: (round(v, 6) if isinstance(v, float) else v) for k, v in r.items()
                       if k != "train_seasons"} for r in rows],
            "aggregate": {k: (round(v, 6) if isinstance(v, float) else v)
                          for k, v in score_predictions(pooled).items()},
            "first_scored_season": rows[0]["test_season"] if rows else None,
            "seasons_scored": len(rows),
            "reading": "Lower Brier and log loss are better; calibration error is how far "
                       "predicted frequencies sit from observed ones. Sharpness is reported "
                       "beside calibration because always predicting 50% is perfectly "
                       "calibrated and useless.",
        }
    return report


def build_report(conn: sqlite3.Connection, cfg: dict, min_train: int = MIN_TRAIN_SEASONS) -> dict:
    fingerprint, clean_cfg = config_fingerprint(cfg)
    return {
        "config_hash": fingerprint,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "min_train_seasons": min_train,
        "config": clean_cfg,
        "predictive": evaluate_predictive(predictive_observations(conn), min_train),
        "achievement": run_achievement(conn),
        "note": "Predictive and achievement sections are deliberately separate. CoE is an "
                "achievement system: tuning it to maximise forecast accuracy would make it a "
                "worse copy of Elo.",
    }


def _fmt(v, width=10, places=5):
    return " " * width if v is None else f"{v:>{width}.{places}f}"


def print_summary(report: dict) -> None:
    print(f"config {report['config_hash']}  ({report['generated_at']})\n")
    print("PREDICTIVE (walk-forward)")
    print(f"  {'model':<10}{'seasons':>9}{'n':>10}{'Brier':>11}{'log loss':>11}{'calib':>10}{'sharp':>10}")
    for model, r in sorted(report["predictive"].items()):
        a = r["aggregate"]
        print(f"  {model:<10}{r['seasons_scored']:>9}{a['n']:>10,}"
              f"{_fmt(a['brier'], 11)}{_fmt(a['log_loss'], 11)}"
              f"{_fmt(a['calibration_error'], 10)}{_fmt(a['sharpness'], 10)}")
    print("\nACHIEVEMENT (CoE diagnostics)")
    ach = report["achievement"]
    def line(label, value):
        print(f"  {label:<34}{value}")
    s = ach.get("schedule_strength", {})
    line("schedule-strength correlation", s.get("mean_correlation"))
    e = ach.get("elite_win_reward", {})
    line("elite win / weak win", e.get("ratio"))
    c = ach.get("cupcake_resistance", {})
    line("weak wins per elite win", c.get("weak_wins_per_elite_win"))
    cb = ach.get("conference_behavior", {})
    line("conference spread (latest)", cb.get("spread"))
    line("internal games in conf. strength",
         f"{cb.get('internal_regular_season_games', 0):,} excluded")
    h = ach.get("historical_overlap", {})
    line("playoff overlap rate", h.get("overlap_rate"))
    ec = ach.get("edge_cases", {})
    line("negative season totals", ec.get("negative_season_totals"))
    print("\n  Each diagnostic carries a 'reading' in the JSON explaining what its value means.")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(REPO / "db" / "league.db"))
    p.add_argument("--config", default=str(REPO / "config" / "model_config.json"))
    p.add_argument("--out-dir", default=str(OUT_DIR))
    p.add_argument("--min-train-seasons", type=int, default=MIN_TRAIN_SEASONS)
    p.add_argument("--compare", help="an earlier config hash to diff this run against")
    args = p.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    conn = sqlite3.connect(args.db)
    report = build_report(conn, cfg, args.min_train_seasons)
    conn.close()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{report['config_hash']}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print_summary(report)
    print(f"\nWrote {path}")

    if args.compare:
        other = out_dir / f"{args.compare}.json"
        if not other.exists():
            sys.exit(f"No report {args.compare} in {out_dir}")
        prev = json.loads(other.read_text(encoding="utf-8"))
        print(f"\nvs {args.compare}:")
        for model in sorted(set(report["predictive"]) | set(prev.get("predictive", {}))):
            a = report["predictive"].get(model, {}).get("aggregate", {})
            b = prev.get("predictive", {}).get(model, {}).get("aggregate", {})
            if a.get("brier") is None or b.get("brier") is None:
                continue
            d = a["brier"] - b["brier"]
            print(f"  {model:<10}Brier {b['brier']:.5f} -> {a['brier']:.5f}  "
                  f"({d:+.5f}, {'better' if d < 0 else 'worse' if d > 0 else 'unchanged'})")


if __name__ == "__main__":
    main()
