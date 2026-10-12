#!/usr/bin/env python3
"""The model's own track record, in the shape a page can render.

src/run_validation.py has scored this project's predictions against the
record on every scheduled refresh for as long as the harness has existed,
writing data/processed/validation_report.json. Nothing read it. A model whose
numbers people are meant to quote was keeping its own accuracy to itself.

This module turns that report into the few hundred numbers a reader needs and
no more. The full report stays in the repository for anyone who wants the
associations, the stability series and the per-model calibration; what comes
out of here is the headline, the comparison, the season-by-season line and the
calibration curve for the one model the site actually publishes predictions
from.

WHICH MODEL THE SITE PUBLISHES

`elo` is the live rating engine's own pregame number -- the same expectation
src/predict_upcoming.py puts on an upcoming game's card and the same one the
rest-of-season projection flips its coins with. It is the row a reader should
read first, which is why HEADLINE names it rather than letting the page pick
whichever model scored best.

WHY THE BASELINES ARE HERE

An accuracy of 72% means nothing without knowing that picking the home team
every time gets 60%. Both baselines are published beside the model for that
reason, and `brier_skill` is measured against the harder of the two.
"""
from __future__ import annotations

import json
from pathlib import Path

DEFAULT_REPORT = Path("data/processed/validation_report.json")

# The model the site's published predictions come from.
HEADLINE = "elo"

# Order, label and one plain sentence per model. The sentences are the ones
# run_validation.py's own docstring uses, because a reader of this page and a
# reader of that file should not be told two different things.
MODELS = [
    ("elo", "Elo", "The live rating engine's pregame number. This is what the site publishes."),
    ("hybrid", "Hybrid",
     "Elo blended with each team's frozen prior-season coefficient. Covers fewer seasons, "
     "because the earliest ones have no prior season to freeze."),
    ("elo_on_hybrid_games", "Elo, same games",
     "Elo again, over only the games the hybrid can predict. The like-for-like comparison: "
     "the two rows above are scored on different sets of games."),
    ("baseline_home_rate", "Always the home team",
     "A constant equal to the observed home-win rate. Harder to beat than it sounds: home "
     "advantage is real signal, and this baseline is handed a rate measured on the very games "
     "it is scored against."),
    ("baseline_coin_flip", "Coin flip",
     "0.5 for every game. The floor; anything that does not clear this is not a model."),
]

METRICS = ("accuracy", "brier", "brier_skill", "log_loss", "calibration_error", "n", "base_rate")


def build_accuracy(report_path: Path | str | None = None) -> dict | None:
    """The track record, or None when no report has been generated yet.

    None rather than an empty shell, so the page can be absent instead of
    present and blank -- the same rule the rest-of-season projection follows.
    """
    path = Path(report_path or DEFAULT_REPORT)
    if not path.exists():
        return None
    report = json.loads(path.read_text())
    models = report.get("models") or {}
    if HEADLINE not in models:
        return None

    rows = []
    for key, label, blurb in MODELS:
        model = models.get(key)
        if not model:
            continue
        overall = model.get("overall") or {}
        seasons = sorted(model.get("seasons") or [])
        rows.append({
            "key": key, "label": label, "blurb": blurb,
            "headline": key == HEADLINE,
            "baseline": key.startswith("baseline_"),
            "first_season": seasons[0] if seasons else None,
            "last_season": seasons[-1] if seasons else None,
            **{m: overall.get(m) for m in METRICS},
        })

    headline = models[HEADLINE]
    by_season = [
        {"season": r.get("season_year"), "accuracy": r.get("accuracy"), "brier": r.get("brier"),
         "brier_skill": r.get("brier_skill"), "base_rate": r.get("base_rate"), "n": r.get("n")}
        for r in sorted(headline.get("by_season") or [], key=lambda r: r.get("season_year") or 0)
    ]
    calibration = [
        {"lower": b.get("bin_lower"), "upper": b.get("bin_upper"),
         "predicted": b.get("mean_predicted"), "observed": b.get("observed_rate"),
         "n": b.get("n")}
        for b in headline.get("calibration") or []
        if b.get("n")
    ]
    return {
        "headline": HEADLINE,
        "models": rows,
        "by_season": by_season,
        "calibration": calibration,
        # The fingerprint is a hash of every prediction scored, so two reports
        # can be told apart. Published because it is the only thing on the page
        # that says WHICH run produced these numbers.
        "fingerprint": (report.get("generated_for") or {}).get("prediction_fingerprint"),
        "bins": (report.get("generated_for") or {}).get("bins"),
    }


def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--report", default=str(DEFAULT_REPORT))
    args = p.parse_args()
    accuracy = build_accuracy(args.report)
    if accuracy is None:
        raise SystemExit(f"No validation report at {args.report} -- run src/run_validation.py first")
    print(json.dumps(accuracy, indent=2))


if __name__ == "__main__":
    main()
