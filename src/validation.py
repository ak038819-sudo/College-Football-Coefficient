#!/usr/bin/env python3
"""
Shared model-validation metrics and walk-forward reporting (MODEL-07).

Before this module, scoring lived in three places that each grew the metrics
they happened to need: calibrate_elo.py had its own Brier loop,
calibrate_hybrid_weight.py had a near-copy of it, and
backtest_performance_layer.py had a private `score()` with Brier, log loss and
a calibration summary. Nothing reported per-season behaviour, and nothing
wrote a machine-readable record, so two runs could only be compared by reading
two consoles. This module owns the metrics; those scripts now call it.

Everything here is pure: it takes (predicted probability, observed outcome)
pairs and returns numbers. No database, no config, no file writing -- that is
run_validation.py's job. It is therefore testable against hand-computed values,
which tests/test_validation.py does.

## What "walk-forward" means here, precisely

This module never trains anything, so it cannot enforce out-of-sample-ness on
its own. It scores predictions that were ALREADY formed. The guarantee has to
come from the caller, and in this repository it does: Elo's expectation for a
game is computed from ratings built only from earlier games, the hybrid
expectation adds only a frozen prior-season CoE, and the xSRDiff curve stores
one fold per season fitted strictly on earlier seasons. So a single
chronological pass yields genuinely out-of-sample predictions and no separate
train/test split is needed -- the argument calibrate_elo.py has always made.

What this module adds is the ability to SEE that, instead of asserting it:
`walk_forward_report` breaks every metric down by season, so a model that
looks good overall while drifting, or one that is really only being carried by
its later seasons, shows up as a row rather than hiding inside a mean. The
early seasons where every team still sits at a flat 1500 are visible the same
way, which is why `from_season` exists rather than a silent burn-in rule.

## Outcome convention

An outcome is 1.0 (the predicted side won), 0.0 (it lost) or 0.5 (tie).
Predictions are always scored from ONE side of a game -- the home team's, by
convention -- because the away side carries identical information
(E_away = 1 - E_home, S_away = 1 - S_home).

Scoring both sides is not merely redundant. Brier, log loss and accuracy are
symmetric and would come out unchanged, but the binned calibration error would
not: the bin holding the favourites is not the mirror image of the bin holding
the underdogs, so a both-sides run reports a different number for the same
model. One side per game, everywhere, is what makes two reports comparable.

## Metrics, and what each one catches that the others miss

    brier            mean squared error. The headline number; the one the
                     existing calibration scripts already optimise.
    log_loss         punishes confident mistakes far harder than Brier does.
                     A model that is usually right but occasionally certain
                     and wrong looks fine on Brier and bad here.
    accuracy         share of games whose favourite won (a tie scores half).
                     Reported because it is the number people ask for, not
                     because it is a good target -- it throws away confidence.
    base_rate        observed rate of the scored side winning. Context for
                     everything below it.
    brier_skill      1 - brier / brier(always predict base_rate). Positive
                     means the model beats the dumbest honest baseline;
                     0.0 means it has added nothing.
    calibration      do games predicted at 70% happen 70% of the time?
                     `calibration_error` is the n-weighted mean gap across
                     bins (the same quantity backtest_performance_layer.py
                     reported), and `calibration_bins` is the table behind
                     it, so a model that is well calibrated in the middle
                     and wrong at the extremes is legible instead of averaged
                     away.

A note on comparing two variants: a lower Brier is not automatically a better
model if the two were produced by engines with different rating volatility.
backtest_performance_layer.py reports mean |dElo| alongside these for exactly
that reason.

## Rank diagnostics

The rank functions at the bottom serve the achievement side rather than the
prediction side. CoE measures achievement, and the design doc is explicit that
it must not be tuned for prediction accuracy -- so it cannot be judged by the
metrics above at all. What can be checked is whether it behaves like a
coherent measure: whether a season's ordering is stable year over year
(`rank_stability`), how far two systems disagree on the same population
(`rank_agreement`), and whether it is associated with things it should or
should not be associated with (`association`) -- a measure that rewards
farming weak opponents would show a negative association with schedule
strength. These are diagnostics to read, not thresholds to pass; the spec's
own philosophy is that a disagreement is not automatically a bug.
"""
from __future__ import annotations

import math
from collections import defaultdict

EPS = 1e-12

# (predicted probability, observed outcome in {0.0, 0.5, 1.0})
Pair = tuple[float, float]
# (season, predicted probability, observed outcome)
SeasonPair = tuple[int, float, float]

DEFAULT_BINS = 10

# Every key `score()` returns, so callers can build empty rows of the right
# shape without restating the list.
METRIC_KEYS = ("n", "brier", "log_loss", "accuracy", "base_rate",
               "brier_skill", "calibration_error")


def _mean(values) -> float | None:
    values = list(values)
    return (sum(values) / len(values)) if values else None


def brier_score(pairs: list[Pair]) -> float | None:
    """Mean squared error between predicted probability and outcome. Lower is
    better; 0.25 is what predicting 0.5 for everything scores."""
    return _mean((p - s) ** 2 for p, s in pairs)


def log_loss(pairs: list[Pair]) -> float | None:
    """
    Mean negative log likelihood. Lower is better; ln(2) = 0.693 is what
    predicting 0.5 for everything scores.

    Probabilities are clamped away from 0 and 1 by EPS, because a single
    confident miss at exactly 0.0 would otherwise make the whole run infinite
    and destroy the comparison it was meant to inform. A tie (s = 0.5) splits
    its likelihood across both terms, which falls out of the same formula.
    """
    return _mean(
        -(s * math.log(max(p, EPS)) + (1 - s) * math.log(max(1 - p, EPS)))
        for p, s in pairs
    )


def accuracy(pairs: list[Pair]) -> float | None:
    """
    Share of games the model got right, counting a p of exactly 0.5 or an
    actual tie as half credit -- there is no defensible way to call either one
    right or wrong, and rounding it to a win would quietly flatter the model.
    """
    def credit(p: float, s: float) -> float:
        if p == 0.5 or s == 0.5:
            return 0.5
        return 1.0 if (p > 0.5) == (s > 0.5) else 0.0

    return _mean(credit(p, s) for p, s in pairs)


def base_rate(pairs: list[Pair]) -> float | None:
    """Observed rate at which the scored side won."""
    return _mean(s for _, s in pairs)


def brier_skill_score(pairs: list[Pair]) -> float | None:
    """
    Improvement over always predicting the observed base rate: 1.0 is perfect,
    0.0 is no better than that baseline, negative is worse than it.

    The baseline uses the base rate measured on these same games, which makes
    it deliberately hard to beat -- it is handed the one fact a real forecaster
    would not have known in advance. A model that clears it has earned it.
    """
    if not pairs:
        return None
    rate = base_rate(pairs)
    reference = _mean((rate - s) ** 2 for _, s in pairs)
    if reference is None or reference < EPS:
        return None  # every outcome identical: no spread to explain
    return 1.0 - brier_score(pairs) / reference


def calibration_bins(pairs: list[Pair], bins: int = DEFAULT_BINS) -> list[dict]:
    """
    Bucket predictions by probability and compare what was predicted against
    what happened. Empty buckets are omitted rather than reported as zeros,
    which would read as "predicted 0.05, observed 0.0" for a bucket holding
    nothing at all.

    `gap` is observed minus predicted, signed: positive means the model was
    underconfident in that band, negative means overconfident.
    """
    if bins < 1:
        raise ValueError("bins must be at least 1")
    grouped: dict[int, list[Pair]] = defaultdict(list)
    for p, s in pairs:
        # A prediction of exactly 1.0 belongs in the top bucket, not a
        # bucket of its own past the end of the range.
        grouped[min(bins - 1, int(p * bins))].append((p, s))

    out = []
    for index in sorted(grouped):
        bucket = grouped[index]
        predicted = _mean(p for p, _ in bucket)
        observed = _mean(s for _, s in bucket)
        out.append({
            "bin_lower": index / bins,
            "bin_upper": (index + 1) / bins,
            "n": len(bucket),
            "mean_predicted": predicted,
            "observed_rate": observed,
            "gap": observed - predicted,
        })
    return out


def calibration_error(pairs: list[Pair], bins: int = DEFAULT_BINS) -> float | None:
    """
    n-weighted mean absolute bin gap -- the expected calibration error. This is
    the same quantity backtest_performance_layer.py already reported, kept
    numerically identical so its existing output stays comparable.
    """
    if not pairs:
        return None
    return sum(b["n"] * abs(b["gap"]) for b in calibration_bins(pairs, bins)) / len(pairs)


def score(pairs: list[Pair], bins: int = DEFAULT_BINS) -> dict:
    """
    Every metric for one set of predictions. An empty set returns the same keys
    with None rather than raising, so a report can carry a row for a variant
    that had nothing to score (a season with no games, a fallback path that
    never fired) instead of omitting it and looking complete.
    """
    pairs = list(pairs)
    if not pairs:
        return {key: (0 if key == "n" else None) for key in METRIC_KEYS}
    return {
        "n": len(pairs),
        "brier": brier_score(pairs),
        "log_loss": log_loss(pairs),
        "accuracy": accuracy(pairs),
        "base_rate": base_rate(pairs),
        "brier_skill": brier_skill_score(pairs),
        "calibration_error": calibration_error(pairs, bins),
    }


def walk_forward_report(model: str, entries: list[SeasonPair], *,
                        bins: int = DEFAULT_BINS,
                        from_season: int | None = None) -> dict:
    """
    Score one model overall and season by season.

    `entries` are (season, predicted, observed) triples whose predictions were
    formed before the game they describe -- see the module docstring on what
    this module can and cannot guarantee about that.

    `from_season` drops earlier seasons entirely, for the Elo burn-in where
    every team still starts at a flat 1500 and K has little signal to work
    with. It is recorded in the report so a number can never be quoted without
    the window it was measured over.
    """
    kept = [(season, p, s) for season, p, s in entries
            if from_season is None or season >= from_season]
    pairs = [(p, s) for _, p, s in kept]

    by_season: dict[int, list[Pair]] = defaultdict(list)
    for season, p, s in kept:
        by_season[season].append((p, s))

    return {
        "model": model,
        "from_season": from_season,
        "seasons": sorted(by_season),
        "n_dropped_before_from_season": len(entries) - len(kept),
        "overall": score(pairs, bins),
        "by_season": [{"season_year": season, **score(by_season[season], bins)}
                      for season in sorted(by_season)],
        "calibration": calibration_bins(pairs, bins),
    }


def spearman(xs: list[float], ys: list[float]) -> float | None:
    """
    Rank correlation of two equal-length sequences, with ties averaged.

    Spearman rather than Pearson throughout the rank diagnostics because these
    measures live on different scales -- an Elo rating and a summed Game CoE
    have no common unit -- and only their ordering is comparable. Returns None
    when either side has no spread, since a correlation with a constant is
    undefined rather than zero.
    """
    if len(xs) != len(ys):
        raise ValueError("spearman needs two equal-length sequences")
    if len(xs) < 2:
        return None

    rx, ry = _tied_ranks(xs), _tied_ranks(ys)
    mx, my = _mean(rx), _mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    if dx < EPS or dy < EPS:
        return None
    return num / (dx * dy)


def _tied_ranks(values: list[float]) -> list[float]:
    """Ascending ranks, 1-based, with tied values sharing their mean rank."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        shared = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[order[k]] = shared
        i = j + 1
    return ranks


def _ranks_descending(values: dict) -> dict:
    """{key: rank} with rank 1 the highest value -- the convention the site and
    compare_models.py already use."""
    return {key: i + 1 for i, (key, _) in
            enumerate(sorted(values.items(), key=lambda kv: -kv[1]))}


def rank_agreement(left: dict, right: dict) -> dict:
    """
    How far two measures disagree about the same population, over the keys they
    share. `mean_abs_rank_change` is in ranks, which is what a reader of the
    site would notice; `spearman` is the scale-free version of the same thing.

    A disagreement is information, not an error -- the spec says so directly.
    This reports its size and leaves the judgement to a person.
    """
    shared = sorted(set(left) & set(right))
    if not shared:
        return {"n": 0, "spearman": None, "mean_abs_rank_change": None,
                "max_abs_rank_change": None}
    lr, rr = _ranks_descending(left), _ranks_descending(right)
    changes = [abs(lr[key] - rr[key]) for key in shared]
    return {
        "n": len(shared),
        "spearman": spearman([left[key] for key in shared], [right[key] for key in shared]),
        "mean_abs_rank_change": _mean(changes),
        "max_abs_rank_change": max(changes),
    }


def rank_stability(values_by_season: dict) -> list[dict]:
    """
    Year-over-year stability of a measure, one row per adjacent season pair
    that shares teams.

    An achievement measure should move when teams actually get better or worse
    and otherwise hold still. Neither extreme is the goal: a measure that
    reshuffles the whole table every year is noise, and one that never moves is
    not measuring the season. Adjacent seasons only, because a jump across a
    gap year would mix a real change with a missing one.
    """
    seasons = sorted(values_by_season)
    return [{"from_season": a, "to_season": b,
             **rank_agreement(values_by_season[a], values_by_season[b])}
            for a, b in zip(seasons, seasons[1:])
            if b == a + 1]


def association(values: dict, against: dict, label: str) -> dict:
    """
    Rank correlation between a measure and something it should (or should not)
    track -- schedule strength faced, games won, and so on -- over shared keys.

    Read the sign, not just the size: an achievement measure that correlates
    NEGATIVELY with the strength of the schedule a team faced is rewarding
    farming weak opponents, which is the failure mode the design doc names.
    """
    shared = sorted(set(values) & set(against))
    return {
        "against": label,
        "n": len(shared),
        "spearman": spearman([values[key] for key in shared],
                             [against[key] for key in shared]) if shared else None,
    }
