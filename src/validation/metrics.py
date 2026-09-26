"""
Forecast metrics (MODEL-07).

Every function takes pairs of (predicted probability, actual outcome) where the
outcome is 1 for a win, 0 for a loss and 0.5 for a tie. Ties are genuinely half
an outcome here rather than dropped: the model did state a probability for them,
and discarding the games a model handled ambiguously would flatter it.

These are the shared definitions. src/backtest_performance_layer.py and
src/run_validation.py both import them so a Brier score always means the same
thing across the project.
"""
from __future__ import annotations

import math

EPS = 1e-12          # keeps log loss finite when a model states certainty and is wrong
DEFAULT_BINS = 10


def _clean(pairs):
    return [(float(p), float(s)) for p, s in pairs]


def brier_score(pairs) -> float | None:
    """Mean squared error of the probability. Lower is better; 0.25 is a coin flip."""
    pairs = _clean(pairs)
    if not pairs:
        return None
    return sum((p - s) ** 2 for p, s in pairs) / len(pairs)


def log_loss(pairs) -> float | None:
    """
    Mean negative log likelihood. Punishes confident mistakes far harder than
    Brier does, which is why both are reported: a model can improve its Brier
    score while getting worse at the games it was surest about.
    """
    pairs = _clean(pairs)
    if not pairs:
        return None
    return -sum(s * math.log(max(p, EPS)) + (1 - s) * math.log(max(1 - p, EPS))
                for p, s in pairs) / len(pairs)


def reliability_bins(pairs, bins: int = DEFAULT_BINS) -> list[dict]:
    """
    Predicted vs. observed frequency per probability band. A well-calibrated
    model wins about 70% of the games it called at 70%; this is where you see it
    not doing that.
    """
    pairs = _clean(pairs)
    buckets: dict = {}
    for p, s in pairs:
        idx = min(bins - 1, max(0, int(p * bins)))
        buckets.setdefault(idx, []).append((p, s))
    out = []
    for idx in sorted(buckets):
        vals = buckets[idx]
        out.append({"bin": idx, "lo": round(idx / bins, 4), "hi": round((idx + 1) / bins, 4),
                    "n": len(vals),
                    "predicted": sum(p for p, _ in vals) / len(vals),
                    "observed": sum(s for _, s in vals) / len(vals)})
    return out


def calibration_error(pairs, bins: int = DEFAULT_BINS) -> float | None:
    """Expected calibration error: |predicted - observed| per bin, weighted by bin size."""
    pairs = _clean(pairs)
    if not pairs:
        return None
    return sum(b["n"] * abs(b["predicted"] - b["observed"]) for b in reliability_bins(pairs, bins)) / len(pairs)


def sharpness(pairs) -> float | None:
    """
    Mean distance from 0.5. Reported beside calibration because the two trade
    off: always predicting 0.5 is perfectly calibrated and useless, and a
    variant that improves calibration by getting timid should be visible as such.
    """
    pairs = _clean(pairs)
    if not pairs:
        return None
    return sum(abs(p - 0.5) for p, _ in pairs) / len(pairs)


def score_predictions(pairs, bins: int = DEFAULT_BINS) -> dict:
    """Every metric at once, for a report row."""
    pairs = _clean(pairs)
    return {"n": len(pairs),
            "brier": brier_score(pairs),
            "log_loss": log_loss(pairs),
            "calibration_error": calibration_error(pairs, bins),
            "sharpness": sharpness(pairs),
            "base_rate": (sum(s for _, s in pairs) / len(pairs)) if pairs else None}
