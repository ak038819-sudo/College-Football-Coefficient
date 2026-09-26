"""
Walk-forward folds (MODEL-07).

The guide's hard rule: no random split where chronology matters. A model that
trained on 2019 must never be scored on 2018, and a rating formed after a game
must never be used to judge that game. Every split this module produces is a
contiguous block of past seasons predicting one future season.

Two things are worth separating, because they are different kinds of leak:

  fold construction   which seasons a configuration may LEARN from. Enforced
                      here by only ever handing back earlier seasons.
  engine chronology   whether the rating used to score a game predates it.
                      That is a property of the engine, not of the split, and
                      it is why an Elo backtest can be scored in one pass:
                      walking games in order already makes every expectation
                      out-of-sample. assert_chronological() is the guard for it.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Fold:
    """One walk-forward split: train on `train`, score on `test`."""
    test_season: int
    train: tuple = field(default=())

    @property
    def trained_through(self) -> int | None:
        return max(self.train) if self.train else None

    def as_dict(self) -> dict:
        return {"test_season": self.test_season, "trained_through": self.trained_through,
                "train_seasons": list(self.train)}


def season_folds(seasons, min_train_seasons: int = 5, window: int | None = None) -> list[Fold]:
    """
    One fold per season that has at least `min_train_seasons` earlier seasons.

    `window` caps how far back training reaches (a rolling window); None means
    every prior season (an expanding window). Either way training is strictly
    earlier than the test season -- that is the invariant, not a convention.
    """
    ordered = sorted({int(s) for s in seasons})
    folds = []
    for i, season in enumerate(ordered):
        prior = ordered[:i]
        if window is not None:
            prior = prior[-window:]
        if len(prior) >= min_train_seasons:
            folds.append(Fold(test_season=season, train=tuple(prior)))
    return folds


def assert_chronological(observations) -> None:
    """
    Raise if any observation was scored with information from its own season or
    later. `observations` are (test_season, trained_through) pairs; a
    trained_through of None means "no training data was used", which is safe.

    Called by the harness before it writes a report, so a leaking configuration
    fails loudly instead of quietly producing a flattering number.
    """
    bad = [(t, k) for t, k in observations if k is not None and k >= t]
    if bad:
        raise ValueError(
            "future data leaked into a walk-forward fold: "
            + ", ".join(f"season {t} trained through {k}" for t, k in sorted(bad)[:5])
            + (f" (and {len(bad) - 5} more)" if len(bad) > 5 else ""))
