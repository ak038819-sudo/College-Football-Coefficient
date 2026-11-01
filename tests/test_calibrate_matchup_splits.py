"""Calibration must select using training only and fail closed on weak metrics."""
import pytest

from src.calibrate_matchup_splits import select_on_training, calibrate
from src.matchup_intelligence import reliability_label


def report(lam, adjusted, baseline, coverage=.7, n=100):
    return {"shrinkage": lam, "min_team_games": 3,
            "opponent_adjusted": {"mae": adjusted, "n": n},
            "matched_team_offense_baseline": {"mae": baseline, "n": n},
            "model_coverage": coverage}


def test_explosiveness_like_metric_stays_baseline():
    result=select_on_training([report(.5, .30, .27), report(2, .29, .27),
                               report(8, .28, .27)])
    assert result["policy"] == "baseline"


def test_training_selection_ignores_worse_coverage_and_small_gains():
    result=select_on_training([report(.5, .18, .20, .4),
                               report(2, .19, .20, .7),
                               report(8, .20, .20, .8)])
    assert result["policy"] == "adjusted"
    assert result["selected"]["lambda"] == 2


def test_small_gain_does_not_automatically_ship():
    result=select_on_training([report(2, .1999, .2)])
    assert result["policy"] == "baseline"


def test_refuse_tuning_on_holdout():
    with pytest.raises(ValueError):
        calibrate(None, training_season=2026, holdout_season=2025)


@pytest.mark.parametrize("games,opponents,expected",[
    (2,2,"insufficient"),(4,4,"provisional"),(6,6,"developing"),
    (9,8,"established"),(12,2,"insufficient"),(12,4,"provisional")])
def test_reliability_requires_diverse_history(games,opponents,expected):
    assert reliability_label(games,opponents)==expected
