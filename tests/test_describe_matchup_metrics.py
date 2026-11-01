"""Metric integrity: descriptive decomposition, unit schedules and coverage."""
import math

import pytest

from src.describe_matchup_metrics import describe_units
from src.matchup_intelligence import Observation, fit_profiles


def fixture():
    # Stronger A and B attacks; different opponents and home/away exposures.
    games = [
        ("A","B",.65,.5), ("B","A",.42,-.5),
        ("A","C",.68,-.5), ("C","A",.35,.5),
        ("A","D",.72,.5), ("D","A",.36,-.5),
        ("B","C",.56,.5), ("C","B",.43,-.5),
        ("B","D",.59,-.5), ("D","B",.39,.5),
        ("C","D",.44,.5), ("D","C",.38,-.5),
    ]
    return [Observation(a,b,v,weight=50 + i,side=side)
            for i,(a,b,v,side) in enumerate(games)]


def test_description_matches_production_solver_and_decomposes_every_observation():
    rows=fixture()
    d=describe_units(rows, shrinkage=200)
    expected, mu=fit_profiles(rows, shrinkage=200)
    assert d["baseline"] == pytest.approx(mu)
    assert d["rated_teams"] == 4
    for name in expected:
        r=d["teams"][name]
        assert r["offense"]["opponent_adjusted"] == pytest.approx(
            mu+expected[name].offense)
        assert r["defense"]["opponent_adjusted"] == pytest.approx(
            mu+expected[name].defense_allowed)
        assert r["offensive_percentile"] == pytest.approx(expected[name].offensive_percentile)
        assert r["defensive_percentile"] == pytest.approx(expected[name].defensive_percentile)
        for side in ("offense","defense"):
            x=r[side]
            assert x["raw"] == pytest.approx(
                x["opponent_adjusted"] + x["opponent_effect"] +
                x["venue_effect"] + x["ridge_residual"])


def test_schedule_strength_is_opposing_unit_quality_not_a_prediction():
    obs=[Observation("A","X",0.5), Observation("A","Y",.6),
         Observation("A","Z",.7),
         Observation("X","A",.35),Observation("Y","A",.40),Observation("Z","A",.45),
         Observation("X","Y",.6),Observation("Y","Z",.5),
         Observation("Z","X",.3),
         Observation("Y","X",.44), Observation("Z","Y",.41),Observation("X","Z",.51)]
    r=describe_units(obs, shrinkage=2)
    assert r["teams"]["A"]["offense"]["games"] == 3
    assert r["teams"]["A"]["opponents"] == 3
    assert r["teams"]["A"]["offensive_schedule_percentile"] is not None
    assert r["teams"]["A"]["reliability"] == "provisional"
    assert "projected_value" not in str(r)


def test_insufficient_and_true_zero_are_not_fabricated():
    r=describe_units([Observation("A","B",0.0)], min_games=3)
    assert r["teams"]["A"]["offense"]["raw"] == 0
    assert r["teams"]["A"]["offensive_percentile"] is None
    assert r["teams"]["B"]["offense"] is None
    assert r["teams"]["B"]["rating_eligible"] is False
    assert r["teams"]["A"]["offensive_schedule_percentile"] is None


def test_lower_is_better_direction_reverses_difficulty():
    rows=fixture()
    normal=describe_units(rows, shrinkage=2)
    inverted=describe_units(rows, shrinkage=2, higher_is_better=False)
    for team in normal["teams"]:
        assert normal["teams"][team]["opposing_offense_difficulty"] == pytest.approx(
            -inverted["teams"][team]["opposing_offense_difficulty"])


def test_invalid_weights_and_empty_data():
    assert describe_units([])["status"] == "no_data"
    with pytest.raises(ValueError):
        describe_units([Observation("A","B",math.inf)])
    with pytest.raises(ValueError):
        describe_units([Observation("A","B",.5,weight=0)])
