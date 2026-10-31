"""Guard against accidentally introducing a second adjusted-stat model."""
import pytest

from src import build_adjusted_stats as established
from src.matchup_intelligence import Observation, fit_profiles, project_matchup


def test_profiles_use_exact_existing_solver_for_venue_and_weighted_effects():
    rows = [
        Observation("1", "2", .50, 72, .5),
        Observation("2", "1", .36, 65, -.5),
        Observation("1", "3", .54, 60, -.5),
        Observation("3", "1", .40, 55, .5),
        Observation("2", "3", .44, 69, .5),
        Observation("3", "2", .48, 63, -.5),
    ]
    source = established.solve(
        [(r.team, r.opponent, r.value, r.weight, r.side) for r in rows],
        established.LAMBDA["success_rate"],
    )
    profiles, baseline = fit_profiles(
        rows, min_games=2, shrinkage=established.LAMBDA["success_rate"])
    assert baseline == pytest.approx(source["mu"])
    for name in ("1", "2", "3"):
        assert profiles[name].offense == pytest.approx(source["off"][name])
        assert profiles[name].defense_allowed == pytest.approx(source["def"][name])


def test_projected_value_uses_same_offense_defense_components():
    rows = [
        Observation("1", "2", .6), Observation("2", "1", .4),
        Observation("1", "2", .7), Observation("2", "1", .3),
        Observation("1", "2", .5), Observation("2", "1", .5),
    ]
    ratings, baseline = fit_profiles(rows)
    p = project_matchup(ratings, baseline, "1", "2", venue_adjustment=.01)
    assert p.projected_value == pytest.approx(
        baseline + ratings["1"].offense + ratings["2"].defense_allowed + .01)
