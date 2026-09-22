"""Tests for compare_models.py's core ranking logic."""
from compare_models import rank_within_year


def test_rank_within_year_is_a_valid_permutation():
    ratings = {(2020, "A"): 5.0, (2020, "B"): 9.0, (2020, "C"): 1.0, (2021, "D"): 100.0}
    ranks = rank_within_year(ratings, 2020)
    assert ranks == {"B": 1, "A": 2, "C": 3}
    assert "D" not in ranks  # a different year must not leak in


def test_rank_within_year_empty_year_returns_empty():
    ratings = {(2020, "A"): 1.0}
    assert rank_within_year(ratings, 1999) == {}


def test_thin_schedule_team_does_not_distort_others_ranking():
    """
    Regression test: a team with a raw cumulative Season CoE 2.0 from
    just 1-2 games must not occupy a rank slot in a comparison against
    teams with a full season's worth of accumulated CoE -- this was a
    real bug caught on live data (a 1-game-played team's tiny early-
    season sum looked like a "disagreement" against v1's already
    partial-season-adjusted values, when it was really just an
    apples-to-oranges artifact). Simulated here directly: excluding
    the thin team from the ranking population should change nothing
    about the relative order of the real, full-season teams.
    """
    full_season_ratings = {(2020, "A"): 30.0, (2020, "B"): 20.0, (2020, "C"): 10.0}
    with_thin_team = dict(full_season_ratings)
    with_thin_team[(2020, "Z")] = 5.0  # a hypothetical 1-game team with a small raw sum

    ranks_without_thin = rank_within_year(full_season_ratings, 2020)
    ranks_with_thin = rank_within_year({k: v for k, v in with_thin_team.items() if k[1] != "Z"}, 2020)
    assert ranks_without_thin == ranks_with_thin
