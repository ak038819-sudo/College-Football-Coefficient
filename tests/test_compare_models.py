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
