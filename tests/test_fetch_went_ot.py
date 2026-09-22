"""
Tests for fetch_cfbd_games.py's went_ot detection -- a real, previously
undiscovered bug: went_ot was silently 0 for EVERY game in the entire
database because the code searched for a top-level "overtime" field
that doesn't exist on CFBD's /games endpoint. The real signal is the
length of homeLineScores/awayLineScores (regulation = 4 entries, one
per quarter; each OT period adds one more).
"""
from fetch_cfbd_games import compute_went_ot


def test_regulation_game_is_not_ot():
    g = {"homeLineScores": [7, 14, 0, 3], "awayLineScores": [3, 7, 7, 0]}
    assert compute_went_ot(g) == 0


def test_single_ot_game_is_detected():
    g = {"homeLineScores": [7, 14, 0, 3, 7], "awayLineScores": [3, 7, 7, 3, 3]}
    assert compute_went_ot(g) == 1


def test_multi_ot_game_is_detected():
    """Based on the real 2024 Hawaii Bowl: 4 quarters + 5 OT periods = 9 total."""
    g = {
        "homeLineScores": [7, 14, 0, 6, 7, 3, 2, 0, 2],
        "awayLineScores": [0, 10, 10, 7, 7, 3, 2, 0, 0],
    }
    assert compute_went_ot(g) == 1


def test_missing_line_scores_defaults_to_not_ot():
    g = {"homeLineScores": None, "awayLineScores": None}
    assert compute_went_ot(g) == 0


def test_falls_back_to_away_line_scores_if_home_missing():
    g = {"homeLineScores": None, "awayLineScores": [3, 7, 7, 3, 3]}
    assert compute_went_ot(g) == 1


def test_snake_case_field_names_also_work():
    """pick() checks both snake_case and camelCase -- confirm both paths work."""
    g = {"home_line_scores": [7, 14, 0, 3, 7]}
    assert compute_went_ot(g) == 1
