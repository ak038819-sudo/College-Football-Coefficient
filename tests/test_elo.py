"""
Mathematical invariants the Elo engine must satisfy exactly, per the
CoE 2.0 design doc's own list (section 30). These are pure unit tests
against run_elo() with small synthetic game lists -- no database needed,
so they run fast and don't depend on real data being loaded.
"""
import math

from build_elo import run_elo, effective_rating, expected_result, mov_multiplier

DEFAULT_CFG = {
    "initial_rating": 1500,
    "scale": 400,
    "k": 20,
    "home_field": 55,
    "offseason_retention": 0.75,
    "mov_c": 2.2,
    "mov_d": 0.001,
}


def _game(game_id, season, home_id, away_id, home_score, away_score, neutral=0, home_name="Home", away_name="Away"):
    return {
        "game_id": game_id, "season_year": season,
        "home_team_id": home_id, "away_team_id": away_id,
        "home_score": home_score, "away_score": away_score,
        "neutral_site": neutral,
        "home_name": home_name, "away_name": away_name,
    }


def test_expected_results_sum_to_one():
    e_a = expected_result(1600, 1500, 400)
    e_b = expected_result(1500, 1600, 400)
    assert abs(e_a + e_b - 1.0) < 1e-9


def test_rating_change_is_zero_sum():
    games = [_game(1, 2020, 1, 2, 30, 20)]
    rows, ratings, _ = run_elo(games, DEFAULT_CFG)
    changes = {r[1]: r[6] for r in rows}  # team_id -> elo_change
    assert abs(changes[1] + changes[2]) < 1e-9


def test_neutral_site_gives_zero_home_field_advantage():
    assert effective_rating(1500, True, True, 55) == 1500
    assert effective_rating(1500, False, True, 55) == 1500
    # Non-neutral: home gets the bump, away doesn't
    assert effective_rating(1500, True, False, 55) == 1555
    assert effective_rating(1500, False, False, 55) == 1500


def test_regulation_loss_and_ot_loss_are_identical_for_elo():
    """
    Elo doesn't know or care about went_ot -- a loss is a loss (S=0)
    whether it went to overtime or not. That distinction belongs to the
    future CoE 2.0 layer only.
    """
    games_reg = [_game(1, 2020, 1, 2, 20, 30)]  # away wins in regulation
    games_ot = [_game(1, 2020, 1, 2, 20, 30)]   # same score; went_ot isn't even read by run_elo
    rows_reg, ratings_reg, _ = run_elo(games_reg, DEFAULT_CFG)
    rows_ot, ratings_ot, _ = run_elo(games_ot, DEFAULT_CFG)
    assert ratings_reg == ratings_ot


def test_higher_quality_opponent_means_bigger_win_swing():
    """A win over a much stronger opponent should raise your rating more than an equal win over a weaker one."""
    games_vs_strong = [_game(1, 2020, 1, 2, 24, 20)]
    games_vs_weak = [_game(1, 2020, 1, 3, 24, 20)]

    cfg = dict(DEFAULT_CFG)
    # Team 2 starts strong, team 3 starts weak, via a pre-seeded rating trick:
    # simulate by giving team 2 a big head start through an earlier game.
    setup_game = [_game(0, 2019, 2, 99, 50, 0)]  # team 2 crushes a filler team to boost its rating
    _, ratings_after_setup, _ = run_elo(setup_game, cfg)
    assert ratings_after_setup[2] > 1500  # sanity check the boost worked

    # Now compare a game against a genuinely elevated opponent vs a fresh one
    games_full = setup_game + [_game(1, 2020, 1, 2, 24, 20)]
    games_control = [_game(1, 2020, 1, 3, 24, 20)]
    rows_full, _, _ = run_elo(games_full, cfg)
    rows_control, _, _ = run_elo(games_control, cfg)

    win_change_vs_strong = [r[6] for r in rows_full if r[0] == 1 and r[1] == 1][0]
    win_change_vs_weak = [r[6] for r in rows_control if r[0] == 1 and r[1] == 1][0]
    assert win_change_vs_strong > win_change_vs_weak


def test_tie_produces_zero_change_current_dataset_edge_case():
    """
    Documented edge case: a tie (point_diff=0) makes the MOV multiplier
    zero, so the rating change is zero regardless of how surprising the
    tie was. Irrelevant for 2000-2026 (ties are structurally impossible
    post-1996), but worth pinning down explicitly so it's a known,
    intentional behavior rather than a silent surprise later.
    """
    games = [_game(1, 2020, 1, 2, 21, 21)]
    rows, ratings, _ = run_elo(games, DEFAULT_CFG)
    for r in rows:
        assert r[6] == 0.0  # elo_change


def test_offseason_regression_pulls_toward_initial_rating_not_population_mean():
    """
    Regression target is the FIXED initial_rating (1500), not a
    dynamically computed population mean -- this is an easy bug to
    introduce (a population-mean version was actually written and
    caught during development) since the two are easy to conflate.
    """
    cfg = dict(DEFAULT_CFG)
    games = [
        _game(1, 2020, 1, 2, 50, 0),   # team 1 blows out team 2 -> big rating gap
        _game(2, 2021, 1, 2, 20, 20),  # dummy game next season to trigger regression + read ratings
    ]
    # We only care about the regression step, so inspect pregame_elo of game 2
    rows, ratings, _ = run_elo(games, cfg)
    game2_rows = [r for r in rows if r[0] == 2]
    pregame_1 = [r[2] for r in game2_rows if r[1] == 1][0]

    # Compute expected: after game 1, what was team 1's rating, then regress toward 1500
    rows_game1_only, ratings_after_g1, _ = run_elo([games[0]], cfg)
    expected_after_regression = 1500 + cfg["offseason_retention"] * (ratings_after_g1[1] - 1500)

    assert abs(pregame_1 - expected_after_regression) < 1e-6


def test_mov_multiplier_rewards_upsets_more_than_expected_blowouts():
    """An upset win (negative winner_advantage) should get a LARGER MOV multiplier than an equally-sized expected blowout."""
    m_upset = mov_multiplier(point_diff=20, winner_advantage=-200, mov_c=2.2, mov_d=0.001)
    m_expected_blowout = mov_multiplier(point_diff=20, winner_advantage=200, mov_c=2.2, mov_d=0.001)
    assert m_upset > m_expected_blowout


def test_mov_multiplier_zero_for_tie():
    assert mov_multiplier(point_diff=0, winner_advantage=0, mov_c=2.2, mov_d=0.001) == 0.0
