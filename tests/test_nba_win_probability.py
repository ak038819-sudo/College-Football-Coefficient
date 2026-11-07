"""The NBA's win-probability model: its clock, its parameters, and the fit.

What these tests are for is that the SHARED model is used with the NBA's own
regulation length. Football plays 3600 seconds and the NBA 2880, and the
failure mode is silent: a state with 1440 seconds left would be read as 40%
of a game remaining instead of half of one, and every number on every game
page would be wrong by an amount no reader could spot.
"""

import json
import math

import pytest

from src import win_probability as shared
from src.nba import fit_win_probability as fit
from src.nba import win_probability as nba

PARAMS = nba.WinProbabilityParams(
    points_per_elo=0.0328, sigma=13.0, time_exponent=0.45,
    overtime_home_win=0.6, tie_half_width=0.4)


class TestClock:
    def test_regulation_is_four_twelve_minute_periods(self):
        assert nba.PERIOD_SECONDS == 720
        assert nba.REGULATION_PERIODS == 4
        assert nba.REGULATION_SECONDS == 2880

    def test_the_nba_clock_is_not_footballs(self):
        # The one assertion that would catch the whole class of mistake this
        # module exists to avoid.
        assert nba.REGULATION_SECONDS != shared.REGULATION_SECONDS

    def test_half_a_game_left_is_half_a_game_left(self):
        # 1440 seconds is half of 2880. The shared model's own default would
        # read it as 40% of a game, which is what passing the league's
        # regulation length prevents.
        half = nba.home_win_probability(0.0, 0.0, 1440, PARAMS)
        quarter = nba.home_win_probability(0.0, 0.0, 720, PARAMS)
        with_football = shared.home_win_probability(0.0, 0.0, 1440, PARAMS)
        assert half != pytest.approx(with_football)
        assert half == pytest.approx(shared.home_win_probability(
            0.0, 0.0, 1440, PARAMS, nba.REGULATION_SECONDS))
        assert quarter != pytest.approx(half)

    def test_tip_off_is_a_whole_game_to_play(self):
        assert nba.pregame_home_win_probability(0.0, PARAMS) \
            == pytest.approx(nba.home_win_probability(
                0.0, 0.0, nba.REGULATION_SECONDS, PARAMS))

    def test_the_shared_model_still_answers_footballs_question(self):
        # Passing no regulation length must leave the three leagues that
        # share this module exactly where they were.
        assert shared.home_win_probability(0.0, 0.0, 1800, PARAMS) \
            == pytest.approx(shared.home_win_probability(
                0.0, 0.0, 1800, PARAMS, 3600))


class TestModel:
    def test_a_lead_is_worth_more_as_the_clock_runs_down(self):
        early = nba.home_win_probability(0.0, 6, 2400, PARAMS)
        late = nba.home_win_probability(0.0, 6, 120, PARAMS)
        assert 0.5 < early < late < 1.0

    def test_regulation_over_and_level_is_the_overtime_share(self):
        assert nba.home_win_probability(0.0, 0, 0, PARAMS) \
            == pytest.approx(PARAMS.overtime_home_win)

    def test_the_better_side_is_favoured_at_tip_off(self):
        assert nba.pregame_home_win_probability(200.0, PARAMS) > 0.5
        assert nba.pregame_home_win_probability(-200.0, PARAMS) < 0.5


class TestConfig:
    def test_the_committed_parameters_load_and_validate(self):
        params = nba.load_params()
        assert params.sigma > 0
        assert 0 < params.time_exponent <= 2
        assert 0 <= params.overtime_home_win <= 1

    def test_the_path_resolves_inside_the_call(self, tmp_path):
        # A def-time default binds at import and cannot be redirected, a trap
        # this repository has fallen into five times.
        target = tmp_path / 'params.json'
        target.write_text(json.dumps(dict(win_probability=dict(
            points_per_elo=0.03, sigma=12.5, time_exponent=0.5,
            overtime_home_win=0.55, tie_half_width=0.3))), encoding='utf-8')
        assert nba.load_params(target).sigma == 12.5

    def test_a_configuration_with_no_tie_band_is_refused(self):
        # Required rather than defaulted: the shared model's geometric half a
        # point is the right fallback for a caller that has never measured
        # it, and the wrong one here, because the fit solves it.
        with pytest.raises(KeyError):
            nba.params_from(dict(points_per_elo=0.03, sigma=12.0,
                                 time_exponent=0.5, overtime_home_win=0.5))

    def test_the_committed_file_says_what_it_was_measured_on(self):
        config = json.loads(nba.CONFIG_PATH.read_text(encoding='utf-8'))
        validation = config['validation']
        assert validation['holdout_states'] > 100000
        assert validation['holdout']['brier'] \
            < validation['holdout']['pregame_brier']


class TestFit:
    def test_the_fast_score_is_the_published_models_own_score(self):
        """The search's inner loop must be a speed-up, not a second model."""
        states = [dict(elo_diff=diff, margin=margin, seconds_left=secs,
                       home_result=result)
                  for diff in (-150.0, 0.0, 220.0)
                  for margin in (-12, -1, 0, 3, 18)
                  for secs in (2880, 1440, 300, 12)
                  for result in (0.0, 1.0)]
        prepared = fit.prepare(states, PARAMS.points_per_elo)
        slow = fit.brier(states, PARAMS)
        fast = fit.fast_brier(prepared, PARAMS.sigma, PARAMS.time_exponent,
                              PARAMS.overtime_home_win, PARAMS.tie_half_width)
        assert fast == pytest.approx(slow, abs=1e-12)

    def test_the_rating_gap_is_read_back_off_the_stored_expectation(self):
        # The engine applied the home-court term when it wrote the
        # expectation, so inverting the logistic recovers the gap it used.
        gap = 137.0
        expected = 1.0 / (1.0 + 10 ** (-gap / 400.0))
        assert fit.elo_diff_from_expectation(expected) == pytest.approx(gap)
        assert fit.elo_diff_from_expectation(None) is None
        assert fit.elo_diff_from_expectation(0.0) is None
        assert fit.elo_diff_from_expectation(1.0) is None

    def test_the_overtime_share_is_counted_over_level_games(self):
        rows = [dict(regulation_margin=0, home_result=1.0),
                dict(regulation_margin=0, home_result=0.0),
                dict(regulation_margin=0, home_result=1.0),
                dict(regulation_margin=7, home_result=1.0)]
        share, level = fit.overtime_home_share(rows)
        assert level == 3
        assert share == pytest.approx(2 / 3)

    def test_a_search_that_lands_on_the_edge_says_so(self):
        states = [dict(elo_diff=0.0, margin=0, seconds_left=1440,
                       home_result=1.0)]
        prepared = fit.prepare(states, 0.0328)
        # One state that the home side won: the best exponent is whichever
        # end of the grid makes the probability largest, which is an edge.
        _, _, at_edge, curve = fit.search_time_exponent(
            prepared, 13.0, 0.6, 0.4, low=0.2, high=1.0, step=0.1)
        assert at_edge is True
        assert len(curve) == 9

    def test_the_search_recovers_an_exponent_it_was_not_told(self):
        """States generated at a known exponent, and the search finds it.

        The target of each state is the model's OWN probability at 0.37,
        which a Brier score is minimised by matching, so the exponent that
        made the states is the exponent that scores best. Landing on 0.37
        rather than on a multiple of the coarse 0.02 step is also the only
        thing that distinguishes the fine pass from the coarse one — a fine
        pass that never ran would answer 0.36 or 0.38.
        """
        truth = nba.WinProbabilityParams(
            points_per_elo=0.0328, sigma=13.0, time_exponent=0.37,
            overtime_home_win=0.6, tie_half_width=0.4)
        states = [dict(elo_diff=diff, margin=margin, seconds_left=secs,
                       home_result=nba.home_win_probability(
                           diff, margin, secs, truth))
                  for diff in (-180.0, 0.0, 160.0)
                  for margin in (-14, -6, -1, 0, 2, 9, 21)
                  for secs in (2800, 2200, 1600, 1000, 500, 160, 40)]
        prepared = fit.prepare(states, truth.points_per_elo)
        exponent, _, at_edge, _ = fit.search_time_exponent(
            prepared, truth.sigma, truth.overtime_home_win,
            truth.tie_half_width)
        assert at_edge is False
        assert exponent == pytest.approx(0.37, abs=0.004)

    def test_the_tie_band_is_solved_against_how_often_games_end_level(self):
        rows = [dict(elo_diff=0.0) for _ in range(200)]
        wide = fit.solve_tie_half_width(rows, 0.0328, 13.0, 0.10)
        narrow = fit.solve_tie_half_width(rows, 0.0328, 13.0, 0.02)
        assert wide > narrow > 0
        # The band's own mass at tip-off is what it was solved to match.
        params = nba.WinProbabilityParams(
            points_per_elo=0.0328, sigma=13.0, time_exponent=0.5,
            overtime_home_win=0.5, tie_half_width=narrow)
        assert shared.tie_mass(0.0, 13.0, params) == pytest.approx(0.02,
                                                                   abs=1e-3)

    def test_sigma_is_the_spread_of_the_regulation_margin(self):
        rows = [dict(regulation_margin=margin, elo_diff=0.0)
                for margin in (-10, -5, 0, 5, 10)]
        assert fit.fit_sigma(rows, 0.0328) == pytest.approx(
            math.sqrt(sum(value ** 2 for value in (-10, -5, 0, 5, 10)) / 5))
