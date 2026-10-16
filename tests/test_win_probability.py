"""
The live win-probability model: what it must do, and what it must refuse.

The number this produces sits on a scorebug beside the score, so the tests
here are about the properties a viewer would notice if they broke -- a lead
that does not get safer as the clock runs down, a comeback that reads the same
with a minute left as with a quarter left, a final score that is not certain --
plus the two shapes of missing input the live feed really serves (a 0-0 pregame
row, and a game with no clock), which must produce no number at all rather
than a number built from a guess.
"""
from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path

import pytest

import win_probability as wp

REPO = Path(__file__).resolve().parent.parent
DB = REPO / "db" / "league.db"

# Deliberately NOT the production values: a test that reads the config cannot
# tell a model change from a config change. Overtime is an even coin flip here
# so the symmetry test means what it says.
PARAMS = wp.WinProbabilityParams(points_per_elo=0.05, sigma=16.0, time_exponent=0.5,
                                 overtime_home_win=0.5, tie_half_width=1.0)


def p(elo_diff, margin, secs, params=PARAMS):
    return wp.home_win_probability(elo_diff, margin, secs, params)


def test_an_even_game_at_kickoff_is_a_coin_flip():
    assert p(0, 0, wp.REGULATION_SECONDS) == pytest.approx(0.5, abs=1e-9)


def test_the_better_team_is_favoured_and_the_worse_one_is_not():
    favourite = p(200, 0, wp.REGULATION_SECONDS)
    assert favourite > 0.5
    assert p(-200, 0, wp.REGULATION_SECONDS) == pytest.approx(1 - favourite, abs=1e-9)


def test_a_lead_gets_safer_as_the_clock_runs_down():
    """The same seven-point lead, later in the game, is worth more."""
    quarters = [p(0, 7, secs) for secs in (2700, 1800, 900, 120)]
    assert quarters == sorted(quarters)
    assert quarters[0] < quarters[-1]
    assert quarters[-1] > 0.95


def test_a_deficit_gets_harder_as_the_clock_runs_down():
    assert p(0, -7, 2700) > p(0, -7, 900) > p(0, -7, 120)


def test_more_points_is_never_worse():
    rising = [p(0, margin, 1800) for margin in range(-21, 22)]
    assert rising == sorted(rising)


def test_the_pregame_rating_fades_as_the_game_goes_on():
    """A favourite trailing by 3 is worth more early than late: at kickoff the
    rating has a whole game to express itself, with a minute left it has none."""
    assert p(300, -3, 2700) > p(300, -3, 900) > p(300, -3, 60)
    # And with the clock gone, the rating is worth nothing at all.
    assert p(300, -3, 0) == 0.0
    assert p(-300, 3, 0) == 1.0


def test_a_finished_game_is_certain_and_a_level_one_goes_to_overtime():
    assert p(0, 1, 0) == 1.0
    assert p(0, -1, 0) == 0.0
    assert p(0, 0, 0) == PARAMS.overtime_home_win
    tilted = wp.WinProbabilityParams(points_per_elo=0.05, sigma=16.0, time_exponent=0.5,
                                     overtime_home_win=0.6, tie_half_width=1.0)
    assert p(0, 0, 0, tilted) == 0.6


def test_a_level_game_in_the_last_seconds_is_about_an_overtime_coin_flip():
    assert p(0, 0, 5) == pytest.approx(0.5, abs=0.01)


def test_every_probability_stays_a_probability():
    for elo_diff in (-900, 0, 900):
        for margin in (-70, -3, 0, 3, 70):
            for secs in (0, 1, 60, 900, 3600):
                value = p(elo_diff, margin, secs)
                assert 0.0 <= value <= 1.0


def test_the_tie_band_is_what_puts_mass_on_a_level_game():
    """Widening the band moves mass from the leader to overtime, and the
    narrow geometric band puts far less mass on a tie than the fitted one."""
    narrow = wp.WinProbabilityParams(points_per_elo=0.05, sigma=16.0, time_exponent=0.5,
                                     overtime_home_win=0.5, tie_half_width=0.5)
    wide = wp.WinProbabilityParams(points_per_elo=0.05, sigma=16.0, time_exponent=0.5,
                                   overtime_home_win=0.5, tie_half_width=2.0)
    assert wp.tie_mass(0.0, 16.0, wide) > wp.tie_mass(0.0, 16.0, narrow)
    # A favourite's lead is slightly less safe when more of the distribution is
    # allowed to end level, because that mass becomes an overtime coin flip.
    assert p(0, 3, 300, wide) < p(0, 3, 300, narrow)


def test_the_time_exponent_is_what_controls_late_game_variance():
    """Regression guard for the one parameter that is assumed rather than
    fitted: a smaller exponent leaves more uncertainty late, so a comeback has
    to be worth more. If this stops being true the fit has nothing to fit."""
    walk = PARAMS
    slower = wp.WinProbabilityParams(points_per_elo=0.05, sigma=16.0, time_exponent=0.3,
                                     overtime_home_win=0.5, tie_half_width=1.0)
    assert p(0, -7, 300, slower) > p(0, -7, 300, walk)


def test_a_bigger_sigma_makes_every_lead_less_safe():
    loose = wp.WinProbabilityParams(points_per_elo=0.05, sigma=24.0, time_exponent=0.5,
                                    overtime_home_win=0.5, tie_half_width=1.0)
    assert p(0, 10, 1800, loose) < p(0, 10, 1800)


class TestTheClock:
    def test_a_period_and_clock_become_regulation_seconds(self):
        assert wp.seconds_remaining(1, "15:00") == 3600
        assert wp.seconds_remaining(2, "15:00") == 2700
        assert wp.seconds_remaining(3, "7:30") == 1350
        assert wp.seconds_remaining(4, "0:00") == 0

    def test_overtime_has_no_regulation_left(self):
        assert wp.seconds_remaining(5, "15:00") == 0
        assert wp.seconds_remaining(6, None) == 0

    def test_an_hour_field_and_a_bare_number_are_both_read(self):
        assert wp.seconds_remaining(4, "00:07:30") == 450
        assert wp.seconds_remaining(4, 450) == 450

    def test_what_cannot_be_read_is_none_and_not_zero(self):
        """Zero means the game is over, so guessing it would end a live game."""
        for period, clock in ((None, "15:00"), (0, "15:00"), (1, "quarter"), (1, None),
                              (1, "15:00:00:00"), (1, "15:75"), (1, ""), ("Q1", "15:00")):
            assert wp.seconds_remaining(period, clock) is None

    def test_a_clock_longer_than_a_period_cannot_widen_the_model(self):
        assert wp.seconds_remaining(1, "20:00") == 3600
        assert wp.fraction_remaining(99_999) == 1.0
        assert wp.fraction_remaining(-5) == 0.0


class TestTheFeed:
    """The shapes `src/fetch_live_scores.py` really publishes."""

    def test_a_scheduled_game_has_no_live_number(self):
        assert wp.live_probability_from_feed(100, None, None, None, None, "scheduled", PARAMS) is None

    def test_a_pregame_zero_zero_row_is_not_a_tie_game(self):
        """CFBD publishes 0-0 before kickoff; the status is the only honest signal."""
        assert wp.live_probability_from_feed(100, 0, 0, None, None, "scheduled", PARAMS) is None

    def test_a_game_under_way_with_no_clock_gives_no_number(self):
        assert wp.live_probability_from_feed(100, 14, 10, None, None, "in_progress", PARAMS) is None
        assert wp.live_probability_from_feed(100, 14, 10, 2, "garbage", "in_progress", PARAMS) is None

    def test_a_game_under_way_with_a_clock_gives_one(self):
        value = wp.live_probability_from_feed(0, 14, 7, 3, "7:30", "in_progress", PARAMS)
        assert value == pytest.approx(p(0, 7, 1350))

    def test_a_completed_game_is_decided_by_its_score_alone(self):
        assert wp.live_probability_from_feed(-400, 31, 28, None, None, "completed", PARAMS) == 1.0
        assert wp.live_probability_from_feed(400, 28, 31, None, None, "completed", PARAMS) == 0.0
        assert wp.live_probability_from_feed(0, 28, 28, None, None, "completed", PARAMS) == 0.5

    def test_scores_that_are_not_whole_numbers_are_refused(self):
        for home, away in ((None, 7), (7, None), ("14", 7), (True, 7), (14.5, 7)):
            assert wp.live_probability_from_feed(0, home, away, 2, "7:30", "in_progress", PARAMS) is None


class TestTheConfiguredModel:
    def test_the_production_config_loads_and_is_usable(self):
        params = wp.WinProbabilityParams.from_config()
        assert 0.02 < params.points_per_elo < 0.12      # about 20 Elo points per point
        assert 10 < params.sigma < 25                   # points of final margin
        assert 0.3 < params.overtime_home_win < 0.7
        assert 0 < params.tie_half_width < 7
        assert params.time_exponent > 0

    def test_a_config_with_no_live_section_is_an_error_not_a_default(self, tmp_path):
        path = tmp_path / "model_config.json"
        path.write_text(json.dumps({"elo": {"k": 45}}), encoding="utf-8")
        with pytest.raises(ValueError):
            wp.WinProbabilityParams.from_config(path)

    def test_the_config_path_is_resolved_at_call_time(self, tmp_path, monkeypatch):
        """A def-time default argument cannot be monkeypatched, which has made
        two tests in this repo pass vacuously. Pin that it is resolved inside."""
        path = tmp_path / "model_config.json"
        path.write_text(json.dumps({"live_win_probability": {
            "points_per_elo": 0.04, "sigma": 15.0, "time_exponent": 0.5,
            "overtime_home_win": 0.5, "tie_half_width": 1.0}}), encoding="utf-8")
        monkeypatch.setattr(wp, "CONFIG_PATH", path)
        assert wp.WinProbabilityParams.from_config().points_per_elo == 0.04

    @pytest.mark.parametrize("bad", [
        {"sigma": 0.0}, {"sigma": -1.0}, {"points_per_elo": 0.0}, {"points_per_elo": -0.05},
        {"time_exponent": 0.0}, {"time_exponent": 3.0}, {"overtime_home_win": 1.5},
        {"tie_half_width": -1.0}, {"tie_half_width": 7.0}, {"sigma": float("inf")},
    ])
    def test_parameters_that_would_publish_a_meaningless_number_are_refused(self, bad):
        values = {"points_per_elo": 0.05, "sigma": 16.0, "time_exponent": 0.5,
                  "overtime_home_win": 0.5, "tie_half_width": 1.0, **bad}
        with pytest.raises(ValueError):
            wp.WinProbabilityParams.from_dict(values)


@pytest.mark.skipif(not DB.exists(), reason="needs db/league.db (run_pipeline.py)")
def test_at_kickoff_this_model_agrees_with_the_ratings_it_is_built_on():
    """The live number replaces a pregame number the viewer can still see.

    With no score on the board it must say about what the rating engine's own
    expectation says; a live model that disagrees with the pregame chance beside
    it is a bug on the page, not a modelling choice.
    """
    params = wp.WinProbabilityParams.from_config()
    with sqlite3.connect(f"file:{DB}?mode=ro", uri=True) as conn:
        rows = conn.execute("""
            SELECT elo_diff_adjusted, elo_expectation FROM elo_game_history
             WHERE elo_diff_adjusted IS NOT NULL LIMIT 20000""").fetchall()
    assert rows, "elo_game_history is empty; build_elo.py has not run"
    gaps = [abs(wp.pregame_home_win_probability(diff, params) - expectation)
            for diff, expectation in rows]
    assert sum(gaps) / len(gaps) < 0.02
    assert max(gaps) < 0.10


@pytest.mark.skipif(not DB.exists(), reason="needs db/league.db (run_pipeline.py)")
def test_the_model_is_calibrated_on_games_it_was_not_fitted_through():
    """Real games, bucketed by what this model said before kickoff.

    Not a restatement of the fit: the fit minimises squared margin error, while
    this asks whether the probabilities that come out of it match how often the
    home team actually won.
    """
    params = wp.WinProbabilityParams.from_config()
    with sqlite3.connect(f"file:{DB}?mode=ro", uri=True) as conn:
        rows = conn.execute("""
            SELECT e.elo_diff_adjusted, g.home_score - g.away_score
              FROM games g JOIN elo_game_history e
                ON e.game_id = g.game_id AND e.team_id = g.home_team_id
             WHERE g.home_score IS NOT NULL AND e.elo_diff_adjusted IS NOT NULL
               AND g.season_year >= 2018""").fetchall()
    assert len(rows) > 2000
    buckets: dict[int, list] = {}
    for diff, margin in rows:
        prob = wp.pregame_home_win_probability(diff, params)
        buckets.setdefault(min(9, int(prob * 10)), []).append(
            (prob, 0.5 if margin == 0 else (1.0 if margin > 0 else 0.0)))
    for index, pairs in sorted(buckets.items()):
        if len(pairs) < 200:
            continue
        predicted = sum(x for x, _ in pairs) / len(pairs)
        actual = sum(y for _, y in pairs) / len(pairs)
        # Three standard errors of the bucket's own mean, floored so a big
        # bucket is not held to a tighter tolerance than the model's shape.
        tolerance = max(0.06, 3 * math.sqrt(0.25 / len(pairs)))
        assert abs(predicted - actual) < tolerance, (index, len(pairs), predicted, actual)
