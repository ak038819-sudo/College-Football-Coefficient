"""The NFL rating engine, and the one thing it does differently.

A tie is a result here. The basketball engine declines a level final because a
tied college basketball game means the data is wrong; the NFL has tied 16
times since 1999 and each of those games says something precise. Several
tests below exist only to stop a future copy-paste from the basketball module
quietly reinstating that exclusion.
"""

import math

import pytest

from src.nfl import elo


def game(key, season, home, away, home_points, away_points, *, neutral=0):
    return dict(game_key=key, season=season, game_date=f'{season}-09-10',
                neutral_site=neutral, home_team_key=home, away_team_key=away,
                home_points=home_points, away_points=away_points)


RESULT_ONLY = dict(k=20, home_advantage=50, offseason_retention=0.8,
                   margin_model='result_only')


class TestOutcome:
    def test_a_tie_is_half(self):
        assert elo.outcome(20, 20) == 0.5

    def test_an_unplayed_game_is_none_not_half(self):
        # None and 0.5 must never be confused: one is no information and the
        # other is the most informative result there is.
        assert elo.outcome(None, None) is None
        assert elo.outcome(17, None) is None


class TestReplay:
    def test_a_tie_moves_both_ratings_toward_each_other(self):
        rows, ratings = elo.replay(
            [game('g1', 2026, 'home', 'away', 20, 20)], RESULT_ONLY)
        assert rows[0]['counted'] == 1
        # The home team was favoured by the venue term, so a tie costs it.
        assert ratings['home'] < 1500 < ratings['away']
        assert rows[0]['rating_change'] == pytest.approx(
            20 * (0.5 - rows[0]['home_expected']))

    def test_a_tie_is_counted_and_an_unplayed_game_is_not(self):
        rows, _ = elo.replay([
            game('tie', 2026, 'home', 'away', 13, 13),
            game('future', 2026, 'home', 'away', None, None),
        ], RESULT_ONLY)
        assert [row['counted'] for row in rows] == [1, 0]
        assert rows[1]['rating_change'] == 0.0

    def test_a_tie_takes_no_margin_multiplier(self):
        config = dict(RESULT_ONLY, margin_model='mov', reference_margin=11.7)
        rows, _ = elo.replay(
            [game('g1', 2026, 'home', 'away', 20, 20)], config)
        assert rows[0]['multiplier'] == 1.0

    def test_a_neutral_site_game_gets_no_venue_term(self):
        home_rows, _ = elo.replay(
            [game('g1', 2026, 'home', 'away', 20, 17)], RESULT_ONLY)
        neutral_rows, _ = elo.replay(
            [game('g1', 2026, 'home', 'away', 20, 17, neutral=1)],
            RESULT_ONLY)
        assert home_rows[0]['home_expected'] > neutral_rows[0]['home_expected']
        assert neutral_rows[0]['home_expected'] == 0.5

    def test_ratings_regress_once_at_a_season_boundary(self):
        rows, ratings = elo.replay([
            game('g1', 2025, 'home', 'away', 30, 0),
            game('g2', 2026, 'home', 'away', 30, 0),
        ], dict(RESULT_ONLY, offseason_retention=0.5))
        # The second season's pregame rating is halfway back to 1500 from
        # where the first season left it.
        assert rows[1]['home_pregame'] == pytest.approx(
            1500 + (rows[0]['home_postgame'] - 1500) * 0.5)

    def test_the_chain_links_one_game_to_the_next(self):
        rows, _ = elo.replay([
            game('g1', 2026, 'a', 'b', 24, 10),
            game('g2', 2026, 'a', 'c', 24, 10),
        ], RESULT_ONLY)
        assert rows[1]['home_pregame'] == rows[0]['home_postgame']

    def test_margin_is_measured_not_assumed(self):
        with pytest.raises(ValueError, match='measured'):
            elo.margin_multiplier('mov', margin=10, elo_diff=0,
                                  reference_margin=0)

    def test_a_tie_can_never_reach_the_margin_multiplier(self):
        with pytest.raises(ValueError, match='tie is not a win'):
            elo.margin_multiplier('mov', margin=0, elo_diff=0,
                                  reference_margin=11.7)

    def test_an_underperforming_favourite_does_not_raise_a_negative_base(self):
        # mov_adjusted subtracts the implied margin, which can go negative;
        # a negative base to a fractional exponent is a complex number in
        # Python rather than an error.
        value = elo.margin_multiplier(
            'mov_adjusted', margin=1, elo_diff=600, reference_margin=11.7,
            exponent=0.5, points_per_elo=0.0527)
        assert value == pytest.approx(0.25)


class TestScoring:
    def _rows(self, pairs):
        return [dict(counted=1, home_expected=p, outcome=o) for p, o in pairs]

    def test_brier_scores_a_tie_against_a_half(self):
        rows = self._rows([(0.5, 0.5)])
        assert elo.brier(rows) == 0.0

    def test_a_confident_call_on_a_tied_game_is_penalised(self):
        assert elo.brier(self._rows([(0.9, 0.5)])) == pytest.approx(0.16)

    def test_log_loss_handles_a_half_outcome(self):
        value = elo.log_loss(self._rows([(0.5, 0.5)]))
        assert value == pytest.approx(math.log(2))

    def test_accuracy_gives_a_tie_half_credit(self):
        assert elo.accuracy(self._rows([(0.9, 0.5)])) == 0.5

    def test_calibration_reads_observed_as_the_mean_outcome(self):
        error, table = elo.calibration(
            self._rows([(0.55, 1.0), (0.55, 0.0)]))
        assert table[0]['observed'] == 0.5
        assert error == pytest.approx(0.05)

    def test_scoring_nothing_raises_rather_than_returning_zero(self):
        with pytest.raises(ValueError, match='No counted game'):
            elo.brier([dict(counted=0, home_expected=0.5, outcome=1.0)])


class TestFittedConfiguration:
    """The committed configuration, and the claim attached to it."""

    def test_the_committed_claim_is_pinned_to_the_parameters(self):
        from src.nfl.build_ratings import check_validation, load_config
        payload = load_config()
        assert check_validation(payload)

    def test_editing_a_parameter_without_refitting_is_refused(self):
        from src.nfl.build_ratings import check_validation, load_config
        payload = load_config()
        payload['elo']['k'] = payload['elo']['k'] + 1
        with pytest.raises(ValueError, match='hash'):
            check_validation(payload)

    def test_the_committed_model_beats_picking_the_home_team(self):
        from src.nfl.build_ratings import MAX_HOLDOUT_BRIER, load_config
        holdout = load_config()['validation']['holdout']
        assert holdout['brier'] < MAX_HOLDOUT_BRIER
        assert holdout['games'] >= 500

    def test_the_fit_and_the_holdout_do_not_overlap(self):
        from src.nfl.build_ratings import load_config
        validation = load_config()['validation']
        assert not (set(validation['fit_seasons'])
                    & set(validation['holdout_seasons']))

    def test_nothing_was_inherited_from_the_other_two_leagues(self):
        """At least one parameter must differ from each shipped model.

        Not a style rule: two of these three leagues landing on identical
        parameters would mean the fit never ran, and that is invisible in a
        config file.
        """
        from src.nfl.build_ratings import load_config
        from src.nfl.fit_elo import INHERITED
        elo_config = load_config()['elo']
        for _, parameters in INHERITED:
            assert any(elo_config[key] != value
                       for key, value in parameters.items())
