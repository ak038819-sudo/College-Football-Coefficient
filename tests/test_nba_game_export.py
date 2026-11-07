"""The game layer of the export: what a month file carries and what it refuses.

The fixture is one game with a hand-built curve, because what is being tested
is the PACKING — four parallel arrays and a shape row read by index — and a
fixture read off the real export would only prove the packer agrees with
itself.
"""

import json

import pytest

from src.nba import game_export, store
from src.nba.identity import game_key, team_key
from src.nba.win_probability import WinProbabilityParams

HOME_ID, AWAY_ID = 1, 2
HOME, AWAY = team_key(HOME_ID), team_key(AWAY_ID)
PROVIDER = '401999001'
KEY = game_key(PROVIDER)

PARAMS = WinProbabilityParams(
    points_per_elo=0.0328, sigma=12.18, time_exponent=0.28,
    overtime_home_win=0.54, tie_half_width=1.04)


@pytest.fixture
def league(tmp_path):
    connection = store.connect(tmp_path / 'nba.db')
    for key, team_id, name in ((HOME, HOME_ID, 'Home Club'),
                               (AWAY, AWAY_ID, 'Away Club')):
        connection.execute(
            'INSERT INTO team (team_key, team_id, name) VALUES (?, ?, ?)',
            (key, team_id, name))
    connection.execute(
        'INSERT INTO game (game_key, provider_id, season, season_label,'
        ' season_type, game_date, home_team_key, away_team_key,'
        ' home_team_id, away_team_id, home_points, away_points)'
        ' VALUES (?, ?, 2026, "2025-26", "regular", "2026-01-20", ?, ?,'
        ' ?, ?, 112, 104)',
        (KEY, PROVIDER, HOME, AWAY, HOME_ID, AWAY_ID))
    connection.execute(
        'INSERT INTO rating_model (model_id, name, config, created_at)'
        ' VALUES (1, "m", "{}", "2026-01-01")')
    connection.execute(
        'INSERT INTO rating_game (game_key, model_id, home_pregame,'
        ' away_pregame, home_expected, home_postgame, away_postgame,'
        ' rating_change) VALUES (?, 1, 1550, 1500, 0.6, 1555, 1495, 5)',
        (KEY,))
    for period, home, away in ((1, 30, 24), (2, 28, 26), (3, 26, 30),
                               (4, 28, 24)):
        connection.execute(
            'INSERT INTO game_period (game_key, period, home_points,'
            ' away_points) VALUES (?, ?, ?, ?)', (KEY, period, home, away))
    connection.execute(
        'INSERT INTO game_shape (game_key, plays, scoring_plays, periods,'
        ' overtime, lead_changes, ties, home_biggest_lead,'
        ' away_biggest_lead, home_biggest_run, away_biggest_run,'
        ' home_time_leading, away_time_leading, time_tied, reconciled)'
        ' VALUES (?, 480, 120, 4, 0, 6, 4, 14, 7, 9, 8, 1700, 1000, 180, 1)',
        (KEY,))
    for sequence, period, clock, secs, elapsed, home, away in (
            (1, 1, '12:00', 2880, 0, 0, 0),
            (2, 2, '6:00', 1080, 1800, 45, 40),
            (3, 4, '2:00', 120, 2760, 108, 100),
            (4, 4, '0:00', 0, 2880, 112, 104)):
        connection.execute(
            'INSERT INTO game_state (game_key, sequence, period, clock,'
            ' seconds_left, elapsed, home_score, away_score)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (KEY, sequence, period, clock, secs, elapsed, home, away))
    connection.execute(
        'INSERT INTO game_play (game_key, sequence, period, clock,'
        ' seconds_left, team_key, points, kind, scorer, scorer_id,'
        ' description, home_score, away_score, reason)'
        ' VALUES (?, 9, 4, "0:31", 31, ?, 2, "Jump Shot", "A. Player", 77,'
        ' "A jump shot", 112, 104, "late")', (KEY, HOME))
    connection.commit()
    return connection


class TestMonthKey:
    def test_the_month_is_the_games_own_date(self):
        assert game_export.month_key(dict(date='2026-01-20')) == '2026-01'

    def test_a_game_with_no_readable_date_has_no_month(self):
        # Filed under a guessed month it would be a game a reader cannot
        # reach, with no sign that it is missing.
        assert game_export.month_key(dict(date=None)) is None
        assert game_export.month_key(dict(date='2026')) is None
        assert game_export.month_key({}) is None

    def test_months_come_back_in_calendar_order(self):
        # Load-bearing: the season's cache token is the hash of the month
        # files concatenated in this order.
        assert game_export.month_order(
            ['2026-01', '2025-11', '2025-12']) == ['2025-11', '2025-12',
                                                   '2026-01']


class TestBuild:
    def test_a_month_payload_carries_the_game_by_its_key(self, league):
        months = game_export.build(league, 2026, model_id=1, params=PARAMS)
        assert list(months) == ['2026-01']
        payload = months['2026-01']
        assert payload['season'] == 2026
        assert payload['month'] == '2026-01'
        assert list(payload['games']) == [KEY]

    def test_the_line_score_is_a_pair_per_period(self, league):
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        assert entry['p'] == [[30, 24], [28, 26], [26, 30], [28, 24]]

    def test_the_curve_is_four_parallel_arrays_of_the_same_length(self,
                                                                 league):
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        elapsed, home, away, chance = entry['c']
        assert elapsed == [0, 1800, 2760, 2880]
        assert home == [0, 45, 108, 112]
        assert away == [0, 40, 100, 104]
        assert len(chance) == 4

    def test_a_probability_is_an_integer_per_mille_and_rises_with_a_lead(
            self, league):
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        chance = entry['c'][3]
        assert all(value is None or isinstance(value, int)
                   for value in chance)
        # 8 ahead with two minutes left is a better place to be than level
        # at tip-off.
        assert chance[2] > chance[0] > 0

    def test_the_buzzer_carries_no_probability(self, league):
        # Regulation seconds remaining is 0 there, and the model answers 1 or
        # 0 by the score alone, which is not a probability the curve should
        # draw as if the model had made it.
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        assert entry['c'][3][3] is None

    def test_an_overtime_state_carries_no_probability(self, league):
        league.execute(
            'INSERT INTO game_state (game_key, sequence, period, clock,'
            ' seconds_left, elapsed, home_score, away_score)'
            ' VALUES (?, 5, 5, "2:00", 0, 3060, 118, 114)', (KEY,))
        league.commit()
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        assert entry['c'][3][-1] is None

    def test_the_shape_row_is_read_by_the_order_the_payload_states(self,
                                                                   league):
        payload = game_export.build(league, 2026, model_id=1, params=PARAMS)['2026-01']
        entry = payload['games'][KEY]
        named = dict(zip(payload['shape_fields'], entry['sh']))
        assert named['lead_changes'] == 6
        assert named['ties'] == 4
        assert named['home_biggest_lead'] == 14
        assert named['away_biggest_run'] == 8
        assert named['home_time_leading'] == 1700
        assert named['scoring_plays'] == 120

    def test_a_key_play_names_its_side_and_its_reason_by_index(self, league):
        payload = game_export.build(league, 2026, model_id=1, params=PARAMS)['2026-01']
        play = payload['games'][KEY]['k'][0]
        assert play[0] == 1                     # the home side
        assert play[1] == 4                     # the fourth period
        assert payload['reasons'][play[8]] == 'late'

    def test_a_reduction_that_adds_up_is_marked_ok(self, league):
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        assert entry['ok'] == 1

    def test_a_reduction_that_does_not_add_up_is_marked_not_ok(self, league):
        league.execute('UPDATE game_shape SET reconciled = 0')
        league.commit()
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        assert entry['ok'] == 0

    def test_a_game_with_nothing_to_reconcile_against_is_neither(self,
                                                                 league):
        league.execute('UPDATE game_shape SET reconciled = NULL')
        league.commit()
        entry = game_export.build(league, 2026, model_id=1,
                                  params=PARAMS)['2026-01']['games'][KEY]
        assert entry['ok'] is None

    def test_a_second_rating_model_does_not_reach_the_game_page(self,
                                                                league):
        """The curve must come from the model the rest of the export used.

        `rating_game` holds a row per model. Joining on the game alone
        returned one row per model and kept whichever was visited last, so a
        game page could draw a curve from a model the schedule and the
        manifest know nothing about.
        """
        before = game_export.build(
            league, 2026, model_id=1, params=PARAMS)['2026-01']['games'][KEY]
        league.execute(
            'INSERT INTO rating_model (model_id, name, config, created_at)'
            ' VALUES (2, "other", "{}", "2026-01-02")')
        league.execute(
            'INSERT INTO rating_game (game_key, model_id, home_pregame,'
            ' away_pregame, home_expected, home_postgame, away_postgame,'
            ' rating_change) VALUES (?, 2, 1400, 1700, 0.1, 1405, 1695, 5)',
            (KEY,))
        league.commit()
        after = game_export.build(
            league, 2026, model_id=1, params=PARAMS)['2026-01']['games'][KEY]
        assert after['wp'] == before['wp']
        # And the other model is not simply ignored: asked for it, the
        # builder answers with its number, which is a different one.
        other = game_export.build(
            league, 2026, model_id=2, params=PARAMS)['2026-01']['games'][KEY]
        assert other['wp'] != before['wp']

    def test_a_season_with_no_reduced_game_builds_nothing(self, league):
        assert game_export.build(league, 2025, model_id=1, params=PARAMS) is None

    def test_a_database_with_no_game_layer_says_so(self, tmp_path):
        empty = store.connect(tmp_path / 'other.db')
        assert game_export.has_plays(empty) is False

    def test_a_database_with_one_is_not_mistaken_for_one_without(self,
                                                                 league):
        assert game_export.has_plays(league) is True


class TestEloDiff:
    def test_the_gap_is_read_back_off_the_stored_expectation(self):
        gap = 137.0
        expected = 1.0 / (1.0 + 10 ** (-gap / 400.0))
        assert game_export.elo_diff(expected) == pytest.approx(gap)

    def test_an_unusable_expectation_is_no_gap_rather_than_an_error(self):
        assert game_export.elo_diff(None) == 0.0
        assert game_export.elo_diff(0.0) == 0.0
        assert game_export.elo_diff(1.0) == 0.0


class TestCoverage:
    def test_coverage_counts_what_the_layer_reaches(self, league):
        counts = game_export.coverage(league)
        assert counts['played'] == 1
        assert counts['with_shape'] == 1
        assert counts['reconciled'] == 1
        assert counts['unreconciled'] == 0
        assert counts['states'] == 4
        assert counts['key_plays'] == 1


class TestWiredIntoTheExport:
    """`export.write` stages the month files and stamps them once a season.

    The gate is stubbed here on purpose: every floor in it is a league-sized
    number and this fixture is one game, so passing it would mean lowering a
    floor, which is how a gate's own test becomes the reason the gate stops
    working. What is under test is the writer.
    """

    def _write(self, league, tmp_path):
        from src.nba import export as export_module
        months = game_export.build(league, 2026, model_id=1, params=PARAMS)
        manifest = dict(schema_version=2, sport='nba', seasons={}, files={},
                        coverage=[], generated_at='2026-01-21T00:00:00Z')
        written = export_module.write(
            manifest, {2026: dict(season=2026, board=[], games=[])},
            {}, {}, tmp_path / 'out', gate=lambda *a, **k: [],
            games={2026: months})
        return written, tmp_path / 'out'

    def test_a_month_file_is_written_and_names_the_global_the_page_reads(
            self, league, tmp_path):
        written, out = self._write(league, tmp_path)
        body = (out / 'games' / '2026' / '2026-01.js').read_text(
            encoding='utf-8')
        assert body.startswith('window.NBA_GAMEMONTHS=')
        assert '"2026/2026-01"' in body
        assert KEY in body

    def test_the_manifest_states_one_token_a_season_and_lists_its_months(
            self, league, tmp_path):
        written, out = self._write(league, tmp_path)
        entry = written['files']['games']['seasons']['2026']
        assert entry['months'] == ['2026-01']
        assert len(entry['v']) == 10

    def test_the_token_changes_when_a_month_file_does(self, league, tmp_path):
        first, _ = self._write(league, tmp_path)
        league.execute('UPDATE game_shape SET lead_changes = 99')
        league.commit()
        second, _ = self._write(league, tmp_path / 'again')
        assert (first['files']['games']['seasons']['2026']['v']
                != second['files']['games']['seasons']['2026']['v'])

    def test_an_export_with_no_game_layer_names_no_games_at_all(self,
                                                                tmp_path):
        from src.nba import export as export_module
        manifest = dict(schema_version=2, sport='nba', seasons={}, files={},
                        coverage=[], generated_at='2026-01-21T00:00:00Z')
        written = export_module.write(
            manifest, {2026: dict(season=2026, board=[], games=[])},
            {}, {}, tmp_path / 'out', gate=lambda *a, **k: [])
        assert 'games' not in written['files']
        assert not (tmp_path / 'out' / 'games').exists()
