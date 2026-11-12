"""The NBA team statistics: what is loaded, what is divided, what is refused.

Every fixture is hand-built so each quantity can be counted by eye, and each
of the loader's traps has a test that fails when the trap is reintroduced.
Two of these are the handoff's own acceptance checks and are written as
such: a game's two opportunity grades cancel, and changing only the POINTS
leaves every opportunity number untouched.

The arithmetic tests pin their expectations to LITERALS computed by hand,
not to the module's own constants. `assert possessions == FGA + W * FTA ...`
restates the implementation and passes however wrong both are.
"""

import pytest

from src.nba import export, identity, publish, publish_gate, store
from src.nba import team_box, team_metrics as metrics

HOME_ID, AWAY_ID = 1, 2
HOME, AWAY = identity.team_key(HOME_ID), identity.team_key(AWAY_ID)
ALL_STAR_ID = 132374
GAME, SECOND, THIRD = '401500001', '401500002', '401500003'


def feed_row(game_id=GAME, team_id=HOME_ID, **kwargs):
    """A `team_box` row as the feed publishes one, as strings."""
    row = dict(game_id=game_id, season='2026', season_type='2',
               game_date='2026-01-20', team_id=str(team_id),
               opponent_team_id=str(AWAY_ID if team_id == HOME_ID
                                    else HOME_ID),
               team_home_away='home' if team_id == HOME_ID else 'away',
               team_score='112', field_goals_attempted='88',
               free_throws_attempted='25', offensive_rebounds='10',
               total_turnovers='15', turnovers='14', team_turnovers='1',
               field_goals_made='40', free_throws_made='20',
               three_point_field_goals_made='12',
               three_point_field_goals_attempted='35',
               defensive_rebounds='33', total_rebounds='43', assists='25',
               steals='7', blocks='5', fouls='18')
    row.update(kwargs)
    return row


def stored(team_key=HOME, **kwargs):
    """A row shaped like one `team_box.season_rows` returns."""
    row = dict(game_key=identity.game_key(GAME), team_key=team_key,
               opponent_key=AWAY if team_key == HOME else HOME,
               season=2026, season_type='regular', game_date='2026-01-20',
               home_away='home', points=112, field_goals_attempted=88,
               free_throws_attempted=25, offensive_rebounds=10,
               possession_turnovers=15, turnover_basis='total',
               total_turnovers=15, turnovers=14, team_turnovers=1)
    row.update(kwargs)
    return row


def _database(tmp_path, *, games=(GAME,), types=None, finals=None):
    connection = store.connect(tmp_path / 'nba.db')
    for key, team_id, name in ((HOME, HOME_ID, 'Home Club'),
                               (AWAY, AWAY_ID, 'Away Club')):
        connection.execute(
            'INSERT INTO team (team_key, team_id, name) VALUES (?, ?, ?)',
            (key, team_id, name))
    for provider in games:
        home_points, away_points = (finals or {}).get(provider, (112, 104))
        connection.execute(
            'INSERT INTO game (game_key, provider_id, season, season_label,'
            ' season_type, game_date, home_team_key, away_team_key,'
            ' home_team_id, away_team_id, home_points, away_points)'
            ' VALUES (?, ?, 2026, "2025-26", ?, "2026-01-20", ?, ?, ?, ?,'
            ' ?, ?)',
            (identity.game_key(provider), provider,
             (types or {}).get(provider, 'regular'), HOME, AWAY, HOME_ID,
             AWAY_ID, home_points, away_points))
    connection.commit()
    return connection


class TestLoadingTheBoxScores:
    def test_an_all_star_row_is_dropped_and_counted(self):
        rows = [feed_row(), feed_row(team_id=ALL_STAR_ID)]
        parsed, counts = team_box.parse(rows, 2026)
        assert [row['team_key'] for row in parsed] == [HOME]
        assert counts['exhibition'] == 1

    def test_a_missing_count_is_stored_as_null_rather_than_zero(self):
        parsed, _ = team_box.parse([feed_row(total_turnovers=None)], 2026)
        assert parsed[0]['total_turnovers'] is None
        # And the counts that ARE there survive beside it.
        assert parsed[0]['offensive_rebounds'] == 10

    def test_a_row_for_a_game_the_schedule_lacks_is_dropped(self, tmp_path):
        connection = _database(tmp_path)
        parsed, _ = team_box.parse([feed_row(), feed_row(game_id=SECOND)],
                                   2026)
        result = team_box.replace_season(connection, 2026, parsed)
        assert result['unknown_games'] == 1
        assert result['stored'] == 1

    def test_the_schedules_season_type_wins_over_the_feeds(self, tmp_path):
        # TRAP 1. The feed calls the Cup final a regular-season game; the
        # schedule knows better, and a season rate built on the feed's word
        # gives two clubs 83 regular-season games.
        connection = _database(tmp_path, games=(GAME,),
                               types={GAME: 'cup_final'})
        parsed, _ = team_box.parse([feed_row(season_type='2')], 2026)
        team_box.replace_season(connection, 2026, parsed)
        kinds = {row['season_type'] for row in
                 team_box.season_rows(connection, 2026)}
        assert kinds == {'cup_final'}

    def test_points_that_disagree_with_the_final_are_flagged_not_fixed(
            self, tmp_path):
        connection = _database(tmp_path, finals={GAME: (99, 104)})
        parsed, _ = team_box.parse([feed_row(team_score='112')], 2026)
        result = team_box.replace_season(connection, 2026, parsed)
        row = team_box.season_rows(connection, 2026)[0]
        assert result['unreconciled'] == 1
        assert row['reconciled'] == 0
        # Flagged, NOT corrected: the box score's own number is kept.
        assert row['points'] == 112

    def test_an_unplayed_game_reconciles_to_nothing_rather_than_to_false(
            self, tmp_path):
        connection = _database(tmp_path, finals={GAME: (None, None)})
        parsed, _ = team_box.parse([feed_row()], 2026)
        team_box.replace_season(connection, 2026, parsed)
        assert team_box.season_rows(connection, 2026)[0]['reconciled'] is None

    def test_re_loading_a_season_replaces_it_rather_than_doubling_it(
            self, tmp_path):
        connection = _database(tmp_path)
        parsed, _ = team_box.parse([feed_row()], 2026)
        team_box.replace_season(connection, 2026, parsed)
        team_box.replace_season(connection, 2026, parsed)
        assert len(team_box.season_rows(connection, 2026)) == 1


class TestWhichTurnoverCountIsRead:
    """TRAP 3, the one that looks right. See `src/nba/team_box.py`."""

    def test_a_season_that_copies_its_turnovers_reads_the_narrower_count(
            self):
        # 2001-02 to 2011-12: `team_turnovers` IS `turnovers`, so the feed's
        # total is the team's turnovers counted twice -- 28 here, against a
        # true 14.
        rows = [dict(total_turnovers=28, turnovers=14, team_turnovers=14)
                for _ in range(10)]
        assert team_box.turnover_basis(rows) == team_box.TEAM_ONLY_BASIS
        assert team_box.possession_turnovers(
            rows[0], team_box.TEAM_ONLY_BASIS) == 14

    def test_a_modern_season_reads_the_combined_total(self):
        rows = [dict(total_turnovers=15, turnovers=14, team_turnovers=1)
                for _ in range(10)]
        assert team_box.turnover_basis(rows) == team_box.TOTAL_BASIS
        assert team_box.possession_turnovers(
            rows[0], team_box.TOTAL_BASIS) == 15

    def test_one_coincidence_does_not_make_a_season_a_copy(self):
        # A club with one turnover and one team turnover is a real row, and
        # it must not reclassify the season around it.
        rows = [dict(total_turnovers=15, turnovers=14, team_turnovers=1)
                for _ in range(9)]
        rows.append(dict(total_turnovers=2, turnovers=1, team_turnovers=1))
        assert team_box.turnover_basis(rows) == team_box.TOTAL_BASIS

    def test_a_season_with_no_total_at_all_reads_the_narrower_count(self):
        # 2017-18, the only season in the archive missing it.
        rows = [dict(total_turnovers=None, turnovers=14, team_turnovers=None)
                for _ in range(10)]
        assert team_box.turnover_basis(rows) == team_box.TEAM_ONLY_BASIS

    def test_the_basis_is_stored_beside_the_count_it_chose(self, tmp_path):
        connection = _database(tmp_path)
        parsed, _ = team_box.parse(
            [feed_row(total_turnovers='28', turnovers='14',
                      team_turnovers='14')], 2026)
        team_box.replace_season(connection, 2026, parsed)
        row = team_box.season_rows(connection, 2026)[0]
        assert row['turnover_basis'] == 'team-only'
        assert row['possession_turnovers'] == 14
        # Both of the feed's own columns are kept, untouched.
        assert (row['total_turnovers'], row['turnovers']) == (28, 14)


class TestPossessions:
    def test_the_estimate_is_the_published_formula(self):
        # 88 + 0.44 x 25 - 10 + 15 = 104.0, computed by hand.
        assert metrics.side_possessions(stored()) == pytest.approx(104.0)

    def test_both_sides_divide_by_the_same_number(self):
        one = stored()
        other = stored(team_key=AWAY, field_goals_attempted=80,
                       free_throws_attempted=25, offensive_rebounds=8,
                       possession_turnovers=13)
        # 80 + 11 - 8 + 13 = 96.0; the shared denominator is (104 + 96) / 2.
        assert metrics.side_possessions(other) == pytest.approx(96.0)
        assert metrics.game_possessions(one, other) == pytest.approx(100.0)

    def test_one_missing_count_makes_the_game_unmeasurable_for_both_sides(
            self):
        one = stored(possession_turnovers=None)
        other = stored(team_key=AWAY)
        assert metrics.measurable(other) is True
        # The denominator is SHARED, so half of it is not a smaller answer.
        assert metrics.game_possessions(one, other) is None
        assert metrics.game_possessions(other, one) is None

    def test_a_zero_denominator_is_unavailable_rather_than_infinite(self):
        empty = stored(field_goals_attempted=0, free_throws_attempted=0,
                       offensive_rebounds=0, possession_turnovers=0)
        assert metrics.game_possessions(empty, empty) is None
        assert metrics.opportunity_rate(empty, None) is None


class TestTheSeasonNumbers:
    def _season(self, **kwargs):
        home = stored(points=110, **kwargs)
        away = stored(team_key=AWAY, points=100,
                      field_goals_attempted=80, free_throws_attempted=25,
                      offensive_rebounds=8, possession_turnovers=13)
        return [home, away]

    def test_the_components_multiply_back_to_the_index_exactly(self):
        teams, league, _ = metrics.season_rates(self._season())
        for key, totals in teams.items():
            stats = metrics.team_season_stats(totals, league)
            # Unrounded. The identity fails in the third decimal if either
            # side has been rounded first, which is why `export` rounds once
            # and nothing reads a rounded value back.
            assert stats['pxp'] == pytest.approx(
                stats['cci'] * stats['ppc'] / 100.0, rel=1e-12)

    def test_a_club_that_scores_the_league_rate_sits_at_one_hundred(self):
        teams, league, _ = metrics.season_rates(self._season())
        values = [metrics.team_season_stats(totals, league)['pxp']
                  for totals in teams.values()]
        # Two clubs, one league: the exposure-weighted mean of the two
        # indexes is 100 by construction.
        assert sum(values) / 2 == pytest.approx(100.0, abs=0.5)
        assert max(values) > 100 > min(values)

    def test_net_opportunities_is_the_published_formula(self):
        rows = self._season()
        teams, league, _ = metrics.season_rates(rows)
        stats = metrics.team_season_stats(teams[HOME], league)
        # Home 10 offensive rebounds and 15 turnovers, away 8 and 13, over a
        # shared 100 possessions: ((10 - 8) - (15 - 13)) / 100 x 100 = 0.0.
        assert stats['net_opportunities'] == pytest.approx(0.0)
        assert stats['possessions'] == pytest.approx(100.0)

    def test_a_club_with_no_measured_game_has_no_estimate(self):
        teams, league, counts = metrics.season_rates(
            [stored(possession_turnovers=None),
             stored(team_key=AWAY)])
        assert counts['measured'] == 0 and counts['unmeasured'] == 1
        assert teams == {}
        assert metrics.team_season_stats(None, league) is None

    def test_a_game_with_no_score_is_left_out_of_the_scoring_rate(self):
        # Found by Codex on PR #204. A missing score added as nothing,
        # against possessions added in full, is zero points per possession
        # rather than an absent measurement: on a two-club league it read
        # as PxP+ of 0 and 200.
        rows = [stored(points=None),
                stored(team_key=AWAY, points=104)]
        teams, _, counts = metrics.season_rates(rows)
        assert counts['unscored'] == 1 and counts['measured'] == 0
        assert teams == {}

    def test_an_unscored_game_still_carries_its_opportunity_grades(self):
        # Points enter PxP+; the opportunity arithmetic reads attempts,
        # rebounds and turnovers, so the two are separable and a game
        # without a score is not a game without a grade.
        rows = [stored(points=None),
                stored(team_key=AWAY, points=None, offensive_rebounds=4,
                       possession_turnovers=20)]
        grades = metrics.replay_expectations(rows)
        entry = grades[identity.game_key(GAME)]
        assert entry[HOME]['net'] == pytest.approx(-entry[AWAY]['net'])
        assert entry[HOME]['net'] != 0

    def test_a_game_with_one_row_is_counted_rather_than_half_measured(self):
        _, _, counts = metrics.season_rates([stored()])
        assert counts['unpaired'] == 1 and counts['games'] == 0


def _two_dates():
    """Two games on two dates: the same clubs, different box scores."""
    first = [stored(points=110),
             stored(team_key=AWAY, points=100, field_goals_attempted=80,
                    free_throws_attempted=25, offensive_rebounds=8,
                    possession_turnovers=13)]
    second = [stored(game_key=identity.game_key(SECOND),
                     game_date='2026-01-22', points=95,
                     offensive_rebounds=4, possession_turnovers=20),
              stored(game_key=identity.game_key(SECOND), team_key=AWAY,
                     game_date='2026-01-22', points=120,
                     field_goals_attempted=85, free_throws_attempted=30,
                     offensive_rebounds=15, possession_turnovers=9)]
    return first + second


class TestTheMatchupExpectation:
    def test_a_games_two_grades_are_equal_and_opposite(self):
        # The handoff's acceptance check, and the reason the denominator is
        # shared: one subtraction, two signs.
        grades = metrics.replay_expectations(_two_dates())
        for entry in grades.values():
            home, away = entry[HOME]['net'], entry[AWAY]['net']
            assert home == pytest.approx(-away)
        # And the cancellation is not the trivial one. The first game of
        # this fixture is symmetric — both clubs take the same chance edge,
        # so both grade zero — which is a correct answer and a useless
        # assertion. The second is lopsided and must grade so.
        second = grades[identity.game_key(SECOND)]
        assert second[HOME]['net'] < 0 < second[AWAY]['net']

    def test_two_clubs_that_do_equally_well_both_grade_zero(self):
        grades = metrics.replay_expectations(_two_dates())
        first = grades[identity.game_key(GAME)]
        # Home 10 offensive rebounds and 15 turnovers, away 8 and 13: the
        # same edge of -5, against the same expectation, is no edge at all.
        assert first[HOME]['net'] == pytest.approx(0.0)
        assert first[AWAY]['net'] == pytest.approx(0.0)

    def test_changing_only_the_points_leaves_the_grades_untouched(self):
        # The handoff's other acceptance check. Points enter PxP+; the
        # opportunity arithmetic reads attempts, rebounds and turnovers
        # only, so a 40-point swing may not move a single grade.
        rows = _two_dates()
        before = metrics.replay_expectations(rows)
        louder = [dict(row, points=row['points'] + 40) for row in rows]
        after = metrics.replay_expectations(louder)
        assert after == before

    def test_truncating_the_replay_leaves_the_earlier_games_identical(self):
        rows = _two_dates()
        whole = metrics.replay_expectations(rows)
        first_day = metrics.replay_expectations(
            [row for row in rows if row['game_date'] == '2026-01-20'])
        key = identity.game_key(GAME)
        assert first_day[key] == whole[key]

    def test_two_games_on_one_date_are_expected_from_the_same_state(self):
        # DATE-FROZEN. Both games of 2026-01-22 must be expected from the
        # state before that date's first game, so a replay cannot depend on
        # resolving two tip-off times within a day. Without the freeze the
        # second game would see the first, and these two numbers would
        # differ.
        rows = _two_dates() + [
            stored(game_key=identity.game_key(THIRD), game_date='2026-01-22',
                   points=101, offensive_rebounds=14,
                   possession_turnovers=11),
            stored(game_key=identity.game_key(THIRD), team_key=AWAY,
                   game_date='2026-01-22', points=99,
                   offensive_rebounds=6, possession_turnovers=17)]
        grades = metrics.replay_expectations(rows)
        same_day = [grades[identity.game_key(SECOND)],
                    grades[identity.game_key(THIRD)]]
        assert same_day[0][HOME]['expected'] == pytest.approx(
            same_day[1][HOME]['expected'])
        assert same_day[0][HOME]['prior_games'] == 1
        assert same_day[1][HOME]['prior_games'] == 1

    def test_an_expectation_never_reads_the_game_it_is_expecting(self):
        # The first game of a season is expected from the priors alone, and
        # both clubs have played nothing.
        grades = metrics.replay_expectations(_two_dates())
        first = grades[identity.game_key(GAME)]
        assert first[HOME]['prior_games'] == 0
        second = grades[identity.game_key(SECOND)]
        assert second[HOME]['prior_games'] == 1

    def test_the_expected_rate_is_the_published_formula(self):
        assert metrics.expected_rate(102.0, 99.0, 100.0) == 101.0
        assert metrics.expected_rate(None, 99.0, 100.0) is None
        assert metrics.expected_rate(102.0, 99.0, None) is None

    def test_a_league_prior_moves_the_first_expectation_and_nothing_else(
            self):
        rows = _two_dates()
        neutral = metrics.replay_expectations(rows)
        tilted = metrics.replay_expectations(rows, league_prior_rate=98.0)
        key = identity.game_key(GAME)
        assert tilted[key][HOME]['expected'] != neutral[key][HOME]['expected']
        # The grade is a difference of two residuals measured against the
        # same league, so a shift in the prior cancels out of it.
        assert tilted[key][HOME]['net'] == pytest.approx(
            neutral[key][HOME]['net'])


class TestWhatTheExportCarries:
    def test_a_board_row_carries_the_statistics_where_they_are_measured(
            self):
        built = metrics.build_season(_two_dates())
        rows = [dict(team=HOME), dict(team=AWAY)]
        export.attach_team_statistics(rows, built)
        assert rows[0]['measured_games'] == 2
        assert rows[0]['pxp'] == pytest.approx(
            round(built['teams'][HOME]['pxp'], 3))

    def test_a_club_with_no_measurement_gets_no_keys_rather_than_zeros(self):
        built = metrics.build_season(_two_dates())
        rows = [dict(team='nba:espn:team:99')]
        export.attach_team_statistics(rows, built)
        # Absent, not zero: a reader must be able to tell "no estimate" from
        # "exactly league average".
        assert 'pxp' not in rows[0] and 'net_opportunities' not in rows[0]

    def test_the_section_states_the_league_rate_and_the_pilot_priors(self):
        section = export.team_statistics_section(
            metrics.build_season(_two_dates()))
        assert section['measured_games'] == 2
        assert section['league']['points_per_possession'] > 0
        assert section['priors']['team_possessions'] == 1500.0
        assert section['estimated'] is True
        assert section['turnover_basis'] == ['total']

    def test_an_absent_layer_yields_no_section_at_all(self):
        assert export.team_statistics_section(None) is None


class TestTheGates:
    def _manifest(self, **stats):
        return dict(coverage=[dict(season=2026, team_stats=dict(
            games=1230, measured=1229, unmeasured=1, teams=30, **stats))])

    def test_a_consistent_coverage_row_passes(self):
        assert publish_gate.check_team_statistics(self._manifest()) == []

    def test_measuring_more_games_than_were_played_is_refused(self):
        manifest = dict(coverage=[dict(season=2026, team_stats=dict(
            games=10, measured=11, unmeasured=0, teams=30))])
        reasons = publish_gate.check_team_statistics(manifest)
        assert reasons and 'more games than it has' in reasons[0]

    def test_an_absent_layer_is_not_a_refusal(self):
        manifest = dict(coverage=[dict(season=2026, team_stats=None)])
        assert publish_gate.check_team_statistics(manifest) == []

    def test_a_shortfall_in_coverage_is_marked_rather_than_refused(self):
        # NOT a floor. Which games can be measured is the feed's business,
        # and refusing a correct export because a provider dropped a field
        # would take the whole site down for one missing number.
        manifest = dict(coverage=[dict(season=2026, team_stats=dict(
            games=1230, measured=3, unmeasured=1227, teams=30))])
        assert publish_gate.check_team_statistics(manifest) == []

    def test_losing_the_statistics_a_live_export_has_is_not_published(self):
        live = dict(coverage=[dict(season=2026, team_stats=dict(
            games=1230, measured=1229, unmeasured=1, teams=30))])
        incoming = dict(coverage=[dict(season=2026, team_stats=None)])
        assert publish._statistics_seasons(live) == {2026}
        assert publish._statistics_seasons(incoming) == set()

    def test_a_season_the_live_export_never_measured_is_not_a_loss(self):
        live = dict(coverage=[dict(season=2026, team_stats=None)])
        incoming = dict(coverage=[dict(season=2026, team_stats=dict(
            games=1230, measured=1229, unmeasured=1, teams=30))])
        assert not (publish._statistics_seasons(live)
                    - publish._statistics_seasons(incoming))
