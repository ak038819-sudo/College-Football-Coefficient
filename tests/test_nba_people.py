"""The NBA people layer: who is stored, what counts as a game, what is not.

The fixtures are hand-built so that every quantity can be counted by eye.
Each of the four traps the loader exists to undo has a test of its own, and
each of those tests was checked against the mutation that reintroduces the
trap: the All-Star row, minutes as the signal that a man played, the feed's
regular-season-only statistics, and a game the schedule does not carry.
"""

import pytest

from src.nba import identity, people, store

HOME_ID, AWAY_ID = 1, 2
HOME, AWAY = identity.team_key(HOME_ID), identity.team_key(AWAY_ID)
# The 2026 All-Star sides, which the box score files under season type 2.
ALL_STAR_ID = 132374
PLAYER_ID, OTHER_ID = 1966, 3975
PLAYER = identity.entity_key('player', PLAYER_ID)
OTHER = identity.entity_key('player', OTHER_ID)
GAME, SECOND = '401500001', '401500002'


def core_row(athlete_id=PLAYER_ID, **kwargs):
    row = dict(athlete_id=str(athlete_id), full_name='A Player',
               display_name='A Player', short_name='A. Player',
               first_name='A', last_name='Player',
               position_name='Forward', position_abbreviation='F',
               jersey='23', display_height="6' 9\"",
               display_weight='250 lbs',
               date_of_birth='1984-12-30T08:00Z', birth_city='Akron',
               birth_state='OH', birth_country='USA', college_id='42',
               draft_year='2003', draft_round='1', draft_selection='1',
               experience_years='23')
    row.update(kwargs)
    return row


def box_row(game_id=GAME, athlete_id=PLAYER_ID, minutes='33.0', **kwargs):
    row = dict(game_id=game_id, athlete_id=str(athlete_id), season='2026',
               season_type='2', game_date='2026-01-20', team_id=str(HOME_ID),
               opponent_team_id=str(AWAY_ID), home_away='home',
               minutes=minutes, starter='True', did_not_play='False',
               points='30', rebounds='8', offensive_rebounds='1',
               defensive_rebounds='7', assists='9', steals='1', blocks='1',
               turnovers='3', fouls='2', field_goals_made='11',
               field_goals_attempted='20',
               three_point_field_goals_made='2',
               three_point_field_goals_attempted='5',
               free_throws_made='6', free_throws_attempted='8',
               plus_minus='7')
    row.update(kwargs)
    return row


def stat_row(athlete_id=PLAYER_ID, games=1, category='averages',
             stat_name='gamesPlayed'):
    return dict(athlete_id=str(athlete_id), category=category,
                stat_name=stat_name, value=str(float(games)))


class TestReadingABio:
    def test_height_and_weight_are_read_from_the_display_strings(self):
        assert people.height_inches("6' 9\"") == 81
        assert people.height_inches("7' 0\"") == 84
        assert people.weight_pounds('250 lbs') == 250

    def test_an_unreadable_height_is_none_rather_than_zero(self):
        # Zero inches would sort a man to the top of a height column and
        # print "0'0"" on his page.
        assert people.height_inches(None) is None
        assert people.height_inches('') is None
        assert people.height_inches('tall') is None
        assert people.weight_pounds('lbs') is None

    def test_a_person_is_keyed_by_the_providers_athlete_id(self):
        assert people.person_key('1966') == 'nba:espn:player:1966'
        assert people.person_key(1966.0) == 'nba:espn:player:1966'

    def test_a_line_with_no_athlete_id_has_no_key(self):
        # 34 of the archive's 851,463 lines are like this. A made-up key
        # would put them all on one page.
        assert people.person_key(None) is None
        assert people.person_key('') is None

    def test_a_bio_carries_the_draft_and_the_birthplace(self):
        parsed = people.parse_people([core_row()])
        assert len(parsed) == 1
        person = parsed[0]
        assert person['person_key'] == PLAYER
        assert person['full_name'] == 'A Player'
        assert person['height_inches'] == 81
        assert person['weight_pounds'] == 250
        assert person['draft_year'] == 2003
        assert person['draft_selection'] == 1
        assert person['birth_city'] == 'Akron'

    def test_a_row_with_no_name_is_not_a_person(self):
        assert people.parse_people([core_row(full_name=None,
                                             display_name=None)]) == []

    def test_the_last_row_for_an_id_wins(self):
        parsed = people.parse_people([core_row(jersey='6'),
                                      core_row(jersey='23')])
        assert [person['jersey'] for person in parsed] == ['23']


class TestWhatCountsAsALine:
    def test_the_all_star_game_is_not_a_regular_season_game(self):
        """TRAP 1. LeBron James has 83 rows for 2017-18 under season type 2
        and the 83rd is for Team LeBron, so a naive sum makes him play 83
        regular-season games."""
        lines, counts = people.parse_games(
            [box_row(), box_row(game_id=SECOND, team_id=str(ALL_STAR_ID))],
            2026)
        assert len(lines) == 1
        assert counts['exhibition'] == 1
        assert lines[0]['game_key'] == identity.game_key(GAME)

    def test_minutes_say_whether_he_played_not_the_flag(self):
        """TRAP 2. `did_not_play` is true for 299 rows in 2009-10 while
        10,767 rows that season carry no minutes, with the flag false and
        `active` false."""
        lines, _ = people.parse_games(
            [box_row(minutes=None, did_not_play='False', points=None),
             box_row(game_id=SECOND, minutes='5.0', did_not_play='True')],
            2026)
        played = {line['game_key']: line['played'] for line in lines}
        assert played[identity.game_key(GAME)] == 0
        assert played[identity.game_key(SECOND)] == 1

    def test_a_line_with_no_player_or_no_game_is_counted_and_dropped(self):
        lines, counts = people.parse_games(
            [box_row(athlete_id=None), box_row(game_id=None), box_row()],
            2026)
        assert len(lines) == 1
        assert counts['no_person'] == 1
        assert counts['no_game'] == 1

    def test_one_man_has_one_line_in_one_game(self):
        lines, counts = people.parse_games([box_row(), box_row(points='99')],
                                           2026)
        assert len(lines) == 1
        assert counts['duplicate'] == 1
        # The FIRST line stands. A second line for the same man in the same
        # game is the feed repeating itself, not a second performance.
        assert lines[0]['points'] == 30

    def test_the_counted_columns_are_read_as_numbers(self):
        line = people.parse_games([box_row()], 2026)[0][0]
        assert line['points'] == 30
        assert line['threes_attempted'] == 5
        assert line['plus_minus'] == 7
        assert line['minutes'] == 33.0
        assert line['started'] == 1
        assert line['season_type'] == 'regular'
        assert line['opponent_key'] == AWAY


class TestRollingUp:
    def test_only_played_games_are_counted(self):
        lines, _ = people.parse_games(
            [box_row(), box_row(game_id=SECOND, minutes=None, points=None)],
            2026)
        rolled = people.roll_up(lines)
        assert len(rolled) == 1
        assert rolled[0]['games'] == 1
        assert rolled[0]['points'] == 30

    def test_a_man_who_changed_club_has_a_row_for_each(self):
        lines, _ = people.parse_games(
            [box_row(), box_row(game_id=SECOND, team_id=str(AWAY_ID))], 2026)
        rolled = people.roll_up(lines)
        assert sorted(row['team_key'] for row in rolled) == [HOME, AWAY]
        assert all(row['games'] == 1 for row in rolled)

    def test_the_postseason_is_its_own_row(self):
        lines, _ = people.parse_games(
            [box_row(), box_row(game_id=SECOND, season_type='3')], 2026)
        rolled = people.roll_up(lines)
        assert sorted(row['season_type'] for row in rolled) == ['postseason',
                                                                'regular']

    def test_the_feeds_count_is_compared_across_a_mans_clubs(self):
        """TRAP 3/4. The feed's count is for a man's whole regular season,
        so a man who played one game for each of two clubs matches a feed
        that says two, and the comparison is made per man, not per club."""
        lines, _ = people.parse_games(
            [box_row(), box_row(game_id=SECOND, team_id=str(AWAY_ID))], 2026)
        rolled = people.roll_up(lines, feed={PLAYER_ID: 2})
        assert all(row['matches_feed'] == 1 for row in rolled)
        assert {row['feed_games'] for row in rolled} == {2}

    def test_a_feed_that_counts_more_games_is_stated_not_reconciled(self):
        lines, _ = people.parse_games([box_row()], 2026)
        rolled = people.roll_up(lines, feed={PLAYER_ID: 39})
        assert rolled[0]['games'] == 1
        assert rolled[0]['feed_games'] == 39
        assert rolled[0]['matches_feed'] == 0

    def test_the_feed_says_nothing_about_the_postseason(self):
        """The feed's season statistics are regular season only, so a
        postseason rollup carries no comparison rather than a failed one."""
        lines, _ = people.parse_games([box_row(season_type='3')], 2026)
        rolled = people.roll_up(lines, feed={PLAYER_ID: 1})
        assert rolled[0]['feed_games'] is None
        assert rolled[0]['matches_feed'] is None

    def test_the_feeds_games_played_is_the_only_statistic_read_from_it(self):
        rows = [stat_row(games=5),
                stat_row(games=99, category='totals', stat_name='points'),
                stat_row(games=99, category='averages',
                         stat_name='avgPoints')]
        assert people.feed_games(rows) == {PLAYER_ID: 5}


def _store(tmp_path, *, games=(GAME,)):
    connection = store.connect(tmp_path / 'nba.db')
    for key, team_id, name in ((HOME, HOME_ID, 'Home Club'),
                               (AWAY, AWAY_ID, 'Away Club')):
        connection.execute(
            'INSERT INTO team (team_key, team_id, name) VALUES (?, ?, ?)',
            (key, team_id, name))
    for provider in games:
        connection.execute(
            'INSERT INTO game (game_key, provider_id, season, season_label,'
            ' season_type, game_date, home_team_key, away_team_key,'
            ' home_team_id, away_team_id, home_points, away_points)'
            ' VALUES (?, ?, 2026, "2025-26", "regular", "2026-01-20", ?, ?,'
            ' ?, ?, 112, 104)',
            (identity.game_key(provider), provider, HOME, AWAY, HOME_ID,
             AWAY_ID))
    connection.commit()
    return connection


class TestStoring:
    def test_a_season_is_stored_and_re_storing_it_changes_nothing(self,
                                                                  tmp_path):
        connection = _store(tmp_path)
        lines, _ = people.parse_games([box_row()], 2026)
        first = people.replace_season(connection, 2026,
                                      people.parse_people([core_row()]),
                                      lines)
        second = people.replace_season(connection, 2026,
                                       people.parse_people([core_row()]),
                                       lines)
        assert first == second
        assert connection.execute(
            'SELECT COUNT(*) FROM person_game').fetchone()[0] == 1
        assert connection.execute(
            'SELECT COUNT(*) FROM person').fetchone()[0] == 1

    def test_a_line_that_has_stopped_existing_stops_existing_here(self,
                                                                  tmp_path):
        connection = _store(tmp_path, games=(GAME, SECOND))
        both, _ = people.parse_games([box_row(), box_row(game_id=SECOND)],
                                     2026)
        people.replace_season(connection, 2026,
                              people.parse_people([core_row()]), both)
        one, _ = people.parse_games([box_row()], 2026)
        people.replace_season(connection, 2026,
                              people.parse_people([core_row()]), one)
        assert connection.execute(
            'SELECT COUNT(*) FROM person_game').fetchone()[0] == 1

    def test_a_line_for_a_game_the_schedule_does_not_have_is_dropped(
            self, tmp_path):
        connection = _store(tmp_path)
        lines, _ = people.parse_games([box_row(), box_row(game_id=SECOND)],
                                      2026)
        stored = people.replace_season(connection, 2026,
                                       people.parse_people([core_row()]),
                                       lines)
        assert stored['lines'] == 1
        assert stored['unknown_games'] == 1

    def test_the_rollups_count_only_the_lines_that_were_stored(self,
                                                              tmp_path):
        """Rolling up before filtering made a man's season count games the
        schedule does not have, which his own game log could never show."""
        connection = _store(tmp_path)
        lines, _ = people.parse_games([box_row(), box_row(game_id=SECOND)],
                                      2026)
        people.replace_season(connection, 2026,
                              people.parse_people([core_row()]), lines)
        assert connection.execute(
            'SELECT games FROM person_season').fetchone()[0] == 1

    def test_a_line_whose_man_has_no_bio_is_counted_and_dropped(self,
                                                               tmp_path):
        connection = _store(tmp_path)
        lines, _ = people.parse_games([box_row(athlete_id=OTHER_ID)], 2026)
        stored = people.replace_season(connection, 2026,
                                       people.parse_people([core_row()]),
                                       lines)
        assert stored['orphans'] == 1
        assert stored['lines'] == 0

    def test_a_span_is_the_seasons_the_archive_carries_not_a_career(
            self, tmp_path):
        connection = _store(tmp_path)
        lines, _ = people.parse_games([box_row()], 2026)
        people.replace_season(connection, 2026,
                              people.parse_people([core_row()]), lines)
        assert people.refresh_spans(connection) == 1
        row = connection.execute(
            'SELECT first_season, last_season FROM person').fetchone()
        assert (row['first_season'], row['last_season']) == (2026, 2026)

    def test_a_man_who_never_played_has_no_span(self, tmp_path):
        connection = _store(tmp_path)
        lines, _ = people.parse_games([box_row(minutes=None, points=None)],
                                      2026)
        people.replace_season(connection, 2026,
                              people.parse_people([core_row()]), lines)
        people.refresh_spans(connection)
        row = connection.execute(
            'SELECT first_season, last_season FROM person').fetchone()
        assert (row['first_season'], row['last_season']) == (None, None)


class TestTwoIdsOneMan:
    def test_the_same_name_and_birthday_are_linked_both_ways(self, tmp_path):
        connection = _store(tmp_path)
        people.replace_season(
            connection, 2026,
            people.parse_people([core_row(), core_row(athlete_id=OTHER_ID)]),
            [])
        assert people.link_same_people(connection) == 2
        rows = connection.execute(
            'SELECT person_key, other_key, basis FROM person_alias'
            ' ORDER BY person_key').fetchall()
        assert {(row['person_key'], row['other_key']) for row in rows} == {
            (PLAYER, OTHER), (OTHER, PLAYER)}
        assert {row['basis'] for row in rows} == {'name and date of birth'}

    def test_two_men_of_one_name_and_different_birthdays_are_not_linked(
            self, tmp_path):
        connection = _store(tmp_path)
        people.replace_season(
            connection, 2026,
            people.parse_people([
                core_row(),
                core_row(athlete_id=OTHER_ID,
                         date_of_birth='1991-05-21T07:00Z')]),
            [])
        assert people.link_same_people(connection) == 0

    def test_a_man_with_no_birthday_is_never_linked_on_his_name_alone(
            self, tmp_path):
        connection = _store(tmp_path)
        people.replace_season(
            connection, 2026,
            people.parse_people([
                core_row(date_of_birth=None),
                core_row(athlete_id=OTHER_ID, date_of_birth=None)]),
            [])
        assert people.link_same_people(connection) == 0

    def test_relinking_replaces_rather_than_doubles(self, tmp_path):
        connection = _store(tmp_path)
        people.replace_season(
            connection, 2026,
            people.parse_people([core_row(), core_row(athlete_id=OTHER_ID)]),
            [])
        people.link_same_people(connection)
        assert people.link_same_people(connection) == 2
        assert connection.execute(
            'SELECT COUNT(*) FROM person_alias').fetchone()[0] == 2


class TestIngest:
    def _given(self, **overrides):
        given = dict(player_core=[core_row()], player_box=[box_row()],
                     player_season_stats=[stat_row(games=1)])
        given.update(overrides)
        return given

    def test_a_season_is_fetched_reduced_and_reported(self, tmp_path):
        connection = _store(tmp_path)
        report = people.ingest_season(connection, 2026,
                                      given=self._given())
        assert report['season'] == 2026
        assert report['lines'] == 1
        assert report['played'] == 1
        assert report['people'] == 1

    def test_a_season_with_no_statistics_file_still_stores_its_lines(
            self, tmp_path):
        """An optional dataset that will not fetch leaves its part empty, the
        rule the schedule ingest already follows for standings."""
        connection = _store(tmp_path)
        report = people.ingest_season(
            connection, 2026, given=self._given(player_season_stats=[]))
        assert report['lines'] == 1
        assert connection.execute(
            'SELECT feed_games FROM person_season').fetchone()[0] is None

    def test_a_run_records_a_snapshot_against_the_season(self, tmp_path):
        connection = _store(tmp_path)
        people.ingest_season(connection, 2026, given=self._given())
        row = connection.execute(
            'SELECT kind, season FROM snapshot ORDER BY snapshot_id DESC'
        ).fetchone()
        assert (row['kind'], row['season']) == ('people', 2026)

    def test_ingest_links_and_spans_after_the_last_season(self, tmp_path):
        connection = _store(tmp_path)
        # `ingest` fetches, so it is driven here through `ingest_season`'s
        # own `given` hook by way of a stub that records the seasons asked
        # for and stores the fixture for each.
        asked = []
        original = people.ingest_season

        def fake(connection, season, **kwargs):
            asked.append(season)
            return original(connection, season, given=self._given())

        people.ingest_season = fake
        try:
            results = people.ingest(connection, [2026])
        finally:
            people.ingest_season = original
        assert asked == [2026]
        assert results[-1] == dict(season=None, spans=1, aliases=0)

    def test_the_command_line_entry_point_runs(self, tmp_path, monkeypatch,
                                               capsys):
        """A module the refresh invokes needs a test that INVOKES it: two
        src/nba modules reached main with a broken `main()` because every
        test drove the functions under it. Only the fetch is stubbed here;
        `ingest`, the spans, the links and every number main prints are the
        real ones."""
        _store(tmp_path).close()
        given = self._given()
        real = people.ingest_season
        monkeypatch.setattr(
            people, 'ingest_season',
            lambda connection, season, **kwargs: real(
                connection, season, given=given))
        assert people.main(['--db', str(tmp_path / 'nba.db'),
                            '--seasons', '2026']) == 0
        printed = capsys.readouterr().out
        assert '1 players, 1 lines (1 played) across 1 seasons' in printed
        assert '0 exhibition rows' in printed
        assert '0 players hold two provider ids' in printed


class TestSeasonsArgument:
    @pytest.mark.parametrize('text, expected', [
        ('2026', [2026]),
        ('2024-2026', [2024, 2025, 2026]),
        ('2026,2024', [2024, 2026]),
    ])
    def test_the_seasons_argument_is_the_schedule_ingests(self, text,
                                                          expected):
        from src.nba.ingest import parse_seasons
        assert parse_seasons(text) == expected
