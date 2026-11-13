"""The Player Elo pilot: the grades imported, the replay, and the pages.

Two layers are under test. `src/nba/net_points.py` resolves ESPN's
per-player, per-game net points onto our own games and people and stores
them; `src/nba/player_elo.py` walks those grades into one rating a player.
Every fixture is hand-built so each quantity can be counted by eye, and the
arithmetic is pinned to LITERALS computed by hand rather than to the
modules' own constants: `assert elo == BASE + SCALE * impact` restates the
implementation and passes however wrong both are.

Each test here was checked against the mutation that breaks it.
"""

import json
import math
import sqlite3

import pytest

from src.nba import (export, identity, net_points, people_export, player_elo,
                     publish_gate, store)

HOME_ID, AWAY_ID = 1, 2
HOME, AWAY = identity.team_key(HOME_ID), identity.team_key(AWAY_ID)
# The two NBA franchise ids those ESPN ids belong to, from the map the
# crosswalk joins on.
HOME_NBA, AWAY_NBA = '1610612737', '1610612738'
GAME, SECOND, THIRD = '401500001', '401500002', '401500003'
SOURCE_GAME, SOURCE_SECOND = '0022500001', '0022500002'
PLAYER_ID, OTHER_ID = 1966, 3975
PLAYER = identity.entity_key('player', PLAYER_ID)
OTHER = identity.entity_key('player', OTHER_ID)
SOURCE_PLAYER, SOURCE_OTHER = '2544', '201939'


def grade_row(**kwargs):
    """A row of ESPN's player box archive, as the parquet carries one."""
    row = dict(game_id=SOURCE_GAME, player_id=SOURCE_PLAYER,
               team_id=HOME_NBA, name='A Player', home=True, starter=True,
               played=True, seconds_played=2100, o_net_pts=6.0,
               d_net_pts=-2.0, o_team_poss=60.0, d_team_poss=40.0)
    row.update(kwargs)
    return row


def source_game(**kwargs):
    row = dict(game_id=SOURCE_GAME, game_type='regular_season',
               game_date='2026-01-20', home_team_id=HOME_NBA,
               away_team_id=AWAY_NBA)
    row.update(kwargs)
    return row


def stored_grade(**kwargs):
    """A row shaped like one `net_points.season_rows` returns."""
    row = dict(person_key=PLAYER, game_key=identity.game_key(GAME),
               season=2026, team_key=HOME, source_player_id=SOURCE_PLAYER,
               source_game_id=SOURCE_GAME, source_team_id=HOME_NBA,
               home=1, started=1, played=1, seconds_played=2100,
               offense_net_points=6.0, defense_net_points=-2.0,
               offense_team_possessions=60.0,
               defense_team_possessions=40.0, resolved_by='name',
               game_date='2026-01-20')
    row.update(kwargs)
    return row


def database(tmp_path, *, games=(GAME,), people=((PLAYER, 'A Player'),),
             minutes=None, dates=None, types=None):
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
            ' home_team_id, away_team_id, home_points, away_points,'
            ' completed, rateable)'
            " VALUES (?, ?, 2026, '2025-26', ?, ?, ?, ?, ?, ?, 112, 104,"
            ' 1, 1)',
            (identity.game_key(provider), provider,
             (types or {}).get(provider, 'regular'),
             (dates or {}).get(provider, '2026-01-20'), HOME, AWAY,
             HOME_ID, AWAY_ID))
    for key, name in people:
        connection.execute(
            'INSERT INTO person (person_key, athlete_id, full_name,'
            ' display_name) VALUES (?, ?, ?, ?)',
            (key, key.rsplit(':', 1)[-1], name, name))
    for (key, provider), played in (minutes or {}).items():
        connection.execute(
            'INSERT INTO person_game (person_key, game_key, season,'
            ' season_type, game_date, team_key, opponent_key, home_away,'
            ' played, minutes) VALUES (?, ?, 2026, ?, ?, ?, ?, ?, 1, ?)',
            (key, identity.game_key(provider),
             (types or {}).get(provider, 'regular'),
             (dates or {}).get(provider, '2026-01-20'), HOME, AWAY, 'home',
             played))
    connection.commit()
    return connection


# --- the crosswalk -------------------------------------------------------

class TestResolvingTheArchiveOntoOurs:
    def test_every_franchise_has_an_espn_id(self):
        # Thirty, and not one of them shared: a map with a duplicate would
        # file one club's grades under another's.
        assert len(net_points.ESPN_TEAM_OF_NBA) == 30
        assert len(set(net_points.ESPN_TEAM_OF_NBA.values())) == 30

    def test_the_asset_url_names_the_season_and_the_dataset(self):
        url = net_points.asset_url('grades', 2026)
        assert url == (net_points.ARCHIVE_ROOT
                       + '/nba/espn/player_box/season=2026/data.parquet')

    def test_a_name_is_folded_before_it_is_matched(self):
        # The two the fingerprint had to settle were accents and a suffix.
        assert net_points.normalised('Luka Dončić') == 'lukadoncic'
        assert net_points.normalised('Jaren Jackson Jr.') \
            == net_points.normalised('Jaren Jackson')

    def test_a_game_resolves_on_the_date_and_the_two_sides(self, tmp_path):
        connection = database(tmp_path)
        by_side, by_pair = net_points.archive_games(connection, 2026)
        resolved, unresolved = net_points.crosswalk_games(
            [source_game()], [grade_row()], by_side, by_pair)
        assert unresolved == []
        assert resolved[SOURCE_GAME]['game_key'] == identity.game_key(GAME)

    def test_the_postseason_is_not_crosswalked_at_all(self, tmp_path):
        # The archive carries playoff games; the pilot rates the regular
        # season, and a playoff grade stored under a regular-season replay
        # would be evidence nobody asked for.
        connection = database(tmp_path)
        by_side, by_pair = net_points.archive_games(connection, 2026)
        resolved, unresolved = net_points.crosswalk_games(
            [source_game(game_type='playoffs')], [grade_row()],
            by_side, by_pair)
        assert resolved == {} and unresolved == []

    def test_a_neutral_site_is_placed_by_the_grades_own_home_flag(
            self, tmp_path):
        # The NBA lists neither club as home for a game abroad, so the
        # date and the pair of teams are all there is -- plus ESPN's own
        # flag to say which side is which.
        connection = database(tmp_path)
        by_side, by_pair = net_points.archive_games(connection, 2026)
        grades = [grade_row(home=True, team_id=HOME_NBA),
                  grade_row(player_id=SOURCE_OTHER, home=False,
                            team_id=AWAY_NBA)]
        resolved, unresolved = net_points.crosswalk_games(
            [source_game(home_team_id=None, away_team_id=None)], grades,
            by_side, by_pair)
        assert unresolved == []
        assert resolved[SOURCE_GAME]['game_key'] == identity.game_key(GAME)

    def test_a_game_we_do_not_have_is_named_rather_than_dropped(
            self, tmp_path):
        connection = database(tmp_path)
        by_side, by_pair = net_points.archive_games(connection, 2026)
        resolved, unresolved = net_points.crosswalk_games(
            [source_game(game_date='2026-02-01')], [grade_row()],
            by_side, by_pair)
        assert resolved == {}
        assert [one[0] for one in unresolved] == [SOURCE_GAME]

    def test_a_player_resolves_on_a_name_unique_in_the_season(
            self, tmp_path):
        connection = database(tmp_path)
        played = {SOURCE_PLAYER: {identity.game_key(GAME)}}
        resolved, unresolved = net_points.crosswalk_people(
            [{'player_id': SOURCE_PLAYER, 'name': 'A Player'}], played,
            net_points.our_names(connection),
            {PLAYER: {identity.game_key(GAME)}})
        assert unresolved == []
        assert resolved[SOURCE_PLAYER] == (PLAYER, 'name')

    def test_a_name_we_do_not_know_falls_to_the_games_he_played(
            self, tmp_path):
        # "Cui Cui" and "Yongxi Cui" are one man. The appearance set is
        # what says so.
        connection = database(tmp_path, games=(GAME, SECOND))
        ours = {PLAYER: {identity.game_key(GAME),
                         identity.game_key(SECOND)}}
        played = {SOURCE_PLAYER: {identity.game_key(GAME),
                                  identity.game_key(SECOND)}}
        resolved, unresolved = net_points.crosswalk_people(
            [{'player_id': SOURCE_PLAYER, 'name': 'Someone Else'}], played,
            net_points.our_names(connection), ours)
        assert unresolved == []
        assert resolved[SOURCE_PLAYER] == (PLAYER, 'the games played')

    def test_a_player_the_fingerprint_cannot_settle_is_quarantined(
            self, tmp_path):
        connection = database(tmp_path, games=(GAME, SECOND, THIRD))
        ours = {PLAYER: {identity.game_key(GAME),
                         identity.game_key(SECOND),
                         identity.game_key(THIRD)}}
        played = {SOURCE_PLAYER: {identity.game_key(GAME)}}
        resolved, unresolved = net_points.crosswalk_people(
            [{'player_id': SOURCE_PLAYER, 'name': 'Someone Else'}], played,
            net_points.our_names(connection), ours)
        assert resolved == {}
        assert unresolved[0][0] == SOURCE_PLAYER
        assert 'overlaps 0.33' in unresolved[0][2]


class TestWhatCountsAsAGrade:
    def test_a_zero_net_points_is_a_grade(self):
        # It is the average performance, not a missing one. Reading it as
        # missing would drop every ordinary night in the archive.
        assert net_points.gradeable(
            stored_grade(offense_net_points=0.0, defense_net_points=0.0))

    def test_a_did_not_play_is_not_a_grade(self):
        assert not net_points.gradeable(stored_grade(played=0))

    def test_no_possessions_on_one_side_is_not_a_grade(self):
        assert not net_points.gradeable(
            stored_grade(offense_team_possessions=0.0))
        assert not net_points.gradeable(
            stored_grade(defense_team_possessions=None))

    def test_a_row_whose_game_or_player_is_unresolved_is_counted_out(self):
        kept, dropped = net_points.rows_for(
            2026, [grade_row(), grade_row(game_id='0022599999'),
                   grade_row(player_id='999999')],
            {SOURCE_GAME: {'game_key': identity.game_key(GAME)}},
            {SOURCE_PLAYER: (PLAYER, 'name')})
        assert len(kept) == 1
        assert sum(dropped.values()) == 2
        assert kept[0]['source_player_id'] == SOURCE_PLAYER
        assert kept[0]['team_key'] == HOME


class TestTheOrderTheReplayWalks:
    def test_rows_come_back_oldest_game_first(self, tmp_path):
        connection = database(
            tmp_path, games=(GAME, SECOND),
            dates={GAME: '2026-01-20', SECOND: '2026-01-02'})
        rows = [stored_grade(),
                stored_grade(game_key=identity.game_key(SECOND),
                             source_game_id=SOURCE_SECOND)]
        net_points.replace_season(connection, 2026, rows, [])
        walked = net_points.season_rows(connection, 2026)
        assert [row['game_key'] for row in walked] == [
            identity.game_key(SECOND), identity.game_key(GAME)]

    def test_an_ungradeable_row_is_stored_and_not_walked(self, tmp_path):
        connection = database(tmp_path)
        net_points.replace_season(
            connection, 2026, [stored_grade(played=0)], [])
        assert net_points.has_net_points(connection, 2026)
        assert net_points.season_rows(connection, 2026) == []
        assert net_points.season_rows(
            connection, 2026, gradeable_only=False) != []


# --- the replay ----------------------------------------------------------

class TestTheArithmetic:
    def test_a_rate_is_per_hundred_possessions(self):
        assert player_elo.rate(6.0, 60.0) == 10.0
        assert player_elo.rate(-3.0, 50.0) == -6.0

    def test_nothing_to_divide_by_is_none_and_not_zero(self):
        # Zero would read as an average night. The player told us nothing.
        assert player_elo.rate(6.0, 0.0) is None
        assert player_elo.rate(None, 60.0) is None

    def test_the_two_sides_keep_their_own_denominators(self):
        # 6 over 60 and -2 over 40 is +10 and -5, not 4 over 100.
        offense, defense, exposure = player_elo.graded(stored_grade())
        assert (offense, defense) == (10.0, -5.0)
        assert exposure == 50.0

    def test_a_missing_grade_gives_no_update_at_all(self):
        assert player_elo.graded(
            stored_grade(offense_net_points=None)) is None

    def test_half_the_history_survives_a_half_life_of_exposure(self):
        assert player_elo.decay(2000.0, 2000.0) == pytest.approx(0.5)
        assert player_elo.decay(0.0, 2000.0) == 1.0

    def test_the_first_grade_is_the_rating(self):
        # All three accumulators start at zero, so the weight and the
        # totals carry the same (1 - decay) factor and it divides out.
        impact = player_elo.Impact()
        assert impact.update(10.0, -5.0, 50.0, 2000.0) \
            == pytest.approx(5.0)
        assert impact.offense == pytest.approx(10.0)

    def test_a_second_grade_is_weighted_by_its_exposure(self):
        # Hand-computed: kept = 2 ** (-50/2000) = 0.9827589...
        impact = player_elo.Impact()
        impact.update(10.0, -5.0, 50.0, 2000.0)
        after = impact.update(0.0, 0.0, 50.0, 2000.0)
        kept = 2.0 ** (-50.0 / 2000.0)
        fresh = 1.0 - kept
        weight = kept * fresh + fresh
        offense = kept * fresh * 10.0
        defense = kept * fresh * -5.0
        assert after == pytest.approx((offense + defense) / weight)
        # And it moved towards the newer, poorer night.
        assert 0.0 < after < 5.0

    def test_the_elo_is_the_impact_on_the_clubs_scale(self):
        assert player_elo.elo(0.0, 25.0) == 1500.0
        assert player_elo.elo(4.0, 25.0) == 1600.0
        assert player_elo.elo(-4.0, 25.0) == 1400.0

    def test_a_man_with_no_graded_game_is_unavailable(self):
        assert player_elo.status(900.0, 0) == 'unavailable'
        assert player_elo.status(40.0, 3, threshold=500.0) == 'provisional'
        assert player_elo.status(500.0, 3, threshold=500.0) == 'rated'


class TestTheWalk:
    def test_a_first_grade_has_no_rating_before_it(self):
        # NOT 1500. A man who arrives at exactly average and then moves is
        # a thing that never happened.
        records, _ = player_elo.replay([stored_grade()])
        assert records[0]['pregame_impact'] is None
        assert records[0]['change'] is None
        assert records[0]['postgame_impact'] == pytest.approx(5.0)

    def test_the_accumulators_carry_from_one_season_into_the_next(self):
        first, impacts = player_elo.replay([stored_grade()])
        second, _ = player_elo.replay(
            [stored_grade(season=2027, game_key=identity.game_key(SECOND))],
            impacts=impacts)
        assert second[0]['pregame_impact'] == pytest.approx(
            first[0]['postgame_impact'])

    def test_an_ungradeable_row_in_the_walk_is_skipped(self):
        records, _ = player_elo.replay(
            [stored_grade(offense_team_possessions=0.0), stored_grade()])
        assert len(records) == 1

    def test_a_snapshot_reports_the_running_halves_not_the_last_nights(
            self):
        # THE BUG THIS PINS: reporting `offense_grade` beside the career
        # impact drew a man with an impact of +6.9 and an offence of -8.1,
        # which was his last night and not his season. The two halves must
        # add to the impact.
        rows = [stored_grade(),
                stored_grade(game_key=identity.game_key(SECOND),
                             offense_net_points=-12.0,
                             defense_net_points=0.0)]
        records, _ = player_elo.replay(rows)
        snaps = player_elo.snapshots(
            records, {PLAYER: 1200.0}, as_of='now', threshold=500.0,
            scale=25.0)
        row = snaps[0]
        assert row['offense_per_100'] + row['defense_per_100'] \
            == pytest.approx(row['impact_per_100'])
        assert row['offense_per_100'] != pytest.approx(
            records[-1]['offense_grade'])

    def test_a_snapshot_counts_only_the_seasons_own_games(self):
        records, _ = player_elo.replay(
            [stored_grade(),
             stored_grade(game_key=identity.game_key(SECOND))])
        snaps = player_elo.snapshots(records, {PLAYER: 480.0}, as_of='now',
                                     threshold=500.0, scale=25.0)
        assert len(snaps) == 1
        assert snaps[0]['graded_games'] == 2
        assert snaps[0]['graded_possessions'] == pytest.approx(100.0)
        assert snaps[0]['season_minutes'] == 480.0
        assert snaps[0]['status'] == 'provisional'

    def test_the_rating_is_the_one_after_his_last_game_of_the_season(self):
        records, _ = player_elo.replay(
            [stored_grade(),
             stored_grade(game_key=identity.game_key(SECOND),
                          offense_net_points=0.0, defense_net_points=0.0)])
        snaps = player_elo.snapshots(records, {PLAYER: 900.0}, as_of='now',
                                     threshold=500.0, scale=25.0)
        assert snaps[0]['impact_per_100'] == pytest.approx(
            records[-1]['postgame_impact'])
        assert snaps[0]['elo'] == pytest.approx(
            1500.0 + 25.0 * records[-1]['postgame_impact'])


class TestTheWholeBuild:
    def _stored(self, connection, count, *, person=PLAYER, offense=6.0):
        games, rows = [], []
        for index in range(count):
            provider = '4015%05d' % index
            games.append(provider)
            rows.append(dict(stored_grade(
                person_key=person, game_key=identity.game_key(provider),
                source_game_id='00225%05d' % index,
                offense_net_points=offense)))
        return games, rows

    def test_a_build_stores_one_row_a_player_a_season(self, tmp_path):
        games, rows = self._stored(None, 3)
        connection = database(
            tmp_path, games=games,
            minutes={(PLAYER, game): 300.0 for game in games})
        net_points.replace_season(connection, 2026, rows, [])
        result = player_elo.build(connection)
        assert result['appearances'] == 3
        assert result['snapshots'] == 1
        assert result['ranked'] == 1
        board = player_elo.leaderboard(connection, 2026)
        assert [row['person_key'] for row in board] == [PLAYER]
        assert board[0]['graded_games'] == 3

    def test_a_man_under_the_cutoff_is_not_on_the_board(self, tmp_path):
        games, rows = self._stored(None, 2)
        connection = database(
            tmp_path, games=games,
            minutes={(PLAYER, game): 20.0 for game in games})
        net_points.replace_season(connection, 2026, rows, [])
        player_elo.build(connection)
        assert player_elo.leaderboard(connection, 2026) == []
        # He is rated all the same, and the export asks for him by name.
        widened = player_elo.leaderboard(
            connection, 2026, statuses=('rated', 'provisional'))
        assert [row['status'] for row in widened] == ['provisional']

    def test_the_settings_are_stored_with_the_model(self, tmp_path):
        games, rows = self._stored(None, 1)
        connection = database(tmp_path, games=games,
                              minutes={(PLAYER, games[0]): 600.0})
        net_points.replace_season(connection, 2026, rows, [])
        player_elo.build(connection)
        config = json.loads(connection.execute(
            'SELECT config FROM rating_model WHERE name = ?',
            (player_elo.MODEL_NAME,)).fetchone()[0])
        assert config['half_life_possessions'] == 2000.0
        assert config['display_scale'] == 25.0
        assert config['eligible_minutes'] == 500.0

    def test_a_rebuild_replaces_rather_than_doubles(self, tmp_path):
        games, rows = self._stored(None, 2)
        connection = database(
            tmp_path, games=games,
            minutes={(PLAYER, game): 300.0 for game in games})
        net_points.replace_season(connection, 2026, rows, [])
        player_elo.build(connection)
        player_elo.build(connection)
        assert connection.execute(
            'SELECT COUNT(*) FROM player_rating_game').fetchone()[0] == 2
        assert connection.execute(
            'SELECT COUNT(*) FROM player_rating').fetchone()[0] == 1


class TestTheDiagnostic:
    def test_a_rating_that_is_right_has_no_error(self):
        # Every night identical, so the rating after five games is exactly
        # what the next 500 possessions produce.
        rows = [stored_grade(game_key=identity.game_key('4015%05d' % i))
                for i in range(20)]
        records, _ = player_elo.replay(rows)
        error, counted = player_elo.next_window_error(
            rows, records, after=5, window=500.0)
        assert counted == 1
        assert error == pytest.approx(0.0, abs=1e-9)

    def test_a_player_without_a_full_window_is_not_counted(self):
        rows = [stored_grade(game_key=identity.game_key('4015%05d' % i))
                for i in range(6)]
        records, _ = player_elo.replay(rows)
        error, counted = player_elo.next_window_error(
            rows, records, after=5, window=500.0)
        assert (error, counted) == (None, 0)


# --- what the pages are given --------------------------------------------

def built(tmp_path, *, minutes=300.0, games=3):
    games_list, rows = [], []
    for index in range(games):
        provider = '4015%05d' % index
        games_list.append(provider)
        rows.append(dict(stored_grade(
            game_key=identity.game_key(provider),
            source_game_id='00225%05d' % index)))
    connection = database(
        tmp_path, games=games_list,
        minutes={(PLAYER, game): minutes / games for game in games_list})
    net_points.replace_season(connection, 2026, rows, [])
    player_elo.build(connection)
    return connection


class TestTheSeasonPayload:
    def test_the_board_carries_the_rated_and_the_provisional_apart(
            self, tmp_path):
        connection = built(tmp_path, minutes=900.0)
        board = export.player_ratings(connection, 2026)
        assert board['ranked'] == 1 and board['provisional'] == 0
        assert board['cutoff_minutes'] == 500.0
        assert str(board['players'][0]['player_id']) == str(PLAYER_ID)
        assert board['players'][0]['status'] == 'rated'

    def test_a_season_nobody_has_the_minutes_for_is_still_published(
            self, tmp_path):
        # Every October. A payload holding only the rated would hide the
        # whole layer until December.
        connection = built(tmp_path, minutes=60.0)
        board = export.player_ratings(connection, 2026)
        assert board['ranked'] == 0 and board['provisional'] == 1
        assert len(board['players']) == 1

    def test_a_database_with_no_replay_has_no_board(self, tmp_path):
        connection = database(tmp_path)
        assert export.player_ratings(connection, 2026) is None

    def test_the_coverage_row_counts_what_it_ranks(self, tmp_path):
        connection = built(tmp_path, minutes=900.0)
        board = export.player_ratings(connection, 2026)
        row = export.coverage_row(connection, 2026, [], [],
                                  player_elo_board=board)
        assert row['player_elo'] == {'ranked': 1, 'provisional': 0,
                                     'graded': 1, 'cutoff_minutes': 500.0}

    def test_the_club_export_never_picks_the_player_model(self, tmp_path):
        # `rating_model` holds both since schema 5, and the club export's
        # default is the newest stored model. Picking the player rating
        # refused the whole export, because no season has a club rating
        # under that name.
        connection = built(tmp_path)
        connection.execute(
            'INSERT INTO rating_model (name, config, created_at) '
            "VALUES ('nba_elo_v1', '{}', '2026-01-01')")
        connection.commit()
        assert export.model_row(connection)['name'] == 'nba_elo_v1'


class TestThePlayerPayload:
    def test_a_mans_seasons_and_his_graded_games_are_attached(
            self, tmp_path):
        connection = built(tmp_path, minutes=900.0)
        snapshots, replay = people_export.ratings(connection,
                                                  player_elo.MODEL_NAME)
        season = snapshots[PLAYER][0]
        assert season[0] == 2026 and season[8] == 'rated'
        assert len(replay[PLAYER][2026]) == 3

    def test_the_first_graded_game_has_no_rating_before_it(self, tmp_path):
        connection = built(tmp_path, minutes=900.0)
        _, replay = people_export.ratings(connection, player_elo.MODEL_NAME)
        first = replay[PLAYER][2026][0]
        assert first[4] is None
        assert first[6] is None

    def test_a_database_from_before_schema_five_attaches_nothing(
            self, tmp_path):
        connection = database(tmp_path)
        connection.execute('DROP TABLE player_rating')
        connection.execute('DROP TABLE player_rating_game')
        connection.commit()
        snapshots, replay = people_export.ratings(connection,
                                                  player_elo.MODEL_NAME)
        assert (dict(snapshots), dict(replay)) == ({}, {})


class TestTheGate:
    def _manifest(self, rating):
        return {'coverage': [{'season': 2026, 'player_elo': rating}]}

    def test_a_consistent_board_passes(self):
        assert publish_gate.check_player_ratings(self._manifest(
            {'ranked': 300, 'provisional': 200, 'graded': 560,
             'cutoff_minutes': 500.0})) == []

    def test_an_export_with_no_ratings_at_all_passes(self):
        # October, and every export written before this layer existed.
        assert publish_gate.check_player_ratings(
            self._manifest(None)) == []
        assert publish_gate.check_player_ratings(self._manifest(
            {'ranked': 0, 'provisional': 0, 'graded': 0,
             'cutoff_minutes': 500.0})) == []

    def test_more_players_carried_than_graded_is_refused(self):
        reasons = publish_gate.check_player_ratings(self._manifest(
            {'ranked': 300, 'provisional': 200, 'graded': 400,
             'cutoff_minutes': 500.0}))
        assert len(reasons) == 1 and '500' in reasons[0]

    def test_a_cutoff_of_nothing_is_refused(self):
        reasons = publish_gate.check_player_ratings(self._manifest(
            {'ranked': 1, 'provisional': 0, 'graded': 1,
             'cutoff_minutes': 0}))
        assert len(reasons) == 1 and 'cutoff' in reasons[0]

    def test_a_negative_count_is_refused(self):
        reasons = publish_gate.check_player_ratings(self._manifest(
            {'ranked': -1, 'provisional': 0, 'graded': 1,
             'cutoff_minutes': 500.0}))
        assert reasons

    def test_schema_five_is_supported_and_is_the_current_one(self):
        assert store.SCHEMA_VERSION == 5
        assert publish_gate.SCHEMA_VERSION == 5
        assert 5 in publish_gate.SUPPORTED_SCHEMA_VERSIONS


class TestWhatThePublisherWillNotDo:
    def test_a_season_that_loses_its_ratings_is_not_published(self,
                                                              tmp_path):
        from src.nba import publish
        live = {'schema_version': 5, 'generated_at': '2026-11-01T00:00:00Z',
                'coverage': [{'season': 2026,
                              'player_elo': {'ranked': 300,
                                             'provisional': 100,
                                             'graded': 500,
                                             'cutoff_minutes': 500.0}}]}
        fresh = {'schema_version': 5, 'generated_at': '2026-11-02T00:00:00Z',
                 'coverage': [{'season': 2026, 'player_elo': None}]}
        assert player_elo.MODEL_NAME  # the layer exists to be lost
        assert publish._rated_seasons(live) == {2026}
        assert publish._rated_seasons(fresh) == set()


def test_the_module_runs_as_a_module(tmp_path, capsys):
    """A `main()` nobody invokes is a `main()` nobody has run.

    Twice in a week an src/nba module reached main with a broken entry
    point because every test drove the functions under it.
    """
    games, rows = [], []
    for index in range(2):
        provider = '4015%05d' % index
        games.append(provider)
        rows.append(dict(stored_grade(
            game_key=identity.game_key(provider),
            source_game_id='00225%05d' % index)))
    path = tmp_path / 'nba.db'
    connection = database(tmp_path, games=games,
                          minutes={(PLAYER, game): 300.0 for game in games})
    net_points.replace_season(connection, 2026, rows, [])
    connection.close()
    assert player_elo.main(['--db', str(path)]) == 0
    printed = capsys.readouterr().out
    assert '2 appearances replayed' in printed
    assert math.isfinite(sqlite3.connect(path).execute(
        'SELECT elo FROM player_rating').fetchone()[0])
