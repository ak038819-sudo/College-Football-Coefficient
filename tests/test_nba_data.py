"""The NBA archive layer: identity, the schedule loader and the ingest.

The tests that matter most here are the ones that drive the real traps this
source sets, because each of them produces data that looks right:

- an unplayed game is written 0-0, so a naive reader sees a draw;
- two different franchises are both spelled "Charlotte Hornets";
- an All-Star game carries the same season type as a league game.
"""

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from src.nba import games as games_module
from src.nba import identity
from src.nba import ingest as ingest_module
from src.nba import source as source_module
from src.nba import store as store_module


# --------------------------------------------------------------- identity

def test_the_thirty_franchises_are_the_ids_one_to_thirty():
    assert len(identity.FRANCHISE_IDS) == 30
    assert min(identity.FRANCHISE_IDS) == 1
    assert max(identity.FRANCHISE_IDS) == 30


def test_an_all_star_side_is_refused_as_a_team():
    # Team LeBron must never acquire a rating, a history or a standings row.
    for exhibition in (31, 32, 111353, -1):
        with pytest.raises(ValueError, match='exhibition|Unknown'):
            identity.team_key(exhibition)


def test_an_unseen_team_id_is_refused_rather_than_skipped():
    with pytest.raises(ValueError, match='Unknown NBA team id'):
        identity.team_key(99)
    assert identity.is_known(99) is False
    assert identity.is_known(31) is True


def test_a_team_key_carries_the_espn_id():
    assert identity.team_key(25) == 'nba:espn:team:25'
    assert identity.team_key('25') == 'nba:espn:team:25'


def test_a_season_is_labelled_by_both_calendar_years():
    assert identity.season_label(2027) == '2026-27'
    assert identity.season_label(2002) == '2001-02'
    # The century boundary is the case a naive `season - 1`/`season % 100`
    # pair gets wrong by dropping the leading zero.
    assert identity.season_label(2009) == '2008-09'
    assert identity.season_label(2000) == '1999-00'


def test_the_season_turns_over_in_september_not_january():
    june = datetime(2027, 6, 1, tzinfo=timezone.utc)
    october = datetime(2027, 10, 1, tzinfo=timezone.utc)
    january = datetime(2028, 1, 15, tzinfo=timezone.utc)
    assert identity.current_season(june) == 2027
    assert identity.current_season(october) == 2028
    # A January game belongs to the season that started the previous autumn.
    assert identity.current_season(january) == 2028


def test_the_play_in_is_not_the_regular_season_and_not_the_postseason():
    assert identity.season_type(5) == 'playin'
    assert 'playin' in identity.AFTER_REGULAR_SEASON
    assert 'regular' not in identity.AFTER_REGULAR_SEASON
    with pytest.raises(ValueError, match='Unknown NBA season type'):
        identity.season_type(1)


# ----------------------------------------------------------------- source

def test_every_dataset_url_names_a_release_asset():
    url = source_module.asset_url('schedule', 2027)
    assert url.endswith('/espn_nba_schedules/nba_schedule_2027.parquet')
    assert url.startswith('https://github.com/sportsdataverse/'
                          'sportsdataverse-data/releases/download/')
    # The stem is not the tag; the standings file is `standings_`, not
    # `espn_nba_standings_`, and guessing it returns 404.
    assert source_module.asset_url('standings', 2026).endswith(
        '/espn_nba_standings/standings_2026.parquet')


def test_an_unknown_dataset_is_refused():
    with pytest.raises(source_module.NbaSourceError, match='Unknown dataset'):
        source_module.asset_url('shots', 2026)


class _Response:
    def __init__(self, body, status=200):
        self._body, self.status = body, status

    def read(self, _size=None):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def test_a_body_that_is_not_parquet_is_refused_rather_than_cached():
    # A proxy or an error page answers 200 with HTML. Cached, it would fail
    # later as an unreadable column rather than as a failed download.
    with pytest.raises(source_module.NbaSourceError, match='not .*Parquet|'
                       'did not answer'):
        source_module.fetch_asset('https://example.invalid/x.parquet',
                                  opener=lambda *a, **k: _Response(
                                      b'<html>404</html>'),
                                  sleep=lambda _s: None)


def test_a_five_hundred_is_retried_and_a_four_oh_four_is_not():
    from urllib.error import HTTPError

    attempts = []

    def flaky(_request, timeout=None):
        attempts.append('try')
        if len(attempts) < 3:
            raise HTTPError('u', 503, 'busy', {}, None)
        return _Response(b'PAR1payloadPAR1')

    body = source_module.fetch_asset('https://example.invalid/x.parquet',
                                     opener=flaky, sleep=lambda _s: None)
    assert body.endswith(b'PAR1')
    assert len(attempts) == 3

    def gone(_request, timeout=None):
        raise HTTPError('u', 404, 'gone', {}, None)

    calls = []
    with pytest.raises(source_module.NbaSourceError, match='HTTP 404'):
        source_module.fetch_asset('https://example.invalid/x.parquet',
                                  opener=gone,
                                  sleep=lambda _s: calls.append(1))
    assert calls == [], 'a 404 must fail at once, not back off five times'


def test_the_seasons_to_fetch_turn_over_in_september():
    august = datetime(2026, 8, 31, tzinfo=timezone.utc)
    september = datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert source_module.available_seasons(august)[-1] == 2026
    assert source_module.available_seasons(september)[-1] == 2027
    assert source_module.available_seasons(september)[0] == 2002


def test_the_cache_path_is_resolved_inside_the_function():
    # A def-time default cannot be redirected, and a test that patches the
    # module attribute then passes while writing to the real directory.
    source_module.DEFAULT_RAW_ROOT = source_module.Path('/tmp/nba-elsewhere')
    try:
        assert str(source_module.cache_path('schedule', 2026)).startswith(
            '/tmp/nba-elsewhere')
    finally:
        source_module.DEFAULT_RAW_ROOT = source_module.Path('data/nba/raw')


# ------------------------------------------------------------------ games

def _row(**overrides):
    row = dict(
        id=401585612, season=2024, season_type=2,
        date='2024-01-15T00:30Z', game_date='2024-01-14',
        neutral_site=False, status_type_completed=True,
        home_id=25, home_score=112, home_abbreviation='OKC',
        home_location='Oklahoma City', home_name='Thunder',
        home_display_name='Oklahoma City Thunder',
        home_logo='h.png', home_logo_dark='hd.png',
        away_id=17, away_score=108, away_abbreviation='BKN',
        away_location='Brooklyn', away_name='Nets',
        away_display_name='Brooklyn Nets',
        away_logo='a.png', away_logo_dark='ad.png',
        venue_id=2317, venue_full_name='Paycom Center',
        venue_address_city='Oklahoma City', venue_address_state='OK',
        attendance=18203, notes_headline='', time_valid=True)
    row.update(overrides)
    return row


def test_an_unplayed_game_is_not_a_nil_nil_draw():
    """THE TRAP. The source writes 0-0 for every fixture not yet played."""
    game = games_module.parse_row(_row(status_type_completed=False,
                                       home_score=0, away_score=0))
    assert game['home_points'] is None, \
        'a scheduled fixture must carry no score, not a zero'
    assert game['away_points'] is None
    assert game['completed'] == 0
    assert game['rateable'] == 0
    assert game['exclusions'] == ['not_played']


def test_a_played_game_is_rateable_and_keeps_its_score():
    game = games_module.parse_row(_row())
    assert (game['home_points'], game['away_points']) == (112, 108)
    assert game['rateable'] == 1
    assert game['exclusions'] == []
    assert game['home_team_key'] == 'nba:espn:team:25'
    assert game['season_label'] == '2023-24'


def test_a_completed_draw_is_refused_because_the_nba_has_no_draws():
    with pytest.raises(games_module.ArchiveError, match='cannot be drawn'):
        games_module.parse_row(_row(home_score=100, away_score=100))


def test_zero_attendance_is_stored_as_an_absence():
    # It appears in 174 games of 2011, which had no empty arenas, so it is a
    # missing reading rather than a crowd of nobody.
    assert games_module.parse_row(_row(attendance=0))['attendance'] is None
    assert games_module.parse_row(_row(attendance=18203))['attendance'] == 18203


def test_an_all_star_game_is_skipped_rather_than_stored():
    """It carries season_type 2, the same as a league game in January."""
    all_star = _row(id=1, home_id=31, away_id=32, season_type=2)
    assert games_module.is_exhibition(all_star) is True
    parsed, skipped = games_module.parse([_row(), all_star])
    assert skipped == 1
    assert [game['provider_id'] for game in parsed] == ['401585612']


def test_a_row_with_one_real_team_and_one_unknown_id_is_a_fault_not_a_skip():
    half = _row(away_id=31)
    assert games_module.is_exhibition(half) is False
    with pytest.raises(games_module.ArchiveError, match='not a franchise'):
        games_module.parse_row(half)


def test_a_team_cannot_play_itself():
    with pytest.raises(games_module.ArchiveError, match='play itself'):
        games_module.parse_row(_row(away_id=25))


def test_a_newline_inside_a_name_is_collapsed():
    # The 2006 and 2007 files really do spell it this way.
    rows = games_module.teams_from_schedule(
        [_row(home_id=3, home_location='NO/Oklahoma City\n',
              home_name='Hornets',
              home_display_name='NO/Oklahoma City\n Hornets')], 2007)
    assert rows[0]['name'] == 'NO/Oklahoma City Hornets'
    assert '\n' not in rows[0]['location']


def test_a_missing_tipoff_time_leaves_no_timestamp():
    game = games_module.parse_row(_row(time_valid=False))
    assert game['tipoff_utc'] is None
    assert game['tipoff_tbd'] == 1


def test_games_sort_by_date_and_never_by_round():
    # The play-in is played between the regular season and the playoffs, so
    # any ordering by the name or number of a round puts it in the wrong
    # place. Ordering a season by round zeroed 1,353 football rating changes.
    regular = _row(id=1, game_date='2024-04-10', season_type=2)
    playin = _row(id=2, game_date='2024-04-16', season_type=5)
    playoff = _row(id=3, game_date='2024-04-20', season_type=3)
    parsed, _ = games_module.parse([playoff, regular, playin])
    assert [g['provider_id'] for g in games_module.in_order(parsed)] \
        == ['1', '2', '3']


def test_a_missing_column_refuses_the_whole_file():
    broken = _row()
    del broken['status_type_completed']
    with pytest.raises(games_module.ArchiveError, match='missing columns'):
        games_module.parse([broken])


def test_the_same_game_twice_is_refused():
    with pytest.raises(games_module.ArchiveError, match='appears twice'):
        games_module.parse([_row(), _row()])


def test_coverage_counts_what_was_played_separately_from_what_is_scheduled():
    parsed, _ = games_module.parse([
        _row(id=1), _row(id=2, status_type_completed=False, home_score=0,
                         away_score=0)])
    row = games_module.coverage(parsed)[0]
    assert (row['games'], row['played'], row['rateable']) == (2, 1, 1)
    assert row['label'] == '2023-24'


# ------------------------------------------------------------------ store

@pytest.fixture()
def connection(tmp_path):
    return store_module.connect(tmp_path / 'nba.db')


def test_the_schema_applies_and_is_idempotent(connection):
    assert store_module.migrate(connection) == store_module.SCHEMA_VERSION
    assert store_module.migrate(connection) == store_module.SCHEMA_VERSION
    version = connection.execute('SELECT version FROM schema_version') \
        .fetchone()['version']
    assert version == store_module.SCHEMA_VERSION


def test_a_newer_database_is_refused_rather_than_downgraded(connection):
    connection.execute('UPDATE schema_version SET version = ?',
                       (store_module.SCHEMA_VERSION + 1,))
    with pytest.raises(RuntimeError, match='schema version'):
        store_module.migrate(connection)


def _store_one(connection, **overrides):
    rows = [_row(**overrides)]
    parsed, _ = games_module.parse(rows)
    season = parsed[0]['season']
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(rows, season))
    return store_module.upsert_games(connection, parsed)


def test_storing_twice_changes_nothing(connection):
    _store_one(connection)
    _store_one(connection)
    assert connection.execute('SELECT count(*) AS n FROM game') \
        .fetchone()['n'] == 1


def test_a_later_score_replaces_an_earlier_absence(connection):
    _store_one(connection, status_type_completed=False, home_score=0,
               away_score=0)
    before = connection.execute(
        'SELECT home_points, rateable FROM game').fetchone()
    assert before['home_points'] is None and before['rateable'] == 0
    _store_one(connection)
    after = connection.execute(
        'SELECT home_points, rateable FROM game').fetchone()
    assert (after['home_points'], after['rateable']) == (112, 1)


def test_a_game_row_short_of_a_column_raises_rather_than_storing_blanks(
        connection):
    parsed, _ = games_module.parse([_row()])
    del parsed[0]['venue']
    with pytest.raises(KeyError, match='venue'):
        store_module.upsert_games(connection, parsed)


def test_a_franchise_is_named_for_its_most_recent_season(connection):
    """Team 25 is the Thunder today, though most of its rows say Seattle."""
    for season, location, nickname in ((2007, 'Seattle', 'SuperSonics'),
                                       (2009, 'Oklahoma City', 'Thunder')):
        rows = [_row(id=season, season=season, game_date=f'{season}-01-10',
                     home_location=location, home_name=nickname,
                     home_display_name=f'{location} {nickname}')]
        parsed, _ = games_module.parse(rows)
        store_module.upsert_team_seasons(
            connection, games_module.teams_from_schedule(rows, season))
        store_module.upsert_games(connection, parsed)
    store_module.refresh_team_identity(connection)
    team = connection.execute(
        'SELECT name, first_season, last_season FROM team WHERE team_id = 25'
    ).fetchone()
    assert team['name'] == 'Oklahoma City Thunder'
    assert (team['first_season'], team['last_season']) == (2007, 2009)
    # And the season rows still say what the source said at the time.
    seattle = connection.execute(
        'SELECT name FROM team_season WHERE team_key = ? AND season = 2007',
        ('nba:espn:team:25',)).fetchone()
    assert seattle['name'] == 'Seattle SuperSonics'


def test_the_hornets_and_the_bobcats_stay_two_franchises(connection):
    """Both are spelled CHA / Charlotte Hornets, eleven seasons apart."""
    pelicans_era = _row(id=1, season=2002, game_date='2002-01-10', home_id=3,
                        home_abbreviation='CHA', home_location='Charlotte',
                        home_name='Hornets',
                        home_display_name='Charlotte Hornets')
    bobcats_era = _row(id=2, season=2020, game_date='2020-01-10', home_id=30,
                       home_abbreviation='CHA', home_location='Charlotte',
                       home_name='Hornets',
                       home_display_name='Charlotte Hornets')
    for row in (pelicans_era, bobcats_era):
        parsed, _ = games_module.parse([row])
        store_module.upsert_team_seasons(
            connection, games_module.teams_from_schedule([row], row['season']))
        store_module.upsert_games(connection, parsed)
    ids = [r['team_id'] for r in connection.execute(
        'SELECT team_id FROM team ORDER BY team_id')]
    assert 3 in ids and 30 in ids, \
        'the two Charlotte Hornets must not collapse into one franchise'
    keys = {r['home_team_key'] for r in connection.execute(
        'SELECT home_team_key FROM game')}
    assert keys == {'nba:espn:team:3', 'nba:espn:team:30'}


def test_only_played_games_come_back_as_rateable(connection):
    _store_one(connection, id=1)
    _store_one(connection, id=2, status_type_completed=False, home_score=0,
               away_score=0)
    rateable = store_module.rateable_games(connection)
    assert [row['home_points'] for row in rateable] == [112]


def test_the_exclusions_column_says_why_a_game_was_left_out(connection):
    _store_one(connection, status_type_completed=False, home_score=0,
               away_score=0)
    stored = connection.execute('SELECT exclusions FROM game').fetchone()
    assert json.loads(stored['exclusions']) == ['not_played']


def test_a_game_cannot_point_at_a_franchise_that_is_not_stored(tmp_path):
    connection = store_module.connect(tmp_path / 'nba.db')
    parsed, _ = games_module.parse([_row()])
    with pytest.raises(sqlite3.IntegrityError):
        store_module.upsert_games(connection, parsed)


# ----------------------------------------------------------------- ingest

def test_the_ingest_stores_a_season_without_touching_the_network(connection):
    result = ingest_module.ingest_season(
        connection, 2024, schedule_rows=[_row(), _row(id=2, home_id=31,
                                                      away_id=32)],
        standings_rows=[dict(team_id=25, group_name='Western Conference'),
                        dict(team_id=17, group_name='Eastern Conference')])
    assert (result['games'], result['exhibitions']) == (1, 1)
    conference = connection.execute(
        'SELECT conference FROM team_season WHERE team_key = ?',
        ('nba:espn:team:25',)).fetchone()['conference']
    assert conference == 'Western Conference'


def test_a_missing_standings_file_leaves_the_conference_null_not_the_season(
        connection):
    def refuse(*_args, **_kwargs):
        raise source_module.NbaSourceError('no such asset')

    result = ingest_module.ingest_season(
        connection, 2027, schedule_rows=[_row(season=2027,
                                              game_date='2027-01-10')],
        standings_rows=None, opener=refuse)
    assert result['games'] == 1, 'the games must survive a missing standings'
    assert connection.execute(
        'SELECT conference FROM team_season').fetchone()['conference'] is None


def test_season_arguments_are_read_as_lists_and_ranges():
    assert ingest_module.parse_seasons('2024,2026') == [2024, 2026]
    assert ingest_module.parse_seasons('2024-2026') == [2024, 2025, 2026]
    assert ingest_module.parse_seasons(' 2025 ') == [2025]
    every = ingest_module.parse_seasons('', now=datetime(2026, 11, 3,
                                                         tzinfo=timezone.utc))
    assert every[0] == 2002 and every[-1] == 2027


def test_the_module_entry_point_runs(tmp_path, monkeypatch, capsys):
    """A module the refresh invokes needs a test that invokes it.

    Twice in this repository a new league module reached main with a broken
    `main()` because every test drove the functions underneath it.
    """
    rows = [_row()]

    monkeypatch.setattr(source_module, 'load_asset',
                        lambda *a, **k: (tmp_path / 'x.parquet', 'given'))
    monkeypatch.setattr(source_module, 'read_parquet', lambda _path: rows)
    exit_code = ingest_module.main(
        ['--db', str(tmp_path / 'nba.db'), '--seasons', '2024'])
    assert exit_code == 0
    printed = capsys.readouterr().out
    assert '1 games across 1 seasons' in printed
    assert '2023-24' in printed
