"""Basketball database loaders. Temporary database only, never db/league.db."""

from pathlib import Path
import json
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb import store as store_module
from src.ncaambb.identity import entity_key, season_label
from src.ncaambb.store import (SCHEMA_VERSION, connect, load_season, migrate,
                               placeholder_teams, record_snapshot,
                               upsert_box_scores, upsert_games,
                               upsert_player_team_seasons, upsert_players,
                               upsert_team_seasons, upsert_teams)


@pytest.fixture
def db(tmp_path):
    connection = connect(tmp_path / 'ncaambb.db')
    yield connection
    connection.close()


def team(identifier=10, school='Alpha', conference='Big Test'):
    return dict(id=identifier, school=school, mascot='Tigers',
                abbreviation='ALP', displayName=f'{school} Tigers',
                shortDisplayName=school, conferenceId=5, conference=conference,
                currentVenue='Alpha Arena', currentCity='Provo',
                currentState='UT')


def game(identifier=1, season=2026, home=10, away=20, status='final',
         home_points=70, away_points=60, day='2025-11-07T19:00:00Z'):
    return dict(id=identifier, season=season, seasonType='regular',
                status=status, startDate=day, startTimeTbd=False,
                neutralSite=False, conferenceGame=True,
                homeTeamId=home, awayTeamId=away, homeTeam='Alpha',
                awayTeam='Beta', homePoints=home_points,
                awayPoints=away_points, homePeriodPoints=[35, 35],
                awayPeriodPoints=[30, 30], venueId=7, venue='Alpha Arena',
                city='Provo', state='UT', attendance=15000,
                homeTeamEloStart=1500, homeTeamEloEnd=1510)


# --- identity --------------------------------------------------------------

def test_a_key_carries_sport_provider_and_kind():
    assert entity_key('team', 18) == 'ncaambb:cbbd:team:18'
    assert entity_key('player', 18) != entity_key('team', 18)
    assert season_label(2026) == '2025-26'
    for bad in (0, -1, '18', True, 1.0):
        with pytest.raises(ValueError):
            entity_key('team', bad)
    with pytest.raises(ValueError):
        entity_key('franchise', 18)
    with pytest.raises(ValueError):
        season_label(True)


# --- schema ----------------------------------------------------------------

def test_the_schema_stores_no_provider_elo(db):
    for table in ('game', 'team', 'team_season'):
        columns = [row[1].lower() for row in db.execute(f'PRAGMA table_info({table})')]
        assert not any('elo' in name for name in columns), table


def test_migrating_twice_changes_nothing(db):
    assert migrate(db) == SCHEMA_VERSION
    assert migrate(db) == SCHEMA_VERSION
    rows = db.execute('SELECT COUNT(*) AS n FROM schema_version').fetchone()
    assert rows['n'] == 1


def test_a_database_from_a_future_version_is_refused(db):
    db.execute('UPDATE schema_version SET version = ?', (SCHEMA_VERSION + 1,))
    db.commit()
    with pytest.raises(RuntimeError, match='schema version'):
        migrate(db)


def test_the_database_path_is_resolved_inside_the_call(tmp_path, monkeypatch):
    # A def-time default would bind db/ncaambb.db and ignore this.
    target = tmp_path / 'nested' / 'moved.db'
    monkeypatch.setattr(store_module, 'DEFAULT_PATH', target)
    connection = connect()
    try:
        assert target.exists()
    finally:
        connection.close()


# --- idempotency -----------------------------------------------------------

def test_loading_the_same_season_twice_leaves_the_same_rows(db):
    teams, games = [team(10), team(20, 'Beta')], [game()]
    first = load_season(db, 2026, games, teams)
    counts = lambda: {name: db.execute(f'SELECT COUNT(*) AS n FROM {name}').fetchone()['n']
                      for name in ('team', 'team_season', 'game')}
    before = counts()
    second = load_season(db, 2026, games, teams)
    assert before == counts()
    assert first == second
    assert before == dict(team=2, team_season=2, game=1)


def test_a_rerun_updates_a_game_that_has_since_finished(db):
    teams = [team(10), team(20, 'Beta')]
    scheduled = game(status='scheduled', home_points=None, away_points=None)
    load_season(db, 2026, [scheduled], teams)
    row = db.execute('SELECT status, home_points, rateable FROM game').fetchone()
    assert row['status'] == 'scheduled'
    assert row['home_points'] is None
    assert row['rateable'] == 0

    load_season(db, 2026, [game()], teams)
    row = db.execute('SELECT status, home_points, rateable FROM game').fetchone()
    assert row['status'] == 'final'
    assert row['home_points'] == 70
    assert row['rateable'] == 1
    assert db.execute('SELECT COUNT(*) AS n FROM game').fetchone()['n'] == 1


def test_a_missing_score_stays_null_and_never_becomes_zero(db):
    load_season(db, 2026, [game(status='postponed', home_points=None,
                                away_points=None)],
                [team(10), team(20, 'Beta')])
    row = db.execute('SELECT home_points, away_points FROM game').fetchone()
    assert row['home_points'] is None and row['away_points'] is None


# --- non-Division I opponents ---------------------------------------------

def test_a_non_division_one_opponent_is_kept_as_a_placeholder(db):
    summary = load_season(db, 2026, [game(away=999)], [team(10)])
    assert summary == dict(season=2026, games=1, division_one_teams=1,
                           other_teams=1)
    row = db.execute(
        'SELECT division_one, conference FROM team_season WHERE team_key = ?',
        (entity_key('team', 999),)).fetchone()
    assert row['division_one'] == 0
    assert row['conference'] is None
    stored = db.execute('SELECT rateable, division_one, exclusions FROM game').fetchone()
    assert stored['rateable'] == 0
    assert stored['division_one'] == 0
    assert json.loads(stored['exclusions']) == ['non-Division I opponent']


def test_a_placeholder_uses_the_providers_own_name_and_invents_nothing():
    rows = placeholder_teams([game(away=999)], {10})
    assert rows == [dict(id=999, school='Beta')]
    unnamed = game(away=999)
    unnamed['awayTeam'] = None
    assert placeholder_teams([unnamed], {10}) == [dict(id=999, school='Team 999')]
    assert placeholder_teams([game()], {10, 20}) == []


def test_a_game_referencing_an_unknown_team_is_refused_not_orphaned(db):
    upsert_teams(db, [team(10)])
    upsert_team_seasons(db, 2026, [team(10)])
    with pytest.raises(sqlite3.IntegrityError):
        upsert_games(db, [game(away=999)], {10})


# --- seasons and dates -----------------------------------------------------

def test_a_november_game_is_stored_against_the_ending_year_season(db):
    load_season(db, 2026, [game(day='2025-11-07T19:00:00Z')],
                [team(10), team(20, 'Beta')])
    row = db.execute('SELECT season, season_label, start_date_utc FROM game').fetchone()
    assert row['season'] == 2026
    assert row['season_label'] == '2025-26'
    assert row['start_date_utc'] == '2025-11-07'


def test_a_march_game_belongs_to_the_same_season_as_november(db):
    teams = [team(10), team(20, 'Beta')]
    load_season(db, 2026, [game(1, day='2025-11-07T19:00:00Z'),
                           game(2, day='2026-03-15T19:00:00Z')], teams)
    labels = {row['season_label'] for row in
              db.execute('SELECT season_label FROM game')}
    assert labels == {'2025-26'}


def test_an_undecided_tip_off_is_recorded_as_undecided(db):
    pending = game(status='scheduled', home_points=None, away_points=None)
    pending['startTimeTbd'] = True
    load_season(db, 2027, [pending], [team(10), team(20, 'Beta')])
    row = db.execute('SELECT start_time_tbd, start_date FROM game').fetchone()
    assert row['start_time_tbd'] == 1
    # The provider's own timestamp is kept verbatim rather than zeroed.
    assert row['start_date'] == pending['startDate']


def test_a_game_without_a_usable_date_is_refused(db):
    upsert_teams(db, [team(10), team(20, 'Beta')])
    upsert_team_seasons(db, 2026, [team(10), team(20, 'Beta')])
    with pytest.raises(ValueError, match='no usable start date'):
        upsert_games(db, [game(day='2025-11-07T19:00:00')], {10, 20})


# --- snapshots -------------------------------------------------------------

def test_a_snapshot_traces_rows_to_the_run_that_made_them(db):
    snapshot_id = record_snapshot(db, 'schedule', '2026-10-20T04:11:44+00:00',
                                  season=2026, source_revision='abc1234',
                                  requests=13, rows_seen=6318)
    load_season(db, 2026, [game()], [team(10), team(20, 'Beta')],
                snapshot_id=snapshot_id)
    row = db.execute('SELECT snapshot_id FROM game').fetchone()
    assert row['snapshot_id'] == snapshot_id
    meta = db.execute('SELECT * FROM snapshot WHERE snapshot_id = ?',
                      (snapshot_id,)).fetchone()
    assert meta['requests'] == 13 and meta['rows_seen'] == 6318
    assert meta['source_revision'] == 'abc1234'


def test_two_snapshots_are_distinct_runs(db):
    first = record_snapshot(db, 'schedule', '2026-10-20T04:00:00+00:00')
    second = record_snapshot(db, 'schedule', '2026-10-20T05:00:00+00:00')
    assert first != second


# --- ingestion -------------------------------------------------------------

def fake_provider(teams, games):
    """A getter that counts its calls, so cache reuse is observable."""
    calls = []

    def getter(path, params, **kwargs):
        calls.append(path)
        if path == '/teams':
            return teams
        day = params['startDateRange'][:10]
        return [row for row in games if row['startDate'][:10] == day[:7] + day[7:10]
                or row['startDate'][:7] == day[:7]]

    return getter, calls


def test_ingestion_caches_raw_responses_and_reuses_them(tmp_path, db):
    from src.ncaambb.ingest import cache_paths, ingest_season
    teams = [team(10), team(20, 'Beta')]
    getter, calls = fake_provider(teams, [game()])

    first = ingest_season(db, 2026, raw_root=tmp_path, getter=getter)
    assert first['from_cache'] is False
    assert first['stored_games'] == 1
    assert calls, 'the first run must reach the provider'
    paths = cache_paths(2026, tmp_path)
    assert paths['teams'].exists() and paths['games'].exists()

    calls.clear()
    second = ingest_season(db, 2026, raw_root=tmp_path, getter=getter)
    assert second['from_cache'] is True
    assert calls == [], 'a cached season must cost no requests'
    assert second['stored_games'] == 1
    assert db.execute('SELECT COUNT(*) AS n FROM game').fetchone()['n'] == 1


def test_refetching_ignores_the_cache(tmp_path, db):
    from src.ncaambb.ingest import ingest_season
    teams = [team(10), team(20, 'Beta')]
    getter, calls = fake_provider(teams, [game()])
    ingest_season(db, 2026, raw_root=tmp_path, getter=getter)
    calls.clear()
    again = ingest_season(db, 2026, raw_root=tmp_path, reuse_cache=False,
                          getter=getter)
    assert again['from_cache'] is False
    assert calls, 'refetch must reach the provider again'


def test_an_interrupted_write_leaves_no_partial_cache_to_reuse(tmp_path):
    from src.ncaambb.ingest import cache_paths, write_cache
    teams_path = cache_paths(2026, tmp_path)['teams']
    write_cache(teams_path, [team(10)])
    assert teams_path.exists()
    assert not list(teams_path.parent.glob('*.tmp'))


def test_each_ingestion_records_its_own_snapshot(tmp_path, db):
    from src.ncaambb.ingest import ingest_season
    teams = [team(10), team(20, 'Beta')]
    getter, _ = fake_provider(teams, [game()])
    first = ingest_season(db, 2026, raw_root=tmp_path, getter=getter,
                          revision='abc1234')
    second = ingest_season(db, 2026, raw_root=tmp_path, getter=getter,
                           revision='abc1234')
    assert first['snapshot_id'] != second['snapshot_id']
    notes = [row['notes'] for row in
             db.execute('SELECT notes FROM snapshot ORDER BY snapshot_id')]
    assert notes == ['fetched from provider', 'reused cached responses']


def test_an_access_failure_exits_two_without_storing_a_season(tmp_path, monkeypatch):
    from src.ncaambb import ingest as ingest_module
    from src.ncaambb.http import CbbdAuthError

    def boom(*args, **kwargs):
        raise CbbdAuthError('HTTP 401')

    monkeypatch.setattr(ingest_module, 'get_json', boom)
    assert ingest_module.main(['--seasons', '2026',
                               '--database', str(tmp_path / 'x.db'),
                               '--raw-root', str(tmp_path)]) == 2
    connection = sqlite3.connect(str(tmp_path / 'x.db'))
    try:
        stored = connection.execute('SELECT COUNT(*) AS n FROM game').fetchone()[0]
    finally:
        connection.close()
    assert stored == 0


# --- rosters and people ----------------------------------------------------

def player(identifier=101, name='A Guard', **over):
    row = dict(id=identifier, name=name, firstName=name.split()[0],
               lastName=name.split()[-1], jersey='3', position='G',
               height=75.0, weight=180.0, dateOfBirth='2004-02-11',
               hometown=dict(city='Provo', state='UT', country='USA'))
    row.update(over)
    return row


def box_line(athlete=101, points=12, minutes=28.0, starter=True):
    return dict(athleteId=athlete, name='A Guard', position='G',
                starter=starter, minutes=minutes, points=points)


def box_score(game_id=1, team=10, lines=None, **over):
    row = dict(gameId=game_id, teamId=team, team='Alpha', isHome=True,
               gameMinutes=200.0, gamePace=68.0, possessions=68.0,
               players=lines if lines is not None else [box_line()])
    row.update(over)
    return row


@pytest.fixture
def loaded(db):
    """A database holding one season's teams and one final game."""
    load_season(db, 2026, [game()], [team(10), team(20, 'Beta')])
    return db


def test_a_player_is_keyed_by_the_provider_id_and_never_by_name(loaded):
    upsert_players(loaded, [player(101, 'A Guard'), player(202, 'A Guard')])
    rows = loaded.execute('SELECT player_key, provider_id, name FROM player'
                          ' ORDER BY provider_id').fetchall()
    assert [r['player_key'] for r in rows] == [entity_key('player', 101),
                                               entity_key('player', 202)]
    assert {r['name'] for r in rows} == {'A Guard'}


def test_loading_the_same_roster_twice_leaves_the_same_rows(loaded):
    pairs = [(player(101), 10), (player(202, 'B Forward'), 20)]
    people = [p for p, _ in pairs]
    upsert_players(loaded, people)
    upsert_player_team_seasons(loaded, 2026, pairs)
    counts = lambda: (loaded.execute('SELECT COUNT(*) AS n FROM player').fetchone()['n'],
                      loaded.execute('SELECT COUNT(*) AS n FROM player_team_season')
                      .fetchone()['n'])
    before = counts()
    upsert_players(loaded, people)
    upsert_player_team_seasons(loaded, 2026, pairs)
    assert before == counts() == (2, 2)


def test_a_transfer_is_two_memberships_of_one_player(loaded):
    upsert_players(loaded, [player(101)])
    upsert_player_team_seasons(loaded, 2026, [(player(101), 10)])
    upsert_player_team_seasons(loaded, 2025, [(player(101), 20)])
    rows = loaded.execute('SELECT team_key, season, season_label FROM'
                          ' player_team_season ORDER BY season').fetchall()
    assert [(r['team_key'], r['season_label']) for r in rows] == [
        (entity_key('team', 20), season_label(2025)),
        (entity_key('team', 10), season_label(2026))]
    assert loaded.execute('SELECT COUNT(*) AS n FROM player').fetchone()['n'] == 1


def test_a_later_partial_row_cannot_erase_a_name_already_known(loaded):
    """Box scores carry a name and no birth date; rosters carry both. Whichever
    arrives second must not blank what the other supplied."""
    upsert_players(loaded, [player(101, 'A Guard')])
    upsert_players(loaded, [dict(id=101, name=None, dateOfBirth=None)])
    row = loaded.execute('SELECT name, date_of_birth, hometown FROM player').fetchone()
    assert row['name'] == 'A Guard'
    assert row['date_of_birth'] == '2004-02-11'
    assert row['hometown'] == 'Provo'


def test_a_player_with_no_hometown_object_is_stored_without_one(loaded):
    upsert_players(loaded, [player(101, hometown=None)])
    row = loaded.execute('SELECT hometown, hometown_state FROM player').fetchone()
    assert row['hometown'] is None and row['hometown_state'] is None


def test_a_player_with_no_id_is_refused_rather_than_guessed(loaded):
    with pytest.raises(Exception):
        upsert_players(loaded, [player(None)])


# --- box scores -------------------------------------------------------------

def test_box_score_lines_are_stored_for_both_teams(loaded):
    upsert_players(loaded, [player(101), player(202, 'B Forward')])
    result = upsert_box_scores(loaded, [
        box_score(1, 10, [box_line(101, points=70)]),
        box_score(1, 20, [box_line(202, points=60)], isHome=False)])
    assert result['team_lines'] == 2
    assert result['player_lines'] == 2
    assert result['skipped_unknown_games'] == 0
    assert result['points_mismatches'] == 0
    rows = loaded.execute('SELECT team_key, is_home, points FROM team_game_stat'
                          ' ORDER BY team_key').fetchall()
    assert [(r['is_home'], r['points']) for r in rows] == [(1, 70), (0, 60)]


def test_loading_the_same_box_score_twice_leaves_the_same_rows(loaded):
    upsert_players(loaded, [player(101)])
    boxes = [box_score(1, 10, [box_line(101, points=70)])]
    first = upsert_box_scores(loaded, boxes)
    second = upsert_box_scores(loaded, boxes)
    assert first == second
    assert loaded.execute('SELECT COUNT(*) AS n FROM player_game_stat'
                          ).fetchone()['n'] == 1


def test_a_box_score_for_a_game_this_database_lacks_is_counted_not_dropped(loaded):
    """Silently skipping it would hide an incomplete schedule load, which is
    the one failure the whole windowed fetch exists to prevent."""
    upsert_players(loaded, [player(101)])
    result = upsert_box_scores(loaded, [box_score(999, 10)])
    assert result['skipped_unknown_games'] == 1
    assert result['team_lines'] == 0
    assert loaded.execute('SELECT COUNT(*) AS n FROM team_game_stat'
                          ).fetchone()['n'] == 0


def test_a_box_score_total_that_contradicts_the_final_score_is_reported(loaded):
    """The game's own final score stays authoritative. The disagreement is
    counted so it can be shown, because the pilot found exactly this kind of
    gap in the lineup feed."""
    upsert_players(loaded, [player(101)])
    result = upsert_box_scores(loaded, [box_score(1, 10, [box_line(101, points=55)])])
    assert result['points_mismatches'] == 1
    assert loaded.execute('SELECT home_points FROM game').fetchone()['home_points'] == 70


def test_a_box_score_with_no_points_recorded_stores_null_not_zero(loaded):
    """Summing an empty list writes 0, which reads as a team held scoreless."""
    upsert_players(loaded, [player(101)])
    result = upsert_box_scores(loaded, [
        box_score(1, 10, [box_line(101, points=None)])])
    assert loaded.execute('SELECT points FROM team_game_stat').fetchone()['points'] is None
    assert loaded.execute('SELECT points FROM player_game_stat'
                          ).fetchone()['points'] is None
    # A game with no measured box-score points is not a disagreement about the
    # score; it is no measurement at all.
    assert result['points_mismatches'] == 0


def test_missing_minutes_stay_null_and_a_boolean_is_not_a_measurement(loaded):
    upsert_players(loaded, [player(101), player(202, 'B Forward')])
    upsert_box_scores(loaded, [box_score(1, 10, [
        box_line(101, minutes=None), box_line(202, minutes=True, points=False)])])
    rows = loaded.execute('SELECT player_key, minutes, points FROM'
                          ' player_game_stat ORDER BY player_key').fetchall()
    assert [(r['minutes'], r['points']) for r in rows] == [(None, 12), (None, None)]


def test_a_line_with_no_athlete_id_is_skipped_without_losing_the_team_line(loaded):
    upsert_players(loaded, [player(101)])
    result = upsert_box_scores(loaded, [box_score(1, 10, [
        box_line(101, points=40), box_line(None, points=30)])])
    assert result['player_lines'] == 1
    assert result['team_lines'] == 1
    # The unattributed 30 points still belong to the team's box score total.
    assert loaded.execute('SELECT points FROM team_game_stat').fetchone()['points'] == 70


def test_a_box_score_line_for_an_unknown_player_is_refused_not_orphaned(loaded):
    with pytest.raises(sqlite3.IntegrityError):
        upsert_box_scores(loaded, [box_score(1, 10, [box_line(404)])])


def test_a_starter_flag_the_provider_omits_stays_unknown(loaded):
    upsert_players(loaded, [player(101)])
    upsert_box_scores(loaded, [box_score(1, 10, [box_line(101, starter=None)])])
    assert loaded.execute('SELECT starter FROM player_game_stat'
                          ).fetchone()['starter'] is None


# --- people ingestion -------------------------------------------------------

def people_provider(teams, games, rosters, boxes):
    """A getter serving all four endpoints and recording the paths it served."""
    calls = []

    def getter(path, params, **kwargs):
        calls.append(path)
        if path == '/teams':
            return teams
        if path == '/teams/roster':
            return rosters
        window = (params['startDateRange'][:10], params['endDateRange'][:10])
        rows = games if path == '/games' else boxes
        return [row for row in rows
                if window[0] <= row['startDate'][:10] <= window[1]]

    return getter, calls


def test_a_season_of_people_is_stored_from_rosters_and_box_scores(tmp_path, db):
    from src.ncaambb.ingest import ingest_people, ingest_season
    teams = [team(10), team(20, 'Beta')]
    rosters = [dict(teamId=10, team='Alpha', players=[player(101)]),
               dict(teamId=20, team='Beta', players=[player(202, 'B Forward')])]
    boxes = [box_score(1, 10, [box_line(101, points=70)],
                       startDate='2025-11-07T19:00:00Z'),
             box_score(1, 20, [box_line(202, points=60),
                               box_line(303, points=0)], isHome=False,
                       startDate='2025-11-07T19:00:00Z')]
    getter, calls = people_provider(teams, [game()], rosters, boxes)

    ingest_season(db, 2026, raw_root=tmp_path, getter=getter)
    result = ingest_people(db, 2026, raw_root=tmp_path, getter=getter)

    assert calls.count('/teams/roster') == 1, 'rosters are one request a season'
    assert result['memberships'] == 2
    assert result['team_lines'] == 2
    assert result['player_lines'] == 3
    assert result['skipped_unknown_games'] == 0
    # 303 never appeared on a roster and still has an identity and a line.
    ids = [row['provider_id'] for row in
           db.execute('SELECT provider_id FROM player ORDER BY provider_id')]
    assert ids == [101, 202, 303]
    assert db.execute('SELECT COUNT(*) AS n FROM player_team_season'
                      ).fetchone()['n'] == 2


def test_people_are_cached_separately_from_the_schedule(tmp_path, db):
    """A schedule refresh must not force two dozen box score windows again,
    and reloading people must not refetch the schedule."""
    from src.ncaambb.ingest import cache_paths, ingest_people, ingest_season
    teams = [team(10), team(20, 'Beta')]
    rosters = [dict(teamId=10, team='Alpha', players=[player(101)])]
    getter, calls = people_provider(teams, [game()], rosters, [])

    ingest_season(db, 2026, raw_root=tmp_path, getter=getter)
    paths = cache_paths(2026, tmp_path)
    assert not paths['rosters'].exists(), 'the schedule run fetched no rosters'

    first = ingest_people(db, 2026, raw_root=tmp_path, getter=getter)
    assert first['from_cache'] is False
    assert paths['rosters'].exists() and paths['box_scores'].exists()

    calls.clear()
    second = ingest_people(db, 2026, raw_root=tmp_path, getter=getter)
    assert second['from_cache'] is True
    assert calls == [], 'a cached season of people must cost no requests'
    assert first['people'] == second['people']
    assert db.execute('SELECT COUNT(*) AS n FROM player').fetchone()['n'] == 1


def test_people_ingestion_records_its_own_snapshot_kind(tmp_path, db):
    from src.ncaambb.ingest import ingest_people, ingest_season
    getter, _ = people_provider([team(10), team(20, 'Beta')], [game()],
                                [dict(teamId=10, players=[player(101)])], [])
    ingest_season(db, 2026, raw_root=tmp_path, getter=getter)
    ingest_people(db, 2026, raw_root=tmp_path, getter=getter)
    kinds = [row['kind'] for row in
             db.execute('SELECT kind FROM snapshot ORDER BY snapshot_id')]
    assert kinds == ['schedule', 'people']


def run_main(tmp_path, monkeypatch, *flags):
    """Run the command line against a temporary database and return the paths
    it served, plus a fresh connection to what it wrote."""
    from src.ncaambb import ingest as ingest_module
    getter, calls = people_provider([team(10), team(20, 'Beta')], [game()],
                                    [dict(teamId=10, players=[player(101)])], [])
    monkeypatch.setattr(ingest_module, 'get_json', getter)
    database = tmp_path / 'ncaambb.db'
    code = ingest_module.main(['--seasons', '2026', '--database', str(database),
                               '--raw-root', str(tmp_path / 'raw'), *flags])
    return code, calls, connect(database)


def test_schedule_only_fetches_no_people(tmp_path, monkeypatch):
    """The flag has to actually skip the work, not just the reporting."""
    code, calls, written = run_main(tmp_path, monkeypatch, '--schedule-only')
    try:
        assert code == 0
        assert '/teams/roster' not in calls
        assert written.execute('SELECT COUNT(*) AS n FROM player'
                               ).fetchone()['n'] == 0
        assert written.execute('SELECT COUNT(*) AS n FROM game'
                               ).fetchone()['n'] == 1
    finally:
        written.close()


def test_the_default_run_does_fetch_people(tmp_path, monkeypatch):
    code, calls, written = run_main(tmp_path, monkeypatch)
    try:
        assert code == 0
        assert '/teams/roster' in calls
        assert written.execute('SELECT COUNT(*) AS n FROM player'
                               ).fetchone()['n'] == 1
    finally:
        written.close()


def test_a_player_counted_once_even_when_rosters_and_box_scores_both_carry_them():
    """Rosters and box scores both name the same person, so counting rows would
    report twice the roll of a season nobody doubled."""
    connection = connect(':memory:')
    try:
        assert upsert_players(connection, [player(101), player(101),
                                           player(202, 'B Forward')]) == 2
    finally:
        connection.close()
