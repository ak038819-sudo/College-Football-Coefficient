"""Snap counts and the weekly injury report.

Every rule here was measured against the real archive before it was written
down, and what is pinned is the reasoning rather than a count: the files are
refetched whole every refresh, so a test asserting 333,129 snap rows would
fail the Sunday a club corrects a report.

What must not move: that a snap row reaches a player only through the
Pro-Football-Reference crosswalk, that a published-but-empty season is not a
failure, that the latest injury report wins over the one it replaced, that a
report for a game that never happened is kept, and that a season's share is
the average of the games he played rather than a ratio this archive cannot
compute.
"""

import pathlib

import pytest

from src.nfl import (games, people, people_export, people_source, store,
                     usage)

SEASON = 2024
GAME = 'nfl:nflverse:game:2024_01_BAL_KC'
KC = 'nfl:nflverse:franchise:KC'
BAL = 'nfl:nflverse:franchise:BAL'
MAHOMES = 'nfl:nflverse:player:00-0033873'


def game_row(game_id='2024_01_BAL_KC', week=1, game_type='REG', **extra):
    row = dict(game_id=game_id, season=SEASON, game_type=game_type, week=week,
               gameday='2024-09-05', gametime='20:20', home_team='KC',
               away_team='BAL', home_score='27', away_score='20',
               location='Home', overtime='0')
    row.update(extra)
    return row


def player_row(gsis_id='00-0033873', pfr_id='MahoPa00', **extra):
    row = dict(gsis_id=gsis_id, pfr_id=pfr_id, display_name='Patrick Mahomes',
               first_name='Patrick', last_name='Mahomes', position='QB',
               position_group='QB', height='75', weight='225',
               birth_date='1995-09-17', college_name='Texas Tech',
               college_conference='Big 12', jersey_number='15',
               rookie_season='2017', last_season='2026', latest_team='KC',
               status='ACT', years_of_experience='8', draft_year='2017',
               draft_round='1', draft_pick='10', draft_team='KC')
    row.update(extra)
    return row


def snap_row(game_id='2024_01_BAL_KC', pfr='MahoPa00', **extra):
    row = dict(game_id=game_id, season=str(SEASON), game_type='REG', week='1',
               player='Patrick Mahomes', pfr_player_id=pfr, position='QB',
               team='KC', opponent='BAL', offense_snaps='70',
               offense_pct='1.0', defense_snaps='0', defense_pct='0',
               st_snaps='0', st_pct='0')
    row.update(extra)
    return row


def injury_row(gsis_id='00-0033873', **extra):
    row = dict(season=str(SEASON), game_type='REG', team='KC', week='1',
               gsis_id=gsis_id, position='QB', full_name='Patrick Mahomes',
               first_name='Patrick', last_name='Mahomes',
               report_primary_injury='Ankle', report_secondary_injury='',
               report_status='Questionable',
               practice_primary_injury='Ankle',
               practice_secondary_injury='',
               practice_status='Limited Participation in Practice',
               date_modified='2024-09-06T19:05:30Z')
    row.update(extra)
    return row


@pytest.fixture
def db():
    connection = store.connect(':memory:')
    store.upsert_teams(connection, [
        dict(season=SEASON, team=code, full=code, location=None,
             nickname=None) for code in ('KC', 'BAL')])
    store.upsert_games(connection, [games.parse_row(game_row())])
    parsed, _ = people.parse_players([player_row()])
    people.upsert_players(connection, parsed)
    yield connection
    connection.close()


def load(connection, snaps=(), injuries=()):
    players_pfr = usage.pfr_index(connection)
    players_gsis = usage.gsis_index(connection)
    by_provider, by_week = usage.game_index(connection)
    snap_rows, snap_drops = usage.parse_snaps(
        list(snaps), players=players_pfr, games=by_provider)
    injury_rows, injury_drops = usage.parse_injuries(
        list(injuries), players=players_gsis, games=by_week)
    usage.replace_snaps(connection, SEASON, snap_rows)
    usage.replace_injuries(connection, SEASON, injury_rows)
    return snap_drops, injury_drops


# --- the crosswalk ---------------------------------------------------------

def test_a_snap_row_reaches_a_player_only_through_the_pfr_id(db):
    """The snap file carries NO GSIS id. Its only person id is
    Pro-Football-Reference's, so `player.pfr_id` is the whole join, and a
    player stored without one is unreachable from it."""
    assert usage.pfr_index(db) == {'MahoPa00': MAHOMES}
    rows, drops = usage.parse_snaps(
        [snap_row()], players=usage.pfr_index(db),
        games=usage.game_index(db)[0])
    assert [row['player_key'] for row in rows] == [MAHOMES]
    assert drops['no_player'] == 0


def test_the_player_file_s_pfr_id_is_stored(db):
    assert db.execute(
        'SELECT pfr_id FROM player WHERE player_key = ?',
        (MAHOMES,)).fetchone()[0] == 'MahoPa00'


def test_a_snap_row_for_an_unknown_person_is_dropped_and_counted(db):
    """29 people in the archive have snaps and no crosswalk entry. A snap
    count with nobody to attribute it to has no page to be shown on, so it is
    dropped — but silently dropping it would hide the day the crosswalk
    breaks."""
    rows, drops = usage.parse_snaps(
        [snap_row(pfr='NobodyXX00')], players=usage.pfr_index(db),
        games=usage.game_index(db)[0])
    assert rows == []
    assert drops['no_player'] == 1


def test_a_snap_row_is_keyed_on_the_source_s_game_id_not_the_game_key(db):
    """`game_id` in the snap file is this archive's `game.provider_id`.
    Reading it as the key resolves NOTHING: every row in every season."""
    by_provider, _ = usage.game_index(db)
    assert '2024_01_BAL_KC' in by_provider
    assert by_provider['2024_01_BAL_KC'] == GAME
    rows, drops = usage.parse_snaps(
        [snap_row(game_id=GAME)], players=usage.pfr_index(db),
        games=by_provider)
    assert rows == [] and drops['no_game'] == 1


def test_a_repeated_player_game_is_dropped_rather_than_colliding(db):
    rows, drops = usage.parse_snaps(
        [snap_row(), snap_row(offense_snaps='3')],
        players=usage.pfr_index(db), games=usage.game_index(db)[0])
    assert len(rows) == 1 and rows[0]['offense_snaps'] == 70
    assert drops['duplicate'] == 1


# --- a published file that is empty ---------------------------------------

def test_an_empty_snap_season_is_no_rows_and_not_a_failure(db):
    """`snap_counts_2012.csv` exists and holds a header and nothing else.
    Published and empty is not absent, and neither is a failure."""
    rows, drops = usage.parse_snaps(
        [], players=usage.pfr_index(db), games=usage.game_index(db)[0])
    assert rows == []
    assert not any(drops.values())


def test_the_fetcher_asks_for_no_season_before_a_file_exists():
    """Snap counts begin in 2013 and injuries in 2009. Asking for 1999 would
    404 on a file that was never published, which is not a failure to
    report."""
    asked = []

    def opener(request, timeout=None):
        asked.append(request.full_url if hasattr(request, 'full_url')
                     else request)
        raise AssertionError('no season in range here should be fetched')

    bundle, origins = people_source.load_usage(
        seasons=[2000, 2005], root='/tmp/nothing-here', opener=opener)
    assert asked == []
    assert bundle['snaps'] == {} and bundle['injuries'] == {}
    assert origins == {}


def test_the_two_archives_start_in_different_years():
    assert people_source.SNAP_FIRST_SEASON == 2013
    assert people_source.INJURY_FIRST_SEASON == 2009
    assert (people_source.snap_url(2024).endswith(
        'snap_counts/snap_counts_2024.csv'))
    assert people_source.injury_url(2024).endswith(
        'injuries/injuries_2024.csv')


# --- the injury report -----------------------------------------------------

def test_the_latest_report_wins_over_the_one_it_replaced(db):
    """Two players in week 15 of 2024 appear twice, Questionable on the
    Saturday and Out on the Sunday morning. The club updated its report; the
    last thing it said before kickoff is what the report said."""
    rows, drops = usage.parse_injuries(
        [injury_row(report_status='Questionable',
                    date_modified='2024-09-06T03:34:33Z'),
         injury_row(report_status='Out',
                    date_modified='2024-09-06T19:05:30Z')],
        players=usage.gsis_index(db), games=usage.game_index(db)[1])
    assert [row['report_status'] for row in rows] == ['Out']
    assert drops['superseded'] == 1


def test_the_later_report_wins_whichever_order_it_arrives_in(db):
    rows, _ = usage.parse_injuries(
        [injury_row(report_status='Out',
                    date_modified='2024-09-06T19:05:30Z'),
         injury_row(report_status='Questionable',
                    date_modified='2024-09-06T03:34:33Z')],
        players=usage.gsis_index(db), games=usage.game_index(db)[1])
    assert [row['report_status'] for row in rows] == ['Out']


def test_a_report_for_a_game_that_was_never_played_is_kept(db):
    """Buffalo and Cincinnati both filed for week 17 of 2022 and the game was
    abandoned after Damar Hamlin's cardiac arrest. Those 17 rows are the only
    17 of 92,890 that reach no game, and they are real reports."""
    rows, drops = usage.parse_injuries(
        [injury_row(week='18')],
        players=usage.gsis_index(db), games=usage.game_index(db)[1])
    assert len(rows) == 1
    assert rows[0]['game_key'] is None
    assert rows[0]['week'] == 18
    assert not any(drops.values())


def test_a_report_round_is_stored_in_this_archive_s_words(db):
    rows, _ = usage.parse_injuries(
        [injury_row(game_type='DIV', week='20')],
        players=usage.gsis_index(db), games=usage.game_index(db)[1])
    assert rows[0]['season_type'] == 'divisional'


def test_an_unknown_round_is_refused_rather_than_stored_raw(db):
    rows, drops = usage.parse_injuries(
        [injury_row(game_type='PRE')],
        players=usage.gsis_index(db), games=usage.game_index(db)[1])
    assert rows == [] and drops['no_week'] == 1


def test_whitespace_in_a_practice_line_is_an_absence(db):
    """215 practice lines in the archive are a newline and four spaces."""
    rows, _ = usage.parse_injuries(
        [injury_row(practice_status=usage.BLANK_PRACTICE)],
        players=usage.gsis_index(db), games=usage.game_index(db)[1])
    assert rows[0]['practice_status'] is None


def test_a_relocated_club_s_old_code_resolves_to_the_franchise(db):
    """The report names Oakland in 2019 and Las Vegas in 2020; one
    franchise."""
    from src.nfl.identity import people_franchise_key
    assert people_franchise_key('OAK') == people_franchise_key('LV')


# --- storing ---------------------------------------------------------------

def test_a_season_is_replaced_rather_than_merged(db):
    """The source rewrites these files in place, so a player dropped from a
    report must disappear from here too, which an upsert cannot do."""
    load(db, snaps=[snap_row()], injuries=[injury_row()])
    assert db.execute('SELECT COUNT(*) FROM player_snap').fetchone()[0] == 1
    load(db, snaps=[], injuries=[])
    assert db.execute('SELECT COUNT(*) FROM player_snap').fetchone()[0] == 0
    assert db.execute(
        'SELECT COUNT(*) FROM injury_report').fetchone()[0] == 0


def test_coverage_counts_the_reports_that_reach_no_game(db):
    load(db, snaps=[snap_row()],
         injuries=[injury_row(), injury_row(week='18')])
    got = usage.coverage(db)
    assert got['snaps'] == {SEASON: 1}
    assert got['injuries'] == {SEASON: 2}
    assert got['injuries_without_game'] == 1


def test_ingest_loads_both_archives_and_reports_what_it_dropped(db):
    result = usage.ingest(db, dict(snaps={SEASON: [snap_row(),
                                                   snap_row(pfr='Nobody00')]},
                                   injuries={SEASON: [injury_row()]}))
    assert result['counts'] == dict(snaps=1, injuries=1)
    assert result['skipped']['snaps'][SEASON]['no_player'] == 1


# --- the export ------------------------------------------------------------

def built(connection, settled_before=2026):
    return people_export.build(connection, settled_before=settled_before)


def entry_for(files, gsis='00-0033873'):
    shard = people_export.shard_of(gsis)
    return files[f'people/careers/{shard}.js'][2]['players'][gsis]


def test_a_season_s_share_is_the_mean_of_the_games_he_played(db):
    """NOT his snaps over the team's: the archive holds no team snap total,
    because the source gives a share per game and nothing to add up. A game
    he spent on the bench is not in the mean and would drag it to a number he
    never played at."""
    store.upsert_games(db, [games.parse_row(
        game_row(game_id='2024_02_KC_CIN', week=2))])
    load(db, snaps=[
        snap_row(offense_snaps='70', offense_pct='1.0'),
        snap_row(game_id='2024_02_KC_CIN', offense_snaps='30',
                 offense_pct='0.5')])
    row = entry_for(built(db))['usage'][0]
    fields = people_export.USAGE_FIELDS
    assert row[fields.index('games')] == 2
    assert row[fields.index('off')] == 100
    assert row[fields.index('offShare')] == 0.75


def test_a_unit_he_never_played_carries_no_row_of_noughts(db):
    load(db, snaps=[snap_row()])
    row = entry_for(built(db))['usage'][0]
    fields = people_export.USAGE_FIELDS
    assert row[fields.index('off')] == 70
    assert row[fields.index('def')] is None
    assert row[fields.index('defShare')] is None


def test_the_regular_season_comes_before_the_postseason(db):
    """Sorting the halves by their own names puts 'post' before 'reg' and a
    player's January above his September."""
    store.upsert_games(db, [games.parse_row(
        game_row(game_id='2024_20_BAL_KC', week=20, game_type='DIV'))])
    load(db, snaps=[snap_row(),
                    snap_row(game_id='2024_20_BAL_KC', game_type='DIV')])
    rows = entry_for(built(db))['usage']
    types = [row[people_export.USAGE_FIELDS.index('type')] for row in rows]
    assert types == [0, 1]


def test_the_report_counts_a_blank_status_apart_from_a_designation(db):
    """Two thirds of the rows since 2016 carry a practice line and no game
    status, because the league abolished "Probable" after 2015. Counting
    appearances across that boundary compares two different reports."""
    load(db, injuries=[injury_row(report_status='Out'),
                       injury_row(gsis_id='00-0033873', week='2',
                                  report_status='')])
    store.upsert_games(db, [games.parse_row(
        game_row(game_id='2024_02_KC_CIN', week=2))])
    row = entry_for(built(db))['availability'][0]
    fields = people_export.AVAILABILITY_FIELDS
    assert row[fields.index('out')] == 1
    assert row[fields.index('listed')] == 1
    assert row[fields.index('questionable')] == 0


def test_the_injuries_a_season_carried_are_listed_once_each(db):
    store.upsert_games(db, [games.parse_row(
        game_row(game_id='2024_02_KC_CIN', week=2))])
    load(db, injuries=[injury_row(report_primary_injury='Ankle'),
                       injury_row(week='2',
                                  report_primary_injury='Ankle')])
    row = entry_for(built(db))['availability'][0]
    assert row[people_export.AVAILABILITY_FIELDS.index('injuries')] == [
        'Ankle']


def test_the_payload_names_the_fields_the_page_unpacks_by():
    """The page reads these rows positionally; a field list that drifted from
    the packer would silently shift every column."""
    body = pathlib.Path('ui/nfl_app.js').read_text(encoding='utf-8')
    for fields, marker in ((people_export.USAGE_FIELDS, 'unpackUsage'),
                           (people_export.AVAILABILITY_FIELDS,
                            'unpackAvailability')):
        section = body.split('function ' + marker, 1)[1].split('}', 1)[0]
        for name in fields:
            if name == 'type':
                continue
            assert name + ':' in section, (marker, name)
