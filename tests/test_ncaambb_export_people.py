"""The people half of the basketball export.

B02 loaded rosters, identities and box scores nationally and nothing ever
published them, so the site's People tab has been drawn disabled since the
league launched. These drive the files behind it.
"""

from pathlib import Path
import json
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb import export as exporter
from src.ncaambb import export_people
from src.ncaambb.identity import entity_key
from src.ncaambb.store import (connect, upsert_box_scores, upsert_games,
                               upsert_player_team_seasons, upsert_players,
                               upsert_team_seasons, upsert_teams)


@pytest.fixture
def db(tmp_path):
    connection = connect(tmp_path / 'ncaambb.db')
    yield connection
    connection.close()


def team(identifier, school=None):
    return dict(id=identifier, school=school or f'School {identifier}',
                mascot='Cats', abbreviation=f'T{identifier}',
                conference='Big Test', conferenceId=1)


def game(identifier, season=2026, home=10, away=20, home_points=70,
         away_points=60, day='2025-11-07T23:00:00Z', status='final'):
    return dict(id=identifier, season=season, seasonType='regular',
                status=status, startDate=day, startTimeTbd=False,
                neutralSite=False, conferenceGame=True, homeTeamId=home,
                awayTeamId=away, homeTeam='A', awayTeam='B',
                homePoints=home_points, awayPoints=away_points,
                homePeriodPoints=None, awayPeriodPoints=None, venueId=1,
                venue='Arena', city='Provo', state='UT', attendance=100)


def player(identifier, name=None, hometown='Provo', state='UT'):
    # The feed nests the hometown, which is the shape `upsert_players` reads.
    return dict(id=identifier, name=name or f'Player {identifier}',
                hometown=dict(city=hometown, state=state, country='USA'),
                dateOfBirth=None)


def line(athlete, points=10, minutes=25, starter=True):
    return dict(athleteId=athlete, name=f'Player {athlete}', position='G',
                starter=starter, minutes=minutes, points=points)


def box(game_id, team_id, lines, home=True):
    return dict(gameId=game_id, teamId=team_id, isHome=home,
                gameMinutes=200, gamePace=70, possessions=70, players=lines)


# Deliberately high, so a test can seed people ALONGSIDE the national season
# fixture from test_ncaambb_export without colliding on a provider id.
GAME = 900001


def seeded(connection, season=2026):
    """One season with two clubs, two players each, one game played."""
    upsert_teams(connection, [team(10), team(20)])
    upsert_team_seasons(connection, season, [team(10), team(20)])
    upsert_games(connection, [game(GAME, season=season)], [10, 20])
    upsert_players(connection, [player(101), player(102), player(201)])
    upsert_player_team_seasons(connection, season, [
        (player(101), 10), (player(102), 10), (player(201), 20)])
    upsert_box_scores(connection, [
        box(GAME, 10, [line(101, points=20, minutes=30),
                       line(102, points=4, minutes=12, starter=False)]),
        box(GAME, 20, [line(201, points=15, minutes=28)], home=False)])
    return season


# --- the shard rule ---------------------------------------------------------

def test_a_shard_is_the_provider_id_modulo_sixteen():
    """The values a reader's browser has to reproduce exactly.

    The matching guard is in tests/ncaambb_app.test.cjs, and that is where it
    has teeth: Python's % is ALREADY floored, so this file cannot tell the
    floored form from the bare one. JavaScript's is not, and -3 % 16 is -3
    there, which is a file that does not exist.
    """
    assert export_people.shard_of(0) == 0
    assert export_people.shard_of(16) == 0
    assert export_people.shard_of(19) == 3
    assert export_people.shard_of(-3) == 13
    assert export_people.shard_of('19') == 3


def test_every_shard_index_is_inside_the_range():
    seen = {export_people.shard_of(value)
            for value in range(-100, 100)}
    assert seen == set(range(export_people.SHARDS))


# --- what the files say -----------------------------------------------------

def test_a_season_with_no_people_exports_no_people_file(db):
    upsert_teams(db, [team(10), team(20)])
    upsert_team_seasons(db, 2026, [team(10), team(20)])
    upsert_games(db, [game(1)], [10, 20])
    files, summary = export_people.payloads(db)
    assert files == {}
    assert summary == []


def test_a_player_appears_once_in_the_index_with_his_hometown(db):
    seeded(db)
    files, _ = export_people.payloads(db)
    _, index = files['people/index.js']
    assert index['seasons'] == [2026]
    assert index['players']['101'] == dict(n='Player 101', h='Provo', s='UT')


def test_a_players_lines_are_in_his_own_shard_and_no_other(db):
    seeded(db)
    files, _ = export_people.payloads(db)
    mine = export_people.shard_of(101)
    _, payload = files[f'people/2026/{mine}.js']
    assert '101' in payload['p']
    for shard in range(export_people.SHARDS):
        if shard == mine:
            continue
        _, other = files[f'people/2026/{shard}.js']
        assert '101' not in other['p']


def test_a_game_line_carries_the_game_the_club_the_minutes_and_the_points(db):
    seeded(db)
    files, _ = export_people.payloads(db)
    _, payload = files[f'people/2026/{export_people.shard_of(101)}.js']
    assert payload['fields'] == ['game', 'team', 'minutes', 'points',
                                 'starter']
    game_id, team_index, minutes, points, starter = payload['p']['101'][0]
    assert game_id == GAME
    assert payload['t'][team_index] == entity_key('team', 10)
    assert (minutes, points, starter) == (30, 20, 1)


def test_an_unrecorded_minute_count_stays_absent_not_zero(db):
    """A player who sat on the bench all night and one whose minutes nobody
    wrote down are different things."""
    seeded(db)
    upsert_box_scores(db, [box(GAME, 10, [line(101, points=0,
                                               minutes=None)])])
    files, _ = export_people.payloads(db)
    _, payload = files[f'people/2026/{export_people.shard_of(101)}.js']
    _, _, minutes, points, _ = payload['p']['101'][0]
    assert minutes is None
    assert points == 0, 'a measured zero is still a measurement'


def test_a_season_line_is_the_players_own_lines_added_up(db):
    seeded(db)
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    rows = {row[0]: row for row in roster['teams'][entity_key('team', 10)]}
    assert roster['fields'] == ['player', 'jersey', 'position', 'games',
                               'minutes', 'points']
    assert rows[101][3:] == [1, 30, 20]
    assert rows[102][3:] == [1, 12, 4]


def test_a_season_total_is_absent_when_nothing_was_ever_recorded(db):
    """The roster row, not the game line.

    A total of nothing recorded is 0 by addition, and writing that 0 claims a
    measurement nobody took. Points are measured here and minutes are not, so
    a row that showed both or neither would pass whatever the code did.
    """
    seeded(db)
    upsert_box_scores(db, [box(GAME, 10, [line(101, points=0, minutes=None)])])
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    rows = {row[0]: row for row in roster['teams'][entity_key('team', 10)]}
    assert rows[101][3] == 1, 'he played'
    assert rows[101][4] is None, 'nobody wrote down his minutes'
    assert rows[101][5] == 0, 'and a measured nil is a measurement'

    # And the other way round, because the two totals are separate guards:
    # minutes recorded, points not.
    upsert_box_scores(db, [box(GAME, 10, [line(102, points=None,
                                               minutes=12)])])
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    rows = {row[0]: row for row in roster['teams'][entity_key('team', 10)]}
    assert rows[102][4] == 12
    assert rows[102][5] is None


def test_a_roster_is_ordered_by_scoring_not_by_id(db):
    """A roster ordered by name or id buries the five players a reader came
    for. The ids here deliberately CONTRADICT the scoring, because ordering by
    id is the tie-break: with ids that agreed, no order at all would pass."""
    seeded(db)
    upsert_players(db, [player(103)])
    upsert_player_team_seasons(db, 2026, [(player(103), 10)])
    upsert_box_scores(db, [
        box(GAME, 10, [line(101, points=20, minutes=30),
                       line(102, points=4, minutes=12, starter=False),
                       line(103, points=31, minutes=33)])])
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    order = [row[0] for row in roster['teams'][entity_key('team', 10)]]
    assert order == [103, 101, 102]


def test_a_player_with_a_line_but_no_roster_row_is_still_on_the_club(db):
    """29 Division I players in 2025-26 recorded a line without appearing in
    any roster snapshot. Dropping them loses real minutes."""
    seeded(db)
    upsert_players(db, [player(999)])
    upsert_box_scores(db, [box(GAME, 10, [line(101, points=20, minutes=30),
                                          line(999, points=7, minutes=9)])])
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    rows = {row[0]: row for row in roster['teams'][entity_key('team', 10)]}
    assert 999 in rows
    assert rows[999][1] is None, 'no jersey, because no roster row says one'
    assert rows[999][3:] == [1, 9, 7]

    # His totals are guarded separately from a listed player's, because they
    # are built in a separate loop. Nothing recorded stays absent there too.
    upsert_players(db, [player(998)])
    upsert_box_scores(db, [box(GAME, 10, [line(101, points=20, minutes=30),
                                          line(998, points=0, minutes=None)])])
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    rows = {row[0]: row for row in roster['teams'][entity_key('team', 10)]}
    assert rows[998][3] == 1
    assert rows[998][4] is None
    assert rows[998][5] == 0

    upsert_players(db, [player(997)])
    upsert_box_scores(db, [box(GAME, 10, [line(101, points=20, minutes=30),
                                          line(997, points=None, minutes=9)])])
    files, _ = export_people.payloads(db)
    _, roster = files['people/2026/roster.js']
    rows = {row[0]: row for row in roster['teams'][entity_key('team', 10)]}
    assert rows[997][4] == 9
    assert rows[997][5] is None


def test_a_membership_carries_the_club_jersey_and_position(db):
    seeded(db)
    db.execute("UPDATE player_team_season SET jersey = '3',"
               " position = 'G', height = 75, weight = 185"
               " WHERE player_key = ?", (entity_key('player', 101),))
    db.commit()
    files, _ = export_people.payloads(db)
    _, payload = files[f'people/2026/{export_people.shard_of(101)}.js']
    team_index, jersey, position, height, weight = payload['m']['101'][0]
    assert payload['t'][team_index] == entity_key('team', 10)
    assert (jersey, position, height, weight) == ('3', 'G', 75, 185)


def test_a_transfer_keeps_two_memberships_and_two_season_lines(db):
    """Nothing is merged and nothing is summed across clubs: the provider id
    is the identity and the membership is per club."""
    seeded(db)
    upsert_player_team_seasons(db, 2026, [(player(101), 20)])
    files, _ = export_people.payloads(db)
    _, payload = files[f'people/2026/{export_people.shard_of(101)}.js']
    assert len(payload['m']['101']) == 2
    _, roster = files['people/2026/roster.js']
    both = {key: [row for row in rows if row[0] == 101]
            for key, rows in roster['teams'].items()}
    assert len(both[entity_key('team', 10)]) == 1
    assert len(both[entity_key('team', 20)]) == 1
    # The club he played the game for has the game; the other has none.
    assert both[entity_key('team', 10)][0][3] == 1
    assert both[entity_key('team', 20)][0][3] == 0


def test_only_seasons_that_have_people_are_named(db):
    seeded(db, 2026)
    upsert_games(db, [game(GAME + 1, season=2027,
                           day='2026-11-07T23:00:00Z')], [10, 20])
    upsert_team_seasons(db, 2027, [team(10), team(20)])
    files, summary = export_people.payloads(db)
    _, index = files['people/index.js']
    assert index['seasons'] == [2026]
    assert [row['season'] for row in summary] == [2026]
    assert 'people/2027/roster.js' not in files


def test_a_season_known_only_from_box_scores_is_still_a_season(db):
    """Rosters and box scores are fetched separately.

    A season can hold game lines and no roster snapshot at all -- the 2026-27
    ingest found 29 Division I players in exactly that position -- and
    measuring the seasons from the roster table alone would publish no people
    files for it while the minutes sat in the database.
    """
    upsert_teams(db, [team(10), team(20)])
    upsert_team_seasons(db, 2026, [team(10), team(20)])
    upsert_games(db, [game(GAME, season=2026)], [10, 20])
    upsert_players(db, [player(101)])
    upsert_box_scores(db, [box(GAME, 10, [line(101, points=20, minutes=30)])])
    assert export_people.people_seasons(db) == [2026]
    files, summary = export_people.payloads(db)
    assert 'people/2026/roster.js' in files
    assert [row['season'] for row in summary] == [2026]
    assert files['people/2026/5.js'][1]['p'] == {
        '101': [[GAME, 0, 30, 20, 1]]}


def test_only_the_most_recent_seasons_get_people_files():
    """The published payload is bounded, because a season is 127,500 lines.

    Thirteen seasons of people against a site already measured at 461 MB of
    GitHub Pages' 1 GB is the thing this bound exists to stop.
    """
    archive = list(range(2015, 2028))
    assert export_people.recent(archive) == [2022, 2023, 2024, 2025, 2026,
                                             2027]
    assert export_people.recent(archive, 2) == [2026, 2027]
    # Newest, not first: a bound that took the head of the list would publish
    # 2015 and leave out the season being played.
    assert export_people.recent(archive, 1) == [2027]
    # None is the default and 0 is "no bound", which are different answers.
    assert export_people.recent(archive, None) == export_people.recent(archive)
    assert export_people.recent(archive, 0) == archive
    assert export_people.recent([2026, 2015], 1) == [2026]


def test_the_bound_is_applied_to_the_files_that_are_written(db):
    seeded(db, 2026)
    upsert_games(db, [game(GAME + 1, season=2027,
                           day='2026-11-07T23:00:00Z')], [10, 20])
    upsert_team_seasons(db, 2027, [team(10), team(20)])
    upsert_player_team_seasons(db, 2027, [(player(101), 10)])
    files, summary = export_people.payloads(db, limit=1)
    assert [row['season'] for row in summary] == [2027]
    assert 'people/2026/roster.js' not in files
    _, index = files['people/index.js']
    assert index['seasons'] == [2027], 'the index names what was published'
    # And holds only those seasons' players. 102 and 201 played in 2026 alone,
    # so an index built over the whole archive would offer a reader two names
    # whose every page is a season this export does not carry.
    assert sorted(index['players']) == ['101']


def test_a_season_the_caller_did_not_ask_for_is_left_out(db):
    seeded(db, 2026)
    files, _ = export_people.payloads(db, seasons=[2027])
    assert files == {}


# --- how they are written ---------------------------------------------------

def test_every_shard_file_exists_even_when_it_holds_nobody(db):
    """A page fetches one shard by number. A missing file is a 404 a reader
    sees; an empty one is an answer."""
    seeded(db)
    files, _ = export_people.payloads(db)
    for shard in range(export_people.SHARDS):
        assert f'people/2026/{shard}.js' in files
    empty = [shard for shard in range(export_people.SHARDS)
             if not files[f'people/2026/{shard}.js'][1]['p']]
    assert empty, 'three players cannot fill sixteen shards'


def test_a_shard_pushes_onto_a_queue_so_the_files_may_load_in_any_order(db):
    seeded(db)
    files, _ = export_people.payloads(db)
    text = export_people.render(*files['people/2026/0.js'])
    assert text.startswith('(window.NCAAMBB_PLAYER_GAMES=')
    assert '.push(' in text


def test_a_roster_file_creates_its_container_before_assigning_into_it(db):
    seeded(db)
    files, _ = export_people.payloads(db)
    text = export_people.render(*files['people/2026/roster.js'])
    assert text.startswith('window.NCAAMBB_ROSTERS=(window.NCAAMBB_ROSTERS||{})')
    assert 'window.NCAAMBB_ROSTERS[2026]=' in text


def test_the_manifest_names_every_people_file_with_its_hash(db, tmp_path):
    from tests.test_ncaambb_export import national, NOW
    national(db)
    seeded(db)
    manifest, payloads, teams, people = exporter.build(db, now=NOW)
    written = exporter.write(manifest, payloads, teams, tmp_path / 'out',
                             people=people)
    section = written['files']['people']
    assert section['shards'] == export_people.SHARDS
    assert '?v=' in section['index']
    season = section['seasons']['2026']
    assert len(season['shards']) == export_people.SHARDS
    assert '?v=' in season['roster']
    # The gate's own file check walks the manifest, so every one of these is
    # verified on disk without the gate knowing anything about people.
    from src.ncaambb import publish_gate
    assert publish_gate.check(written, root=tmp_path / 'out') == []
    for path, _ in publish_gate.manifest_files(written):
        assert (tmp_path / 'out' / path).exists()


def test_the_manifest_lists_the_shards_in_numeric_order(db, tmp_path):
    """Not alphabetical. A plain sort of the paths gives 0, 1, 10, 11, 12,
    which a reader that trusted the order would read as shard 2 at index 2."""
    from tests.test_ncaambb_export import national, NOW
    national(db)
    seeded(db)
    manifest, payloads, teams, people = exporter.build(db, now=NOW)
    written = exporter.write(manifest, payloads, teams, tmp_path / 'out',
                             people=people)
    shards = written['files']['people']['seasons']['2026']['shards']
    numbers = [int(path.split('/')[-1].split('.')[0]) for path in shards]
    assert numbers == list(range(export_people.SHARDS))


def test_a_people_file_edited_after_the_fact_fails_the_gate(db, tmp_path):
    from tests.test_ncaambb_export import national, NOW
    from src.ncaambb import publish_gate
    national(db)
    seeded(db)
    manifest, payloads, teams, people = exporter.build(db, now=NOW)
    written = exporter.write(manifest, payloads, teams, tmp_path / 'out',
                             people=people)
    target = tmp_path / 'out' / 'people' / 'index.js'
    target.write_text(target.read_text() + '\n// edited\n', encoding='utf-8')
    reasons = publish_gate.check(written, root=tmp_path / 'out')
    assert any('people/index.js' in reason for reason in reasons)


def test_an_export_with_no_people_still_writes_and_passes(db, tmp_path):
    """Basketball published for eleven days with no people at all. That
    export must stay publishable."""
    from tests.test_ncaambb_export import national, NOW
    from src.ncaambb import publish_gate
    national(db)
    manifest, payloads, teams, people = exporter.build(db, now=NOW)
    assert people == {}
    written = exporter.write(manifest, payloads, teams, tmp_path / 'out',
                             people=people)
    assert 'people' not in written['files']
    assert publish_gate.check(written, root=tmp_path / 'out') == []


def test_the_summary_reports_what_each_season_holds(db):
    seeded(db)
    _, summary = export_people.payloads(db)
    assert summary == [dict(season=2026, players=3, clubs=2, lines=3)]
