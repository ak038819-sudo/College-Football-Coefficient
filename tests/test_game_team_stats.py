"""The team box score: fetch, load, and the one place a string is interpreted.

Nothing published reads this archive yet, which is exactly when a defect is
cheapest to catch: a blank stored as a zero, a team's totals shown under the
other team's name, a conversion out of no attempts reported as 0%.
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import fetch_cfbd_game_team_stats as fetch  # noqa: E402
import load_game_team_stats as load  # noqa: E402
import team_box_score as box  # noqa: E402


def stat(category, value):
    return {'category': category, 'stat': value}


def side(**over):
    row = {'teamId': 333, 'team': 'Alabama', 'conference': 'SEC',
           'homeAway': 'home', 'points': 42,
           'stats': [stat('firstDowns', '24'), stat('thirdDownEff', '6-14')]}
    row.update(over)
    return row


def game(**over):
    row = {'id': 401520281, 'teams': [side(), side(teamId=99, team='Auburn',
                                            homeAway='away', points=10)]}
    row.update(over)
    return row


def games_file(tmp_path, year, weeks=((('regular'), 1), ('postseason', 1))):
    """The committed games file the fetcher reads its week list from."""
    path = tmp_path / f'games_{year}.csv'
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=['game_id', 'week', 'season_type'])
        writer.writeheader()
        for index, (season_type, week) in enumerate(weeks, start=1):
            writer.writerow({'game_id': index, 'week': week,
                             'season_type': season_type})
    return path


# ---- the fetcher: unknown fields stop the run, unknown categories do not ----

def test_a_new_top_level_field_fails_rather_than_being_dropped():
    with pytest.raises(ValueError, match='excitement'):
        fetch.stat_rows([game(excitement=7.2)], 2024)


def test_a_new_team_field_fails_rather_than_being_dropped():
    with pytest.raises(ValueError, match='startElo'):
        fetch.stat_rows([game(teams=[side(startElo=1600)])], 2024)


def test_a_new_stat_field_fails_rather_than_being_dropped():
    with pytest.raises(ValueError, match='rank'):
        row = dict(side(), stats=[{'category': 'firstDowns', 'stat': '24', 'rank': 3}])
        fetch.stat_rows([game(teams=[row])], 2024)


def test_an_unfamiliar_CATEGORY_is_kept_and_does_not_fail():
    """A category is data, not schema. CFBD adds them season by season, and
    refusing one would refuse the season for being new."""
    rows = fetch.stat_rows(
        [game(teams=[side(stats=[stat('qbHurriesAllowed', '3')])])], 2024)
    assert [r['category'] for r in rows] == ['qbHurriesAllowed']


# ---- the fetcher: what it writes --------------------------------------------

def test_every_value_is_kept_as_the_provider_spelled_it():
    rows = fetch.stat_rows(
        [game(teams=[side(stats=[stat('possessionTime', '31:45'),
                                 stat('totalPenaltiesYards', '5-45')])])], 2024)
    assert {r['category']: r['stat'] for r in rows} == {
        'possessionTime': '31:45', 'totalPenaltiesYards': '5-45'}


def test_a_team_with_no_points_is_blank_and_never_zero():
    """0 points is a real and frequent score, so writing 0 for 'not reported'
    would feed a later reader a shutout nobody played."""
    rows = fetch.stat_rows([game(teams=[side(points=None)])], 2024)
    assert rows[0]['points'] == ''


def test_a_game_with_no_id_fails():
    with pytest.raises(ValueError, match='no game id'):
        fetch.stat_rows([game(id=None)], 2024)


def test_a_team_with_no_id_fails():
    with pytest.raises(ValueError, match='no teamId'):
        fetch.stat_rows([game(teams=[side(teamId=None)])], 2024)


def test_a_repeated_category_for_one_team_fails():
    """Two values for one category means one of them is being thrown away, and
    the loader's primary key would silently pick a winner."""
    rows = [stat('firstDowns', '24'), stat('firstDowns', '19')]
    with pytest.raises(ValueError, match='duplicate'):
        fetch.stat_rows([game(teams=[side(stats=rows)])], 2024)


def test_every_column_the_header_declares_is_written():
    rows = fetch.stat_rows([game()], 2024)
    assert set(rows[0]) == set(fetch.CSV_COLUMNS)


def test_the_output_directory_is_resolved_inside_the_function(tmp_path, monkeypatch):
    """A default argument binds at def time, so `out_dir=OUT_DIR` could not be
    patched and this test would pass while the code wrote into the real
    data/raw. That trap has bitten this repository five times."""
    monkeypatch.setattr(fetch, 'OUT_DIR', tmp_path)
    games_file(tmp_path, 2099)
    monkeypatch.setattr(fetch, 'fetch', lambda year, season_type, week, headers:
                        [game()] if season_type == 'regular' else [])
    assert fetch.write_season(2099, {}) > 0
    assert (tmp_path / 'game_team_stats_2099.csv').exists()
    # A season no backfill covers, so a file here could only have come from
    # this test writing into the real archive.
    assert not list(Path('data/raw').glob('game_team_stats_2099.csv'))


def test_the_postseason_is_fetched_too(tmp_path, monkeypatch):
    """Leaving it out would drop every bowl and playoff game in silence."""
    asked = []
    games_file(tmp_path, 2024)

    def spy(year, season_type, week, headers):
        asked.append(season_type)
        return [game()] if season_type == 'postseason' else []

    monkeypatch.setattr(fetch, 'fetch', spy)
    fetch.write_season(2024, {}, out_dir=tmp_path, raw_dir=tmp_path)
    assert asked == ['regular', 'postseason']
    assert (tmp_path / 'game_team_stats_2024.csv').exists()


def test_a_season_with_no_rows_writes_no_file(tmp_path, monkeypatch):
    """A header-only file reads downstream as a season in which no team gained
    a yard, which is the same shape as a real answer."""
    games_file(tmp_path, 2001)
    monkeypatch.setattr(fetch, 'fetch',
                        lambda year, season_type, week, headers: [])
    assert fetch.write_season(2001, {}, out_dir=tmp_path, raw_dir=tmp_path) == 0
    assert not list(tmp_path.glob('game_team_stats_*.csv'))


def test_a_season_already_committed_is_skipped_so_a_backfill_resumes(tmp_path, monkeypatch):
    called = []
    games_file(tmp_path, 2024)
    monkeypatch.setattr(fetch, 'fetch', lambda year, season_type, week, headers:
                        called.append(season_type) or
                        ([game()] if season_type == 'regular' else []))
    fetch.write_season(2024, {}, out_dir=tmp_path, raw_dir=tmp_path)
    called.clear()
    assert fetch.write_season(2024, {}, out_dir=tmp_path, raw_dir=tmp_path) == 0
    assert called == [], 'a committed season must not be re-requested'
    assert fetch.write_season(2024, {}, out_dir=tmp_path, raw_dir=tmp_path,
                              force=True) > 0


def test_a_season_is_requested_week_by_week_with_the_week_in_the_query():
    """/games/teams answers 400 for a year alone: one of week, team or
    conference is required alongside it. The whole backfill failed on its
    first season because the week was missing."""
    import inspect
    source = inspect.getsource(fetch.fetch)
    assert '"week": week' in source, (
        'the request must carry a week, or CFBD refuses it with 400')
    assert 'week' in inspect.signature(fetch.fetch).parameters


def test_the_weeks_come_from_the_archive_so_a_played_week_is_never_skipped(tmp_path):
    """A blind range is a guess in both directions. 2001 played no week 4 and
    ran through week 16, so 1..15 would drop a week in silence."""
    games_file(tmp_path, 2001,
               weeks=[('regular', 1), ('regular', 2), ('regular', 3),
                      ('regular', 5), ('regular', 16), ('postseason', 1)])
    assert fetch.season_weeks(2001, 'regular', tmp_path) == [1, 2, 3, 5, 16]
    assert fetch.season_weeks(2001, 'postseason', tmp_path) == [1]


def test_the_real_archive_names_the_weeks_2001_actually_played():
    """Not a fixture: the committed games file is what the backfill reads, and
    the September 11 cancellations left 2001 with no week 4."""
    weeks = fetch.season_weeks(2001, 'regular')
    assert 16 in weeks, 'week 16 is played and must be requested'
    assert 4 not in weeks, 'no week 4 was played in 2001'


def test_every_week_the_archive_names_is_actually_requested(tmp_path, monkeypatch):
    asked = []
    games_file(tmp_path, 2024,
               weeks=[('regular', 1), ('regular', 14), ('postseason', 1)])
    monkeypatch.setattr(fetch, 'fetch',
                        lambda year, season_type, week, headers:
                        asked.append((season_type, week)) or [])
    assert fetch.season_rows(2024, {}, tmp_path) == []
    assert asked == [('regular', 1), ('regular', 14), ('postseason', 1)]


def test_a_missing_games_file_stops_the_run_rather_than_fetching_nothing(tmp_path):
    """Returning no weeks would write no file and read as a season CFBD has
    no box score for, which is the shape of a real answer."""
    with pytest.raises(FileNotFoundError):
        fetch.season_weeks(2024, 'regular', tmp_path)


def test_a_game_with_no_week_stops_the_run(tmp_path):
    path = tmp_path / 'games_2024.csv'
    path.write_text('game_id,week,season_type\n7,,regular\n', encoding='utf-8')
    with pytest.raises(ValueError, match='no week'):
        fetch.season_weeks(2024, 'regular', tmp_path)


def test_the_raw_directory_is_resolved_inside_season_weeks():
    """The same def-time default trap as the output directory."""
    import inspect
    assert inspect.signature(fetch.season_weeks).parameters['raw_dir'].default is None


def test_a_year_before_the_archive_begins_is_refused_rather_than_requested(capsys):
    assert fetch.main([str(fetch.FIRST_SEASON - 1)]) == 2
    assert str(fetch.FIRST_SEASON) in capsys.readouterr().out


# ---- the loader -------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    conn = sqlite3.connect(':memory:')
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE team_aliases (alias TEXT, team_name TEXT);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                            home_team_id INTEGER, away_team_id INTEGER);
        INSERT INTO teams VALUES (1, 'Alabama'), (2, 'Auburn');
        INSERT INTO games VALUES (500, 2024, 1, 2);
    """)
    conn.executescript(load.SCHEMA.read_text(encoding='utf-8'))
    return conn


def write_csv(tmp_path, season, rows):
    path = tmp_path / f'game_team_stats_{season}.csv'
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fetch.CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def raw(**over):
    row = {'season_year': 2024, 'game_id': 500, 'team_id': 333,
           'school': 'Alabama', 'conference': 'SEC', 'home_away': 'home',
           'points': 42, 'category': 'firstDowns', 'stat': '24'}
    row.update(over)
    return row


def test_a_loaded_value_keeps_the_provider_string(db, tmp_path):
    path = write_csv(tmp_path, 2024, [raw(category='possessionTime', stat='31:45')])
    load.load_file(db, path)
    assert db.execute('SELECT stat FROM game_team_stats').fetchone()[0] == '31:45'


def test_the_internal_team_id_is_stored_not_the_providers(db, tmp_path):
    """CFBD's teamId is a different numbering from this database's. Storing it
    would join to the wrong team or to none, and every later count would be
    wrong in a way no page could show."""
    load.load_file(db, write_csv(tmp_path, 2024, [raw()]))
    assert db.execute('SELECT team_id FROM game_team_stats').fetchone()[0] == 1


def test_a_game_this_database_does_not_carry_is_skipped_and_counted(db, tmp_path):
    result = load.load_file(db, write_csv(tmp_path, 2024, [raw(game_id=999)]))
    assert result['loaded'] == 0 and result['unknown_games'] == [999]


def test_a_school_that_cannot_be_resolved_is_skipped_and_counted(db, tmp_path):
    """/games/teams covers games against FCS opponents, so an unresolvable name
    is usually a real school this database does not carry."""
    result = load.load_file(db, write_csv(tmp_path, 2024, [raw(school='Mercer')]))
    assert result['loaded'] == 0 and result['unmatched'] == ['Mercer']


def test_a_blank_value_is_not_stored_at_all(db, tmp_path):
    """Stored, it would be indistinguishable from a measured zero."""
    load.load_file(db, write_csv(tmp_path, 2024, [raw(stat='')]))
    assert db.execute('SELECT COUNT(*) FROM game_team_stats').fetchone()[0] == 0


def test_reloading_the_same_season_does_not_double_it(db, tmp_path):
    path = write_csv(tmp_path, 2024, [raw(), raw(category='totalYards', stat='417')])
    load.load_file(db, path)
    load.load_file(db, path)
    assert db.execute('SELECT COUNT(*) FROM game_team_stats').fetchone()[0] == 2


def test_a_season_shrinking_loses_the_rows_it_no_longer_has(db, tmp_path):
    load.load_file(db, write_csv(tmp_path, 2024, [raw(), raw(category='totalYards', stat='417')]))
    load.load_file(db, write_csv(tmp_path, 2024, [raw()]))
    assert db.execute('SELECT COUNT(*) FROM game_team_stats').fetchone()[0] == 1


def test_a_row_labelled_another_season_fails_before_anything_is_deleted(db, tmp_path):
    """The file name decides the season. A mislabelled row would otherwise
    empty 2024 and refill it with 2023's rows."""
    load.load_file(db, write_csv(tmp_path, 2024, [raw()]))
    bad = write_csv(tmp_path, 2024, [raw(season_year=2023)])
    with pytest.raises(ValueError, match='2023'):
        load.load_file(db, bad)
    assert db.execute('SELECT COUNT(*) FROM game_team_stats').fetchone()[0] == 1


# ---- interpreting the strings ------------------------------------------------

def test_a_conversion_out_of_no_attempts_has_no_percentage():
    """0 of 0 is not a 0% conversion rate. Nobody failed on fourth down if
    nobody went for it."""
    assert box.percent('fourthDownEff', '0-0') is None
    assert box.percent('fourthDownEff', '1-2') == 50.0


def test_penalties_are_not_read_as_made_of_attempted():
    """"5-45" is five penalties for forty-five yards. A 11.1% there would be a
    number nobody measured."""
    assert box.percent('totalPenaltiesYards', '5-45') is None
    assert box.fraction('totalPenaltiesYards', '5-45') is None


def test_a_clock_is_not_mistaken_for_a_fraction():
    assert box.percent('possessionTime', '31:45') is None


def test_a_category_with_no_curated_label_is_still_labelled():
    assert box.label_for('qbHurriesAllowed') == 'Qb hurries allowed'
    assert box.label_for('') == 'Unnamed'
    assert box.label_for(None) == 'Unnamed'


def test_the_rows_come_out_in_box_score_order_not_the_providers():
    rows = box.panel_rows({'totalYards': '417', 'firstDowns': '24'},
                          {'totalYards': '388', 'firstDowns': '19'})
    assert [r['category'] for r in rows] == ['firstDowns', 'totalYards']


def test_an_unfamiliar_category_sorts_after_the_known_ones_and_is_shown():
    rows = box.panel_rows({'zzNew': '1', 'totalYards': '417'}, {})
    assert [r['category'] for r in rows] == ['totalYards', 'zzNew']


def test_a_category_only_one_side_reported_still_appears():
    """The two teams of one game can disagree on which categories exist, and
    blanking the row for both would lose a real measurement."""
    rows = box.panel_rows({'sacks': '3'}, {})
    assert len(rows) == 1 and rows[0]['away'] == '3' and rows[0]['home'] == ''


def test_a_category_neither_side_reported_is_not_a_row_of_dashes():
    assert box.panel_rows({'sacks': ''}, {'sacks': ''}) == []


def test_what_the_scoreboard_already_says_is_not_repeated():
    assert box.panel_rows({'passingTDs': '3', 'firstDowns': '24'}, {}) \
        == box.panel_rows({'firstDowns': '24'}, {})


# ---- the export --------------------------------------------------------------

def _export_db():
    conn = sqlite3.connect(':memory:')
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                            home_team_id INTEGER, away_team_id INTEGER);
        INSERT INTO games VALUES (500, 2024, 1, 2);
    """)
    conn.executescript(load.SCHEMA.read_text(encoding='utf-8'))
    conn.executemany(
        'INSERT INTO game_team_stats VALUES (?,?,?,?,?)',
        [(500, 1, 2024, 'firstDowns', '24'), (500, 2, 2024, 'firstDowns', '19')])
    return conn


def test_the_export_puts_each_team_on_its_own_side():
    """Showing one team's totals under the other team's name is the one
    failure here a reader could not detect."""
    import export_static_data as esd
    payload = esd.build_team_box(_export_db())
    assert payload[2024]['500'] == {'home': {'firstDowns': '24'},
                                    'away': {'firstDowns': '19'}}


def test_a_team_in_neither_side_of_its_game_is_left_out_not_guessed():
    import export_static_data as esd
    conn = _export_db()
    conn.execute("INSERT INTO game_team_stats VALUES (500, 7, 2024, 'sacks', '3')")
    payload = esd.build_team_box(conn)
    assert set(payload[2024]['500']) == {'home', 'away'}


def test_the_export_is_empty_when_the_archive_was_never_loaded():
    """A checkout with no data/raw/game_team_stats_*.csv must run the whole
    export unchanged and simply not draw the section."""
    import export_static_data as esd
    conn = sqlite3.connect(':memory:')
    conn.executescript('CREATE TABLE games (game_id INTEGER, home_team_id INTEGER, '
                       'away_team_id INTEGER, season_year INTEGER);')
    assert esd.build_team_box(conn) == {}


def test_a_shard_is_written_where_the_box_scores_and_drives_land(tmp_path):
    """Sharing the rule is the point: a game's three requests are predictable
    and a change to one rule cannot leave another asking for a file nobody
    wrote."""
    import export_static_data as esd
    entry = esd.write_team_box_shards(
        tmp_path, 2024, {'500': {'home': {'firstDowns': '24'}}})
    shard = esd.box_score_shard(500)
    assert list(entry['files']) == [str(shard)]
    assert (tmp_path / '2024' / f'{shard}.js').exists()
    assert entry['shards'] == esd.BOX_SCORE_SHARDS


def test_a_shard_merges_rather_than_replacing_what_is_already_loaded(tmp_path):
    """Two shards of one season load into the same global, and an assignment
    would leave only whichever arrived last."""
    import export_static_data as esd
    esd.write_team_box_shards(tmp_path, 2024, {'500': {'home': {'firstDowns': '24'}}})
    shard = esd.box_score_shard(500)
    written = (tmp_path / '2024' / f'{shard}.js').read_text(encoding='utf-8')
    assert 'Object.assign' in written and '__CFB_TEAM_BOX__' in written
