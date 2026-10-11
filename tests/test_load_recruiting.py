"""The recruiting loader: the rules about what is NOT in this database.

A quarter of CFBD's recruiting classes belong to schools this project does not
carry, and a third of recruits never commit to one. The loader's job is to
keep those rows anyway, with the provider's own name beside them, rather than
drop them the way this repository has dropped things before. Most of this file
pins that.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import load_recruiting as lr  # noqa: E402

TEAM_CSV = ('season_year,team,rank,points\n')
PLAYER_CSV = ('season_year,recruit_id,athlete_id,recruit_type,name,school,'
              'committed_to,position,height,weight,stars,rating,national_rank,'
              'city,state_province,country,hometown_latitude,hometown_longitude,'
              'hometown_county_fips\n')


@pytest.fixture
def db(tmp_path):
    conn = sqlite3.connect(tmp_path / 'league.db')
    conn.execute('CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT)')
    conn.execute('CREATE TABLE team_aliases (alias TEXT, team_name TEXT)')
    conn.execute("INSERT INTO teams VALUES (1, 'Georgia')")
    conn.execute("INSERT INTO teams VALUES (2, 'Alabama')")
    conn.commit()
    conn.executescript(lr.SCHEMA.read_text(encoding='utf-8'))
    return conn


def write(tmp_path, name, header, *rows):
    path = tmp_path / name
    path.write_text(header + ''.join(r + '\n' for r in rows), encoding='utf-8')
    return tmp_path


def recruit_line(**over):
    fields = {'season_year': 2024, 'recruit_id': 'r1', 'athlete_id': 4567,
              'recruit_type': 'HighSchool', 'name': 'A Player',
              'school': 'Central High', 'committed_to': 'Georgia', 'position': 'DL',
              'height': 76, 'weight': 280, 'stars': 5, 'rating': 0.99,
              'national_rank': 3, 'city': 'Macon', 'state_province': 'GA',
              'country': 'USA', 'hometown_latitude': 32.84,
              'hometown_longitude': -83.63, 'hometown_county_fips': '13021'}
    fields.update(over)
    order = PLAYER_CSV.strip().split(',')
    return ','.join(str(fields[k]) for k in order)


# ---- classes -----------------------------------------------------------------

def test_a_school_this_database_carries_gets_a_team_id(db, tmp_path):
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2024,Georgia,1,310.5')
    assert lr.load_classes(db, tmp_path) == (1, 0)
    assert db.execute('SELECT team_id, class_rank, points FROM recruiting_classes'
                      ).fetchone() == (1, 1, 310.5)


def test_a_school_it_does_not_carry_is_kept_with_no_team_id(db, tmp_path):
    """Harvard and Eastern Washington sign classes too. Dropping them would be
    indistinguishable afterwards from a school that signed nobody."""
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV,
          '2024,Georgia,1,310.5', '2024,Eastern Washington,88,41.2')
    assert lr.load_classes(db, tmp_path) == (2, 1)
    row = db.execute("SELECT source_team, team_id, class_rank FROM recruiting_classes "
                     "WHERE source_team = 'Eastern Washington'").fetchone()
    assert row == ('Eastern Washington', None, 88)


def test_a_blank_metric_becomes_null_and_never_zero(db, tmp_path):
    """A class with 0 points is a real and occasionally true row."""
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2024,Georgia,,')
    lr.load_classes(db, tmp_path)
    assert db.execute('SELECT class_rank, points FROM recruiting_classes'
                      ).fetchone() == (None, None)


def test_a_row_disagreeing_with_its_file_name_fails(db, tmp_path):
    """The file name decides the season. A row that says otherwise would
    attribute a class to the wrong coach, silently."""
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2023,Georgia,1,310.5')
    with pytest.raises(ValueError, match='2023'):
        lr.load_classes(db, tmp_path)


def test_reloading_the_same_archive_updates_rather_than_duplicates(db, tmp_path):
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2024,Georgia,4,280.0')
    lr.load_classes(db, tmp_path)
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2024,Georgia,1,310.5')
    lr.load_classes(db, tmp_path)
    assert db.execute('SELECT COUNT(*) FROM recruiting_classes').fetchone()[0] == 1
    assert db.execute('SELECT class_rank FROM recruiting_classes').fetchone()[0] == 1


def test_each_season_is_loaded_from_its_own_file(db, tmp_path):
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2024,Georgia,1,310.5')
    write(tmp_path, 'recruiting_teams_2025.csv', TEAM_CSV, '2025,Georgia,2,300.0')
    assert lr.load_classes(db, tmp_path) == (2, 0)
    assert db.execute('SELECT COUNT(DISTINCT season_year) FROM recruiting_classes'
                      ).fetchone()[0] == 2


# ---- recruits ----------------------------------------------------------------

def test_a_recruit_is_linked_to_the_team_he_committed_to(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV, recruit_line())
    assert lr.load_recruits(db, tmp_path) == (1, 0)
    assert db.execute('SELECT committed_team_id, committed_to, name FROM recruits'
                      ).fetchone() == (1, 'Georgia', 'A Player')


def test_an_uncommitted_recruit_is_kept(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV,
          recruit_line(committed_to=''))
    assert lr.load_recruits(db, tmp_path) == (1, 1)
    assert db.execute('SELECT committed_team_id, committed_to FROM recruits'
                      ).fetchone() == (None, None)


def test_a_recruit_committed_outside_this_database_keeps_the_school_name(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV,
          recruit_line(committed_to='Yale'))
    lr.load_recruits(db, tmp_path)
    assert db.execute('SELECT committed_team_id, committed_to FROM recruits'
                      ).fetchone() == (None, 'Yale')


def test_the_hometown_is_stored_as_numbers_a_map_can_use(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV, recruit_line())
    lr.load_recruits(db, tmp_path)
    lat, lon, fips = db.execute(
        'SELECT hometown_latitude, hometown_longitude, hometown_county_fips '
        'FROM recruits').fetchone()
    assert (lat, lon) == (32.84, -83.63) and fips == '13021'


def test_a_recruit_with_no_coordinates_is_still_loaded(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV,
          recruit_line(hometown_latitude='', hometown_longitude='',
                       hometown_county_fips=''))
    assert lr.load_recruits(db, tmp_path) == (1, 0)
    assert db.execute('SELECT hometown_latitude FROM recruits').fetchone()[0] is None


def test_unrated_is_null_and_never_zero_stars(db, tmp_path):
    """0 stars would read as "rated and rated badly"."""
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV,
          recruit_line(stars='', rating='', athlete_id=''))
    lr.load_recruits(db, tmp_path)
    assert db.execute('SELECT stars, rating, athlete_id FROM recruits'
                      ).fetchone() == (None, None, None)


def test_a_recruit_with_no_id_fails(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV, recruit_line(recruit_id=''))
    with pytest.raises(ValueError, match='no id'):
        lr.load_recruits(db, tmp_path)


def test_reloading_updates_a_recruit_rather_than_duplicating_him(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV, recruit_line(stars=4))
    lr.load_recruits(db, tmp_path)
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV, recruit_line(stars=5))
    lr.load_recruits(db, tmp_path)
    assert db.execute('SELECT COUNT(*) FROM recruits').fetchone()[0] == 1
    assert db.execute('SELECT stars FROM recruits').fetchone()[0] == 5


def test_a_recruit_row_disagreeing_with_its_file_name_fails(db, tmp_path):
    write(tmp_path, 'recruiting_players_2024.csv', PLAYER_CSV,
          recruit_line(season_year=2023))
    with pytest.raises(ValueError, match='2023'):
        lr.load_recruits(db, tmp_path)


# ---- the join this exists for ------------------------------------------------

def test_a_class_can_be_joined_to_the_coach_who_signed_it(db, tmp_path):
    """The whole point: a coach's season and his class meet on team and year."""
    db.execute('CREATE TABLE coach_tenures (coach_id INTEGER, team_id INTEGER, '
               'season_year INTEGER)')
    db.execute('INSERT INTO coach_tenures VALUES (9, 1, 2024)')
    write(tmp_path, 'recruiting_teams_2024.csv', TEAM_CSV, '2024,Georgia,1,310.5')
    lr.load_classes(db, tmp_path)
    assert db.execute(
        'SELECT rc.class_rank FROM coach_tenures t JOIN recruiting_classes rc '
        'ON rc.team_id = t.team_id AND rc.season_year = t.season_year'
    ).fetchone() == (1,)
