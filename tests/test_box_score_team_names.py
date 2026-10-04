"""Six FBS teams whose player lines could not link, in any season.

The box-score archive carries CFBD's own team names. A game page looks a team up
by the canonical name this database uses, so for the six teams where those
differ -- UL Monroe/ULM, San José State/San Jose State, App State/Appalachian
State, Florida Atlantic/FAU, Florida International/FIU and plain Miami/Miami
(FL) -- the lookup missed, the roster was never found, and not one player line
on that side of any game linked to a person. Measured on the exports before the
fix: 6 of 128 team names in 2015, and on Georgia-UL Monroe 2015, 79 of 159 rows
were left as plain text with every ULM player unlinked.

The project already knew all six: they are rows in `team_aliases`. Nothing was
consulting them in this exporter.
"""
from __future__ import annotations

import sqlite3

import pytest

from export_static_data import canonical_box_score_teams, team_alias_map


@pytest.fixture
def conn():
    db = sqlite3.connect(':memory:')
    db.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE team_aliases (alias TEXT, team_name TEXT);
        INSERT INTO teams VALUES (1, 'ULM'), (2, 'Miami (FL)'), (3, 'Miami (OH)'),
                                 (4, 'Georgia');
        INSERT INTO team_aliases VALUES ('UL Monroe', 'ULM'), ('Miami', 'Miami (FL)');
    """)
    return db


def test_the_feeds_name_maps_to_this_databases_name(conn):
    assert team_alias_map(conn) == {'UL Monroe': 'ULM', 'Miami': 'Miami (FL)'}


def test_a_real_teams_name_is_never_renamed_by_an_alias(conn):
    """'Miami (OH)' already matches a team, so an alias must not capture it."""
    conn.execute("INSERT INTO team_aliases VALUES ('Miami (OH)', 'Miami (FL)')")
    assert 'Miami (OH)' not in team_alias_map(conn)
    archive = {'1': [{'name': 'Miami (OH)', 'categories': []}]}
    assert canonical_box_score_teams(archive, team_alias_map(conn)) == archive


def test_an_alias_pointing_at_no_team_is_dropped(conn):
    conn.execute("INSERT INTO team_aliases VALUES ('Idaho Vandals', 'Idaho')")
    assert 'Idaho Vandals' not in team_alias_map(conn)


def test_each_side_of_a_game_is_renamed_and_the_rest_is_untouched(conn):
    archive = {
        '400603831': [
            {'name': 'Georgia', 'home_away': 'home',
             'categories': [{'name': 'passing', 'lines': [{'name': 'Greyson Lambert', 'id': '530521'}]}]},
            {'name': 'UL Monroe', 'home_away': 'away',
             'categories': [{'name': 'passing', 'lines': [{'name': 'Garrett Smith', 'id': '547000'}]}]},
        ],
    }
    out = canonical_box_score_teams(archive, team_alias_map(conn))
    assert [side['name'] for side in out['400603831']] == ['Georgia', 'ULM']
    # The lines, their ids and every other key survive the rename.
    assert out['400603831'][1]['categories'] == archive['400603831'][1]['categories']
    assert out['400603831'][1]['home_away'] == 'away'
    # The input is not mutated: the archive is read by three other callers.
    assert archive['400603831'][1]['name'] == 'UL Monroe'


def test_an_archive_with_nothing_to_rename_is_returned_as_it_is(conn):
    archive = {'1': [{'name': 'Georgia', 'categories': []}]}
    assert canonical_box_score_teams(archive, team_alias_map(conn)) == archive
    assert canonical_box_score_teams(archive, {}) is archive


def test_a_malformed_side_does_not_break_the_export(conn):
    """The archive is 23 seasons of a third-party feed; one odd row must not
    take the whole export down."""
    archive = {'1': 'not a list', '2': [{'no_name': True}, {'name': None},
                                        {'name': 'UL Monroe'}]}
    out = canonical_box_score_teams(archive, team_alias_map(conn))
    assert out['1'] == 'not a list'
    assert out['2'][2]['name'] == 'ULM'
    assert out['2'][0] == {'no_name': True}


def test_the_six_real_aliases_are_in_the_live_database(db_path):
    """The names measured in the archive, checked against the shipped database
    rather than against this test's fixture."""
    db = sqlite3.connect(str(db_path))
    mapping = team_alias_map(db)
    for feed_name, canonical in [('UL Monroe', 'ULM'), ('San José State', 'San Jose State'),
                                 ('App State', 'Appalachian State'), ('Florida Atlantic', 'FAU'),
                                 ('Florida International', 'FIU'), ('Miami', 'Miami (FL)')]:
        assert mapping.get(feed_name) == canonical, feed_name
