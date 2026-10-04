"""The 74 people CFBD recorded under two athlete ids, and what links them.

Both ids resolve to a page here, so each page holds part of one career and
neither looks short. The verdicts in data/identity/split_careers.csv were
reached by reading both records: hometown is the field that discriminates,
because position label, jersey number and height all disagree across a pair for
the same person. These tests pin the two rules that keep the link honest -- a
pair is linked only on a verdict that says one person, and never to a page this
database does not hold.
"""
from __future__ import annotations

import csv
import sqlite3

import pytest

import person_identity as identity


@pytest.fixture
def conn():
    db = sqlite3.connect(':memory:')
    db.executescript("""
        CREATE TABLE players (player_id INTEGER PRIMARY KEY, display_name TEXT);
        INSERT INTO players VALUES (1, 'Sherod White'), (2, 'Sherod White'),
                                   (3, 'Brett Johnson'), (4, 'Brett Johnson'),
                                   (5, 'L.A. Ramsby');
    """)
    return db


def write(tmp_path, rows):
    path = tmp_path / 'split_careers.csv'
    with path.open('w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['season', 'name', 'id_a', 'id_b', 'verdict'])
        writer.writerows(rows)
    return path


def test_the_link_is_symmetric_and_rewrites_nothing(conn, tmp_path):
    """Neither page is the real one, so each names the other."""
    path = write(tmp_path, [[2022, 'Sherod White', 1, 2, 'one person']])
    links = identity.linked_careers(conn, path)
    assert links[1] == {'id': 2, 'name': 'Sherod White', 'season': 2022}
    assert links[2] == {'id': 1, 'name': 'Sherod White', 'season': 2022}


def test_two_people_who_share_a_name_are_never_linked(conn, tmp_path):
    """Brett Johnson, California 2023: a DB #25 and a DL #90, four inches and a
    city apart. Linking them would put one player's work under the other's name."""
    path = write(tmp_path, [[2023, 'Brett Johnson', 3, 4, 'TWO PEOPLE']])
    assert identity.linked_careers(conn, path) == {}


def test_a_hometown_contradiction_waits_for_a_human(conn, tmp_path):
    path = write(tmp_path, [[2014, 'L.A. Ramsby', 5, 1, 'needs a human']])
    assert identity.linked_careers(conn, path) == {}


def test_a_link_to_a_page_this_database_lacks_is_dropped(conn, tmp_path):
    """The same defect the box-score exporter was fixed for: a link that lands
    on 'player not found' is worse than no link."""
    path = write(tmp_path, [[2022, 'Nobody Here', 1, 999, 'one person']])
    assert identity.linked_careers(conn, path) == {}


def test_an_id_paired_with_itself_is_dropped(conn, tmp_path):
    path = write(tmp_path, [[2022, 'Sherod White', 1, 1, 'one person']])
    assert identity.linked_careers(conn, path) == {}


def test_a_missing_file_links_nothing_rather_than_failing(conn, tmp_path):
    assert identity.load_split_careers(tmp_path / 'absent.csv') == []
    assert identity.linked_careers(conn, tmp_path / 'absent.csv') == {}


def test_the_shipped_file_holds_all_74_cases_and_their_verdicts():
    cases = identity.load_split_careers()
    assert len(cases) == 74
    counts = {}
    for case in cases:
        counts[case['verdict']] = counts.get(case['verdict'], 0) + 1
        assert len(case['ids']) == 2 and case['ids'][0] != case['ids'][1], case
    assert counts == {'one person': 62, 'needs a human': 9,
                      'likely one person': 2, 'TWO PEOPLE': 1}


def test_every_linked_pair_resolves_in_the_live_database(db_path):
    """The file names people; this asserts the pages exist to link together.

    Skipped where the person tables are absent, which is the CI test job: that
    database is built by run_pipeline.py, and people arrive separately from
    sync-people.yml. The verdict file itself is checked above without a
    database, so a wrong row still fails there rather than only on a machine
    that happens to have the rosters loaded.
    """
    db = sqlite3.connect(str(db_path))
    if not db.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                      "AND name = 'players'").fetchone():
        pytest.skip('this database has no person tables -- run sync_people first')
    links = identity.linked_careers(db)
    assert len(links) == 2 * 64, 'the 62 one-person and 2 likely pairs, both ways'
    held = {int(row[0]) for row in db.execute('SELECT player_id FROM players')}
    for player_id, link in links.items():
        assert player_id in held and link['id'] in held
        assert links[link['id']]['id'] == player_id, 'the link must be symmetric'


def test_the_exporter_drops_a_link_whose_counterpart_has_no_page(tmp_path, monkeypatch):
    """A person with no season row is not exported, so a link to them would land
    a reader on 'player not found'."""
    import export_people_pages as exporter

    db = sqlite3.connect(':memory:')
    db.executescript("""
        CREATE TABLE players (player_id INTEGER PRIMARY KEY, display_name TEXT,
            first_name TEXT, last_name TEXT, primary_position TEXT, hometown TEXT,
            home_state TEXT, height INTEGER, weight INTEGER, headshot_path TEXT,
            headshot_url TEXT, latest_season INTEGER);
        CREATE TABLE player_team_seasons (player_id INTEGER, season_year INTEGER,
            team_id INTEGER, jersey INTEGER, position TEXT, class_year INTEGER,
            height INTEGER, weight INTEGER, source TEXT);
        CREATE TABLE player_season_stats (player_id INTEGER, season_year INTEGER,
            team_id INTEGER, category TEXT, stat_type TEXT, stat REAL);
        CREATE TABLE player_season_stat_caveats (player_id INTEGER, season_year INTEGER,
            reason TEXT, source TEXT);
        INSERT INTO players (player_id, display_name) VALUES (1, 'Split Career'),
                                                            (2, 'No Roster Row');
        INSERT INTO player_team_seasons (player_id, season_year, team_id, source)
            VALUES (1, 2022, 7, 'cfbd');
    """)
    path = write(tmp_path, [[2022, 'Split Career', 1, 2, 'one person']])
    monkeypatch.setattr(identity, 'SPLIT_CAREERS', path)
    players = exporter.build_players(db)
    assert set(players) == {1}, 'the person with no season row is not displayable'
    assert 'same_person' not in players[1], 'and must not be linked to'
