"""A coach's season against the expectation the ratings already held.

The measure is simple -- sum the pregame Elo win probability of each game, and
compare it with what the team actually did -- so these tests are about the three
ways it can be wrong: attributing games to a coach who did not coach them,
ordering a season wrongly, and counting a game on one side of the comparison but
not the other.

The ordering test is a regression. CFBD numbers some postseason games week 1, so
ordering a season by week put a bowl game at the FRONT: 1,353 of 5,287 seasons
then reported an Elo change of exactly zero, because the first row's pregame
rating was the rating the season had ENDED on.
"""
from __future__ import annotations

import sqlite3

import pytest

import build_coach_metrics as metrics


@pytest.fixture
def conn():
    db = sqlite3.connect(':memory:')
    db.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE coaches (coach_id INTEGER PRIMARY KEY, display_name TEXT);
        CREATE TABLE coach_tenures (coach_id INTEGER, team_id INTEGER,
            season_year INTEGER, role TEXT, hire_date TEXT, games INTEGER,
            wins INTEGER, losses INTEGER, ties INTEGER, preseason_rank INTEGER,
            postseason_rank INTEGER, source TEXT, source_team TEXT);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
            week INTEGER, game_date TEXT, home_team_id INTEGER, away_team_id INTEGER,
            home_score INTEGER, away_score INTEGER);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER,
            pregame_elo REAL, opponent_pregame_elo REAL, elo_expectation REAL,
            elo_change REAL, postgame_elo REAL);
        CREATE TABLE team_coe2_by_season (team_id INTEGER, season_year INTEGER,
            season_coe2 REAL);
        INSERT INTO teams VALUES (1, 'Our Team'), (2, 'Them');
        INSERT INTO coaches VALUES (10, 'The Coach'), (11, 'The Interim');
    """)
    return db


def tenure(db, coach_id, season, team_id=1):
    db.execute("INSERT INTO coach_tenures (coach_id, team_id, season_year, role) "
               "VALUES (?, ?, ?, 'head coach')", (coach_id, team_id, season))


def game(db, game_id, season, week, date, *, won, expectation, pregame, postgame,
         team_id=1, tie=False):
    ours, theirs = (24, 17) if won else (17, 24)
    if tie:
        ours = theirs = 20
    db.execute("INSERT INTO games (game_id, season_year, week, game_date, home_team_id, "
               "away_team_id, home_score, away_score) VALUES (?, ?, ?, ?, ?, 2, ?, ?)",
               (game_id, season, week, date, team_id, ours, theirs))
    db.execute("INSERT INTO elo_game_history (game_id, team_id, pregame_elo, "
               "elo_expectation, elo_change, postgame_elo) VALUES (?, ?, ?, ?, ?, ?)",
               (game_id, team_id, pregame, expectation, postgame - pregame, postgame))


def row(db, coach_id=10, season=2020):
    db.row_factory = sqlite3.Row
    return db.execute("SELECT * FROM coach_season_metrics WHERE coach_id = ? "
                      "AND season_year = ?", (coach_id, season)).fetchone()


def test_expected_wins_is_the_sum_of_the_pregame_probabilities(conn):
    tenure(conn, 10, 2020)
    game(conn, 1, 2020, 1, '2020-09-05', won=True, expectation=0.9,
         pregame=1600, postgame=1610)
    game(conn, 2, 2020, 2, '2020-09-12', won=False, expectation=0.4,
         pregame=1610, postgame=1580)
    metrics.build(conn)

    r = row(conn)
    assert r['attributed'] == 1 and r['reason'] is None
    assert r['games'] == 2 and r['wins'] == 1 and r['losses'] == 1
    assert r['expected_wins'] == pytest.approx(1.3)
    # One win where 1.3 were expected is a season slightly below the rating's
    # own forecast, which is the whole point of the number.
    assert r['wins_above_expected'] == pytest.approx(-0.3)


def test_a_tie_is_half_a_win_on_both_sides_of_the_comparison(conn):
    tenure(conn, 10, 1985)
    game(conn, 1, 1985, 1, '1985-09-07', won=False, tie=True, expectation=0.5,
         pregame=1500, postgame=1500)
    metrics.build(conn)

    r = row(conn, season=1985)
    assert (r['wins'], r['losses'], r['ties']) == (0, 0, 1)
    assert r['wins_above_expected'] == pytest.approx(0.0)


def test_a_season_shared_with_another_head_coach_is_not_measured(conn):
    """A firing mid-season. CFBD gives each coach a season-long record and never
    says which games were whose, so neither row gets a number."""
    tenure(conn, 10, 2020)
    tenure(conn, 11, 2020)
    game(conn, 1, 2020, 1, '2020-09-05', won=True, expectation=0.3,
         pregame=1500, postgame=1540)
    metrics.build(conn)

    for coach_id in (10, 11):
        r = row(conn, coach_id)
        assert r['attributed'] == 0, coach_id
        assert r['wins_above_expected'] is None and r['expected_wins'] is None
        assert 'does not say which games' in r['reason']


def test_a_tenure_with_no_games_here_says_so_rather_than_reading_as_zero(conn):
    tenure(conn, 10, 1912)
    metrics.build(conn)

    r = row(conn, season=1912)
    assert r['attributed'] == 0 and r['games'] is None
    assert r['reason'] == metrics.NO_GAMES


def test_the_season_is_ordered_by_date_not_by_week(conn):
    """The regression. A bowl game CFBD numbers week 1 must not start a season:
    ordering by week made the first row's pregame rating the season's FINAL
    rating, and the Elo change came out exactly zero.
    """
    tenure(conn, 10, 1983)
    # Played last, numbered week 1.
    game(conn, 99, 1983, 1, '1983-12-24', won=True, expectation=0.5,
         pregame=1662, postgame=1761)
    game(conn, 1, 1983, 3, '1983-09-10', won=True, expectation=0.5,
         pregame=1648, postgame=1665)
    game(conn, 2, 1983, 4, '1983-09-17', won=False, expectation=0.5,
         pregame=1665, postgame=1662)
    metrics.build(conn)

    r = row(conn, season=1983)
    assert r['pregame_elo'] == 1648, 'the season starts with the first game PLAYED'
    assert r['postgame_elo'] == 1761, 'and ends with the last one played'
    assert r['elo_change'] == pytest.approx(113)
    # The stored chain is the authority on the order: every game's change must
    # add up to the season's.
    chain = conn.execute("SELECT SUM(elo_change) FROM elo_game_history e "
                         "JOIN games g USING (game_id) WHERE g.season_year = 1983"
                         ).fetchone()[0]
    assert r['elo_change'] == pytest.approx(chain)


def test_a_game_with_no_recorded_expectation_counts_on_neither_side(conn):
    """Dropping it from the expectation alone would hand the coach a free win
    above expectation."""
    tenure(conn, 10, 2020)
    game(conn, 1, 2020, 1, '2020-09-05', won=True, expectation=0.5,
         pregame=1500, postgame=1520)
    game(conn, 2, 2020, 2, '2020-09-12', won=True, expectation=None,
         pregame=1520, postgame=1540)
    metrics.build(conn)

    r = row(conn)
    assert r['games'] == 2, 'the game is still part of the record'
    assert r['expected_wins'] == pytest.approx(0.5)
    assert r['wins_above_expected'] == pytest.approx(0.5), \
        'one rated win against half a rated expectation, not two wins'


def test_an_unplayed_game_is_not_a_loss(conn):
    tenure(conn, 10, 2026)
    game(conn, 1, 2026, 1, '2026-09-05', won=True, expectation=0.6,
         pregame=1500, postgame=1520)
    conn.execute("INSERT INTO games (game_id, season_year, week, game_date, "
                 "home_team_id, away_team_id) VALUES (2, 2026, 2, '2026-09-12', 1, 2)")
    conn.execute("INSERT INTO elo_game_history (game_id, team_id, pregame_elo, "
                 "elo_expectation, elo_change, postgame_elo) VALUES (2, 1, 1520, 0.6, 0, 1520)")
    metrics.build(conn)

    r = row(conn, season=2026)
    assert r['games'] == 1 and r['wins'] == 1
    assert r['expected_wins'] == pytest.approx(0.6)


def test_rebuilding_replaces_rather_than_doubles(conn):
    tenure(conn, 10, 2020)
    game(conn, 1, 2020, 1, '2020-09-05', won=True, expectation=0.5,
         pregame=1500, postgame=1520)
    metrics.build(conn)
    metrics.build(conn)
    assert conn.execute("SELECT COUNT(*) FROM coach_season_metrics").fetchone()[0] == 1


def test_every_tenure_in_the_live_database_has_a_row_and_a_verdict(db_conn):
    """Measured against the shipped database rather than a fixture."""
    if not db_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                           "AND name = 'coach_season_metrics'").fetchone():
        pytest.skip('run src/build_coach_metrics.py first')
    tenures = db_conn.execute("SELECT COUNT(*) FROM coach_tenures").fetchone()[0]
    rows = db_conn.execute("SELECT COUNT(*) FROM coach_season_metrics").fetchone()[0]
    assert rows == tenures, 'every tenure gets a row, measured or not'

    unexplained = db_conn.execute(
        "SELECT COUNT(*) FROM coach_season_metrics WHERE attributed = 0 "
        "AND reason IS NULL").fetchone()[0]
    assert unexplained == 0, 'an unmeasured season must say why'

    # A probability sum cannot exceed the games it was summed over, and no
    # season can be more above expectation than it had games to win.
    impossible = db_conn.execute(
        "SELECT COUNT(*) FROM coach_season_metrics WHERE attributed = 1 AND ("
        "expected_wins > games OR expected_wins < 0 OR "
        "ABS(wins_above_expected) > games)").fetchone()[0]
    assert impossible == 0

    # The ordering bug showed up as a mass of seasons whose rating had not
    # moved at all. One or two could be real; 1,353 were not.
    frozen = db_conn.execute("SELECT COUNT(*) FROM coach_season_metrics "
                             "WHERE attributed = 1 AND elo_change = 0").fetchone()[0]
    assert frozen <= 2, f'{frozen} seasons with no rating movement at all'


def test_a_coachs_season_is_the_same_number_the_team_page_shows(repo_root, db_conn):
    """The team pages already measure a team-season against the Elo expectation.
    A coach who coached every game of that season must carry exactly that number,
    or the site states two different truths about one season.

    Compared only where the export counted the same games as this build: the
    current season's export is a different vintage, which is a difference of data
    rather than of formula.
    """
    import json

    if not db_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                           "AND name = 'coach_season_metrics'").fetchone():
        pytest.skip('run src/build_coach_metrics.py first')
    export = repo_root / "ui" / "data" / "team_pages.js"
    if not export.exists():
        pytest.skip('team pages have not been exported')
    raw = export.read_text(encoding="utf-8")
    data = json.loads(raw[raw.index("{"):raw.rindex("}") + 1])
    # [team_id, season, games, w, l, t, start, end, change, sos, sos_rank,
    #  expected, actual, above_expected, ...]
    team = {(row[0], row[1]): row for row in data["team_seasons"]}

    compared = 0
    for team_id, season, games, expected, above in db_conn.execute(
            "SELECT team_id, season_year, games, expected_wins, wins_above_expected "
            "FROM coach_season_metrics WHERE attributed = 1"):
        row = team.get((team_id, season))
        if row is None or row[2] != games:
            continue
        compared += 1
        assert round(expected, 2) == pytest.approx(row[11], abs=0.011), (team_id, season)
        assert round(above, 2) == pytest.approx(row[13], abs=0.011), (team_id, season)
    assert compared > 4000, f"only {compared} coach-seasons could be compared"
