"""
P1-08 (data-coverage indicators): src/coverage.py counts what each season
actually has, so the site can say why a metric is absent instead of drawing the
absence as a zero.

The guide asks for three fixtures specifically -- null vs. zero, partial
coverage, and a historical sparse season -- and they are the first three tests
here.
"""
import sqlite3

import pytest

from coverage import DIMENSIONS, build_coverage

KEYS = [d["key"] for d in DIMENSIONS]


def _db(tmp_path, *, games, advanced=(), game_advanced=(), elo=(), hybrid=(), kickoffs=()):
    conn = sqlite3.connect(str(tmp_path / "cov.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        CREATE TABLE team_season_advanced (team_id INTEGER, season_year INTEGER, off_success_rate REAL);
        CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER, season_year INTEGER,
            off_success_rate REAL);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER);
        CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER);
        CREATE TABLE game_kickoffs (game_id INTEGER, kickoff_utc TEXT, time_tbd INTEGER, date_only INTEGER);
    """)
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?,?)", games)
    conn.executemany("INSERT INTO team_season_advanced VALUES (?,?,?)", advanced)
    conn.executemany("INSERT INTO game_team_advanced VALUES (?,?,?,?)", game_advanced)
    conn.executemany("INSERT INTO elo_game_history VALUES (?,?)", elo)
    conn.executemany("INSERT INTO hybrid_game_ratings VALUES (?,?)", hybrid)
    conn.executemany("INSERT INTO game_kickoffs VALUES (?,?,?,?)", kickoffs)
    conn.commit()
    return conn


# ---------------------------------------------- the three fixtures the guide names

def test_null_is_reported_as_absent_and_never_as_zero_coverage(tmp_path):
    """A season with no advanced stats must report [0, n] -- an explicit "none of
    n", which the UI turns into a reason. It must never look like a real value."""
    conn = _db(tmp_path, games=[(1, 1994, 10, 20, 24, 17)],
               advanced=[(10, 1994, None)])          # a row exists, but the metric is NULL
    cov = build_coverage(conn)
    # The row counts as coverage (the team-season was collected); the NULL metric
    # itself is the UI's N/A, which is a different question from coverage.
    assert cov["seasons"]["1994"]["team_advanced"] == [1, 2]
    assert cov["seasons"]["1994"]["game_advanced"] == [0, 1]
    assert cov["first"]["game_advanced"] is None, "nothing collected means None, not 0"
    conn.close()


def test_partial_coverage_is_visible_as_a_fraction(tmp_path):
    conn = _db(tmp_path,
               games=[(1, 2002, 10, 20, 24, 17), (2, 2002, 30, 40, 14, 21)],
               advanced=[(10, 2002, 0.45), (20, 2002, 0.41)])   # 2 of the 4 teams that played
    cov = build_coverage(conn)
    have, of = cov["seasons"]["2002"]["team_advanced"]
    assert (have, of) == (2, 4)
    assert 0 < have < of, "a partial season must be distinguishable from full and from none"
    conn.close()


def test_a_historically_sparse_season_reports_every_dimension(tmp_path):
    """1980 has results and Elo but nothing else; each dimension still gets a
    counted answer rather than being missing from the payload."""
    conn = _db(tmp_path, games=[(1, 1980, 10, 20, 24, 17)],
               elo=[(1, 10), (1, 20)],
               kickoffs=[(1, "1980-09-06T00:00:00Z", 1, 1)])     # date-only: not a real time
    cov = build_coverage(conn)
    row = cov["seasons"]["1980"]
    assert set(row) == set(KEYS), "every dimension must be answered for every season"
    assert row["scores"] == [1, 1]
    assert row["elo"] == [1, 1]
    assert row["kickoff"] == [0, 1], "a date-only stamp is not kickoff coverage"
    assert row["team_advanced"] == [0, 2]
    assert row["coe2"] == [0, 1]
    conn.close()


# ------------------------------------------------------------------ the rest

def test_scheduled_games_count_against_scores_but_not_against_ratings(tmp_path):
    conn = _db(tmp_path,
               games=[(1, 2026, 10, 20, 24, 17), (2, 2026, 30, 40, None, None)],
               elo=[(1, 10), (1, 20)])
    row = build_coverage(conn)["seasons"]["2026"]
    assert row["scores"] == [1, 2], "one of two games has a result"
    # Ratings are measured against COMPLETED games: a scheduled game has no rating
    # and must not make Elo look incomplete.
    assert row["elo"] == [1, 1]
    conn.close()


def test_first_season_reports_where_each_dimension_actually_starts(tmp_path):
    conn = _db(tmp_path,
               games=[(1, 1999, 10, 20, 7, 3), (2, 2001, 10, 20, 24, 17)],
               advanced=[(10, 2001, 0.45), (20, 2001, 0.44)],
               kickoffs=[(1, "1999-09-04T00:00:00Z", 1, 1), (2, "2001-09-01T19:00:00Z", 0, 0)])
    cov = build_coverage(conn)
    assert cov["first"]["scores"] == 1999
    assert cov["first"]["team_advanced"] == 2001
    assert cov["first"]["kickoff"] == 2001, "the date-only 1999 stamp must not count"
    conn.close()


def test_missing_optional_tables_do_not_break_the_report(tmp_path):
    """A database built before a feature existed must still produce a full report
    -- every dimension answered as "none", never a crash or a missing key."""
    conn = sqlite3.connect(str(tmp_path / "bare.db"))
    conn.execute("""CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
        home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER)""")
    conn.execute("INSERT INTO games VALUES (1, 1990, 10, 20, 21, 14)")
    conn.commit()
    cov = build_coverage(conn)
    row = cov["seasons"]["1990"]
    assert set(row) == set(KEYS)
    assert row["scores"] == [1, 1]
    assert all(row[k][0] == 0 for k in ("kickoff", "team_advanced", "game_advanced", "elo", "coe2"))
    conn.close()


def test_every_dimension_documents_its_source_and_its_absence(tmp_path):
    for d in DIMENSIONS:
        assert d["label"] and d["source"], d["key"]
        assert d["absent"].endswith("."), f"{d['key']}: the absence reason is shown as a sentence"


def test_source_boundaries_are_read_from_the_fetchers_not_restated(repo_root):
    """If a fetcher changes its coverage window, this must follow rather than keep
    claiming the old one."""
    from coverage import _first_season
    assert _first_season("fetch_cfbd_advanced") == 2001
    assert _first_season("fetch_cfbd_game_advanced") == 2001
    assert _first_season("does_not_exist") is None
    text = (repo_root / "src" / "fetch_cfbd_advanced.py").read_text(encoding="utf-8")
    assert "FIRST_SEASON = 2001" in text, "the constant this reads must stay a simple assignment"


# ---------------------------------------------- invariants on the real database

def test_real_coverage_matches_the_database_it_describes(db_conn):
    cov = build_coverage(db_conn)
    seasons = {s for (s,) in db_conn.execute("SELECT DISTINCT season_year FROM games")}
    assert {int(s) for s in cov["seasons"]} == seasons
    for season, row in cov["seasons"].items():
        for key in KEYS:
            have, of = row[key]
            assert 0 <= have <= of, f"{season} {key}: {have} of {of}"
    completed = dict(db_conn.execute(
        "SELECT season_year, COUNT(*) FROM games WHERE home_score IS NOT NULL GROUP BY 1"))
    for season, n in completed.items():
        assert cov["seasons"][str(season)]["scores"][0] == n


def test_a_stats_row_for_a_team_that_played_no_games_is_not_counted(tmp_path):
    """CFBD can carry a team-season row for a programme whose season this dataset
    has no games for -- New Mexico State's cancelled 2020 is the real case. Counting
    it produced "128 of 127", which is not a sentence anyone can act on."""
    conn = _db(tmp_path,
               games=[(1, 2020, 10, 20, 24, 17)],
               advanced=[(10, 2020, 0.45), (20, 2020, 0.42), (99, 2020, 0.40)])
    have, of = build_coverage(conn)["seasons"]["2020"]["team_advanced"]
    assert (have, of) == (2, 2), "the team with no games must count for neither side"
    assert have <= of
    conn.close()


def test_real_coverage_confirms_the_documented_era_boundaries(db_conn):
    """The claims the methodology page makes, checked against the data."""
    cov = build_coverage(db_conn)
    first = cov["first"]
    assert first["scores"] == 1980
    assert first["elo"] == 1980
    assert first["coe2"] == 1985, "CoE 2.0 needs five prior seasons"
    assert first["kickoff"] == 2001
    assert first["team_advanced"] == 2001
    # Per-game efficiency is the EXP-03 data gap: implemented, not yet collected.
    assert first["game_advanced"] is None


def test_the_manifest_carries_coverage_for_the_dashboard(repo_root):
    import json
    manifest = json.loads((repo_root / "ui" / "data" / "static_manifest.json").read_text(encoding="utf-8"))
    cov = manifest.get("coverage")
    assert cov, "the dashboard reads coverage from the embedded manifest"
    assert [d["key"] for d in cov["dimensions"]] == KEYS
    assert len(cov["seasons"]) == len({s["season"] for s in manifest["seasons"]})
