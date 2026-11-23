"""
P1-08 (data-coverage indicators): src/coverage.py counts what each season
actually has, so the site can say why a metric is absent instead of drawing the
absence as a zero.

The guide asks for three fixtures specifically -- null vs. zero, partial
coverage, and a historical sparse season -- and they are the first three tests
here.
"""
import sqlite3

import coverage as cov_mod
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
    """
    The claims the methodology page makes, checked against the data.

    Kickoff times and advanced stats come from optional load steps that
    run_pipeline.py performs but CI's leaner test bootstrap does not, so those
    two are asserted only where the data was actually loaded. Asserting them
    unconditionally would make this test a statement about which build script
    ran rather than about the boundaries themselves.
    """
    cov = build_coverage(db_conn)
    first = cov["first"]
    # Always present: these come from the game results every build loads.
    assert first["scores"] == 1980
    assert first["elo"] == 1980
    assert first["coe2"] == 1985, "CoE 2.0 needs five prior seasons"
    # Optional loads: right boundary when present, cleanly absent when not.
    # Per-game efficiency joined this group once EXP-03's fetch was actually run. It
    # used to be asserted absent, as the one dimension implemented but uncollected;
    # that is no longer true of the project, so it is no longer asserted here. It is
    # now a boundary like the others -- checked where the data was loaded, allowed to
    # be cleanly absent in CI's leaner bootstrap.
    for key, expected in (("kickoff", 2001), ("team_advanced", 2001), ("game_advanced", 2001)):
        assert first[key] in (expected, None), f"{key} starts at {first[key]}, expected {expected} or absent"


def test_the_manifest_carries_coverage_for_the_dashboard(repo_root):
    import json
    manifest = json.loads((repo_root / "ui" / "data" / "static_manifest.json").read_text(encoding="utf-8"))
    cov = manifest.get("coverage")
    assert cov, "the dashboard reads coverage from the embedded manifest"
    # The COMMITTED manifest is regenerated by the deploy, so it lags this
    # module by one run whenever a dimension is added. Pinning equality here
    # makes adding one fail the build that would have produced the new file --
    # the deadlock this repository has hit four times. What must hold either
    # way is that the manifest names no dimension this module does not define,
    # and that every key it does name is a real one.
    keys = [d["key"] for d in cov["dimensions"]]
    assert keys, "the manifest must carry at least the dimensions it was built with"
    assert set(keys) <= set(KEYS), f"manifest names unknown dimensions: {set(keys) - set(KEYS)}"
    assert keys == [k for k in KEYS if k in set(keys)], "the manifest keeps this module's order"
    for key in keys:
        assert all(key in row for row in cov["seasons"].values()), f"{key} missing from a season"
    assert len(cov["seasons"]) == len({s["season"] for s in manifest["seasons"]})


# ------------------------------------------- the archives added 2026-11-16
#
# Six of this project's fourteen datasets were described here; the other eight
# were collected later and never added, so the page a reader opens to find out
# what the project holds understated it by more than half.


def _wide_db(tmp_path):
    """A two-game, four-team 2016 season with one row in each later archive."""
    conn = sqlite3.connect(str(tmp_path / "wide.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, week INTEGER,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        CREATE TABLE game_drives (game_id INTEGER, drive_number INTEGER);
        CREATE TABLE game_lines (game_id INTEGER, provider TEXT, market_home_margin REAL);
        CREATE TABLE player_team_seasons (player_id INTEGER, season_year INTEGER, team_id INTEGER);
        CREATE TABLE player_season_stats (player_id INTEGER, season_year INTEGER, team_id INTEGER);
        CREATE TABLE coach_tenures (coach_id INTEGER, season_year INTEGER, team_id INTEGER);
        CREATE TABLE recruiting_classes (source_team TEXT, season_year INTEGER, team_id INTEGER);
        CREATE TABLE poll_rankings (season_year INTEGER, season_type TEXT, week INTEGER,
            poll TEXT, rank INTEGER, school TEXT);
        INSERT INTO games VALUES (1, 2016, 1, 10, 20, 24, 17), (2, 2016, 2, 30, 40, 14, 21);
    """)
    conn.commit()
    return conn


def test_a_per_game_archive_counts_games_and_not_rows(tmp_path):
    # 41 drives and 3 providers' lines on ONE of the two games. Counting rows
    # would report 41 of 2 games covered.
    conn = _wide_db(tmp_path)
    conn.executemany("INSERT INTO game_drives VALUES (?,?)", [(1, n) for n in range(41)])
    conn.executemany("INSERT INTO game_lines VALUES (?,?,?)",
                     [(1, "consensus", -3.5), (1, "Bovada", -3.0), (1, "DraftKings", -3.5)])
    conn.commit()
    cov = build_coverage(conn)
    assert cov["seasons"]["2016"]["drives"] == [1, 2]
    assert cov["seasons"]["2016"]["lines"] == [1, 2]
    conn.close()


def test_a_line_with_no_priced_spread_is_not_coverage(tmp_path):
    # A game nobody priced is stored with a NULL spread on purpose (see
    # sql/line_tables.sql); counting the row would claim the market called it.
    conn = _wide_db(tmp_path)
    conn.executemany("INSERT INTO game_lines VALUES (?,?,?)",
                     [(1, "consensus", -3.5), (2, "consensus", None)])
    conn.commit()
    assert build_coverage(conn)["seasons"]["2016"]["lines"] == [1, 2]
    conn.close()


def test_a_per_team_archive_counts_teams_that_played(tmp_path):
    conn = _wide_db(tmp_path)
    # Team 99 played no games in 2016; its rows are not coverage of anything,
    # exactly as for team_season_advanced.
    conn.executemany("INSERT INTO player_team_seasons VALUES (?,?,?)",
                     [(1, 2016, 10), (2, 2016, 10), (3, 2016, 20), (4, 2016, 99)])
    conn.executemany("INSERT INTO player_season_stats VALUES (?,?,?)", [(1, 2016, 10), (5, 2016, 99)])
    conn.executemany("INSERT INTO coach_tenures VALUES (?,?,?)", [(7, 2016, 10), (8, 2016, 20)])
    conn.executemany("INSERT INTO recruiting_classes VALUES (?,?,?)",
                     [("A", 2016, 10), ("B", 2016, None)])     # an FCS class keeps a NULL team
    conn.commit()
    cov = build_coverage(conn)["seasons"]["2016"]
    assert cov["rosters"] == [2, 4], "two of the four teams that played, not four player rows"
    assert cov["season_stats"] == [1, 4]
    assert cov["coaches"] == [2, 4]
    assert cov["recruiting"] == [1, 4], "a class with no team here is not coverage of a team here"
    conn.close()


def test_a_poll_released_in_a_week_with_no_games_is_still_a_slot(tmp_path):
    """The denominator used to be the game weeks plus one, which is a guess,
    and the guess was low: the preseason poll falls in no game week, so 1982
    reported 16 releases of 15 and the page showed more than all of
    something. A numerator above its denominator is a defect wherever it
    appears, so it is pinned here rather than left to the real-archive test."""
    conn = _wide_db(tmp_path)
    weeks = {week for (week,) in
             conn.execute("SELECT DISTINCT week FROM games WHERE season_year = 2016")}
    unplayed = max(weeks) + 5
    conn.executemany("INSERT INTO poll_rankings VALUES (?,?,?,?,?,?)",
                     [(2016, "regular", week, "ap", 1, "T1")
                      for week in sorted(weeks) + [unplayed]])
    conn.commit()
    have, of = build_coverage(conn)["seasons"]["2016"]["polls"]
    assert have == len(weeks) + 1
    assert have <= of, 'a release in a week with no game is a slot, not an overflow'
    conn.close()


def test_polls_count_releases_and_not_ranked_teams(tmp_path):
    # 25 teams in one week is one release, not 25. The denominator is every
    # release slot either source attests: the weeks games were played in, and
    # the weeks a poll was actually released in.
    conn = _wide_db(tmp_path)
    conn.executemany("INSERT INTO poll_rankings VALUES (?,?,?,?,?,?)",
                     [(2016, "regular", 1, "ap", n, f"T{n}") for n in range(1, 26)] +
                     [(2016, "postseason", 1, "ap", 1, "T1")])
    conn.commit()
    assert build_coverage(conn)["seasons"]["2016"]["polls"] == [2, 3]
    conn.close()


def test_box_scores_are_counted_from_the_files_not_a_table(tmp_path):
    # The box-score archive is gzipped JSON on disk, so the exporter -- which
    # has already read it -- hands the counts in. Nothing passed means nothing
    # counted, not a crash.
    conn = _wide_db(tmp_path)
    assert build_coverage(conn)["seasons"]["2016"]["box_scores"] == [0, 2]
    assert build_coverage(conn, box_scores={2016: 1})["seasons"]["2016"]["box_scores"] == [1, 2]
    assert build_coverage(conn, box_scores={"2016": 2})["seasons"]["2016"]["box_scores"] == [2, 2]
    conn.close()


def test_every_later_archive_survives_a_database_that_never_loaded_it(tmp_path):
    # A fresh bootstrap loads games and little else. Each absent table must read
    # as "not collected", never as a crash.
    conn = sqlite3.connect(str(tmp_path / "bare.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        INSERT INTO games VALUES (1, 2016, 10, 20, 24, 17);
    """)
    conn.commit()
    row = build_coverage(conn)["seasons"]["2016"]
    for key in ("drives", "lines", "box_scores", "team_box", "season_stats",
                "rosters", "coaches", "recruiting", "polls"):
        assert row[key][0] == 0, key
    assert row["polls"] == [0, 0], "no week column means no denominator, not week 1"
    conn.close()


def test_team_box_scores_count_games_and_not_rows(tmp_path):
    """One game holds two teams and dozens of categories apiece: the archive
    is stored long, so counting rows would report a season as many times
    over-complete."""
    conn = _wide_db(tmp_path)
    conn.executescript("""
        CREATE TABLE game_team_stats (game_id INTEGER, team_id INTEGER,
            season_year INTEGER, category TEXT, stat TEXT,
            PRIMARY KEY (game_id, team_id, category));
    """)
    game = conn.execute("SELECT game_id FROM games WHERE season_year = 2016 "
                        "AND home_score IS NOT NULL LIMIT 1").fetchone()[0]
    conn.executemany("INSERT INTO game_team_stats VALUES (?,?,?,?,?)",
                     [(game, team, 2016, category, "1")
                      for team in (10, 20)
                      for category in ("firstDowns", "totalYards", "sacks")])
    conn.commit()
    have, of = build_coverage(conn)["seasons"]["2016"]["team_box"]
    assert have == 1, 'six rows over one game is one game with a box score'
    assert have <= of
    conn.close()


def test_the_nine_later_archives_are_all_described(tmp_path):
    added = {"drives", "lines", "box_scores", "team_box", "season_stats",
             "rosters", "coaches", "recruiting", "polls"}
    assert added <= set(KEYS)
    assert len(KEYS) == 15, "six original dimensions plus the nine added here"
    for d in DIMENSIONS:
        assert d["label"] and d["source"] and d["absent"].endswith(".")


def test_the_deduplicated_count_matches_asking_every_row(tmp_path):
    """The fast form must answer exactly what the per-row question answered.

    `_played_team_counts` replaced a correlated EXISTS over every row, which
    cost 15.2s on 1.1m player_season_stats rows because nothing indexes the
    lookup. The rewrite is only correct if it is indistinguishable, so this
    asks the old question directly and compares -- including a team that played
    ONLY as the away side, which a one-legged participation set would lose.
    """
    conn = _wide_db(tmp_path)
    conn.executemany("INSERT INTO player_season_stats VALUES (?,?,?)",
                     [(1, 2016, 10), (2, 2016, 10), (3, 2016, 10),   # three rows, one team
                      (4, 2016, 20), (5, 2016, 99),                  # 99 never played
                      (6, 2016, None)])                              # and no team at all
    conn.commit()
    slow = {int(season): int(n) for season, n in conn.execute("""
        SELECT season_year, COUNT(DISTINCT team_id) FROM player_season_stats p
        WHERE team_id IS NOT NULL
          AND EXISTS (SELECT 1 FROM games g WHERE g.season_year = p.season_year
                      AND (g.home_team_id = p.team_id OR g.away_team_id = p.team_id))
        GROUP BY 1""")}
    assert cov_mod._played_team_counts(conn, "player_season_stats") == slow
    assert slow, "a fixture that counts nothing would make this vacuous"
    conn.close()


def test_a_team_that_only_ever_played_away_still_counts(tmp_path):
    # The participation set unions the home and away columns. Keeping one leg
    # only would silently drop every team with no home game that season.
    conn = _wide_db(tmp_path)
    away_only = conn.execute("""SELECT away_team_id FROM games
                                WHERE season_year = 2016 AND away_team_id NOT IN
                                (SELECT home_team_id FROM games WHERE season_year = 2016)
                                LIMIT 1""").fetchone()
    if away_only is None:                       # give the fixture one on purpose
        conn.execute("INSERT INTO games (game_id, season_year, home_team_id, away_team_id, "
                     "completed) VALUES (900, 2016, 10, 77, 1)")
        away_only = (77,)
    conn.execute("INSERT INTO player_season_stats VALUES (?,?,?)", (90, 2016, away_only[0]))
    conn.commit()
    counts = cov_mod._played_team_counts(conn, "player_season_stats")
    assert counts.get(2016, 0) >= 1, f"team {away_only[0]} played away and was dropped"
    conn.close()
