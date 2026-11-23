"""
Boards for a reader who does not already know a name.

`#section=people` rendered a search box and nothing else. These drive the
payload that puts 99,813 players and 827 coaches behind something browsable.

Two refusals are the point of this module and are tested as such: a rate or a
maximum is never summed into a career total, and a coach season two head
coaches shared is never folded into a career figure.

The browser side is tests/people_boards.test.cjs.
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import export_people_boards as ex  # noqa: E402


def db(*, stats=(), players=(), metrics=(), coaches=(), tables=("stats", "coach")):
    conn = sqlite3.connect(":memory:")
    if "stats" in tables:
        conn.executescript("""
          CREATE TABLE player_season_stats (player_id INTEGER, season_year INTEGER,
            team_id INTEGER, category TEXT, stat_type TEXT, stat REAL, source TEXT);
          CREATE TABLE players (player_id INTEGER PRIMARY KEY, display_name TEXT);
        """)
        conn.executemany("INSERT INTO player_season_stats (player_id, season_year, category, "
                         "stat_type, stat) VALUES (?,?,?,?,?)", stats)
        conn.executemany("INSERT INTO players VALUES (?,?)", players)
    if "coach" in tables:
        conn.executescript("""
          CREATE TABLE coach_season_metrics (coach_id INTEGER, team_id INTEGER,
            season_year INTEGER, attributed INTEGER, reason TEXT, games INTEGER,
            wins INTEGER, losses INTEGER, ties INTEGER, expected_wins REAL,
            wins_above_expected REAL);
          CREATE TABLE coaches (coach_id INTEGER PRIMARY KEY, display_name TEXT);
        """)
        conn.executemany("INSERT INTO coach_season_metrics (coach_id, season_year, attributed, "
                         "games, wins, wins_above_expected) VALUES (?,?,?,?,?,?)", metrics)
        conn.executemany("INSERT INTO coaches VALUES (?,?)", coaches)
    return conn


# ---- career leaders -------------------------------------------------------

def test_no_board_is_built_on_a_rate_or_a_maximum():
    # A career completion percentage made by adding eighteen season
    # percentages is not a number, and LONG is a maximum, not a total.
    offered = {stat for _, stat, _ in ex.COUNTING}
    assert not offered & ex.NOT_SUMMABLE
    assert {"YPA", "PCT", "YPC", "YPR", "AVG", "LONG"} <= ex.NOT_SUMMABLE


def test_a_career_is_the_sum_of_its_seasons_with_the_seasons_counted():
    conn = db(stats=[(1, 2019, "rushing", "YDS", 1000), (1, 2020, "rushing", "YDS", 1500),
                     (2, 2019, "rushing", "YDS", 1200)],
              players=[(1, "A Back"), (2, "B Back")])
    board = ex.build_career_leaders(conn)["rushing_YDS"]
    assert board["leaders"][0][:4] == [1, "A Back", 2500.0, 2]
    assert board["leaders"][1][:4] == [2, "B Back", 1200.0, 1]
    assert board["leaders"][0][4] == 2019, "and the first season the archive has for him"


def test_each_board_reports_the_season_its_own_archive_starts():
    # Defensive statistics begin seven years after passing ones. One claim for
    # both would be wrong about one of them.
    conn = db(stats=[(1, 2009, "passing", "YDS", 3000), (1, 2016, "defensive", "TOT", 90)],
              players=[(1, "A Player")])
    boards = ex.build_career_leaders(conn)
    assert boards["passing_YDS"]["first_season"] == 2009
    assert boards["defensive_TOT"]["first_season"] == 2016


def test_a_half_sack_is_not_rounded_into_an_integer():
    conn = db(stats=[(1, 2019, "defensive", "SACKS", 7.5), (1, 2020, "defensive", "SACKS", 4.0)],
              players=[(1, "A Rusher")])
    assert ex.build_career_leaders(conn)["defensive_SACKS"]["leaders"][0][2] == 11.5


def test_ties_break_on_the_name_so_the_board_is_the_same_every_build():
    conn = db(stats=[(1, 2019, "rushing", "YDS", 500), (2, 2019, "rushing", "YDS", 500)],
              players=[(1, "Zeta"), (2, "Alpha")])
    assert [r[1] for r in ex.build_career_leaders(conn)["rushing_YDS"]["leaders"]] == \
        ["Alpha", "Zeta"]


def test_a_board_is_capped_and_the_cap_is_the_top_of_it():
    rows = [(i, 2019, "rushing", "YDS", i) for i in range(1, 11)]
    conn = db(stats=rows, players=[(i, f"P{i}") for i in range(1, 11)])
    board = ex.build_career_leaders(conn, limit=3)["rushing_YDS"]
    assert [r[2] for r in board["leaders"]] == [10.0, 9.0, 8.0]


def test_a_statistic_for_a_player_this_database_cannot_name_is_left_off():
    # Rather than a row reading "undefined" with a link that goes nowhere.
    conn = db(stats=[(1, 2019, "rushing", "YDS", 900), (99, 2019, "rushing", "YDS", 2000)],
              players=[(1, "A Back")])
    assert [r[0] for r in ex.build_career_leaders(conn)["rushing_YDS"]["leaders"]] == [1]


def test_a_category_with_nothing_in_it_is_absent_rather_than_an_empty_board():
    conn = db(stats=[(1, 2019, "rushing", "YDS", 900)], players=[(1, "A Back")])
    boards = ex.build_career_leaders(conn)
    assert "rushing_YDS" in boards
    assert "passing_YDS" not in boards


def test_a_database_with_no_archive_publishes_nothing():
    assert ex.build_career_leaders(db(tables=())) == {}
    assert ex.build_coach_board(db(tables=())) == {}
    assert ex.build(db(tables=())) == {}


# ---- the coach board ------------------------------------------------------

def test_a_shared_season_is_counted_beside_the_total_and_never_inside_it():
    # CFBD's feed never says which games a coach was on the sideline for when
    # a team had two. Crediting an interim with a predecessor's wins is the
    # failure this guards.
    conn = db(metrics=[(1, 2019, 1, 12, 9, 2.5), (1, 2020, 0, None, None, None),
                       (1, 2021, 1, 12, 10, 1.5)],
              coaches=[(1, "A Coach")], tables=("coach",))
    row = ex.build_coach_board(conn)["coaches"][0]
    idx = {f: i for i, f in enumerate(ex.build_coach_board(conn)["fields"])}
    assert row[idx["wins_above_expected"]] == 4.0
    assert row[idx["seasons_measured"]] == 2
    assert row[idx["seasons_unmeasured"]] == 1
    assert row[idx["first_season"]] == 2019 and row[idx["last_season"]] == 2021, \
        "the span still covers the season that could not be measured"


def test_a_coach_whose_every_season_is_shared_is_absent_not_a_zero():
    conn = db(metrics=[(1, 2019, 0, None, None, None)], coaches=[(1, "An Interim")],
              tables=("coach",))
    out = ex.build_coach_board(conn)
    assert out["coaches"] == []
    assert out["measured_coaches"] == 0
    assert out["unmeasured_seasons"] == 1, "he is still counted as unmeasured"


def test_the_board_is_ordered_best_first_and_says_what_it_is_the_top_of():
    conn = db(metrics=[(1, 2019, 1, 12, 6, -1.0), (2, 2019, 1, 12, 11, 3.0),
                       (3, 2019, 1, 12, 9, 1.0)],
              coaches=[(1, "Worst"), (2, "Best"), (3, "Middle")], tables=("coach",))
    out = ex.build_coach_board(conn, limit=2)
    assert [r[1] for r in out["coaches"]] == ["Best", "Middle"]
    assert out["measured_coaches"] == 3, "the denominator is every measured coach, not the cap"


def test_a_negative_career_survives_rather_than_being_clipped_at_zero():
    conn = db(metrics=[(1, 2019, 1, 12, 3, -2.75)], coaches=[(1, "A Coach")], tables=("coach",))
    assert ex.build_coach_board(conn)["coaches"][0][2] == -2.75


def test_the_attributed_flag_decides_and_not_whether_a_number_happens_to_be_there():
    """build_coach_metrics writes NULLs for a shared season today, so dropping
    the `attributed` check would pass unnoticed. The flag is the contract: a
    row marked unattributed must be refused even when it carries a figure."""
    conn = db(metrics=[(1, 2019, 1, 12, 9, 2.5), (1, 2020, 0, 12, 8, 9.0)],
              coaches=[(1, "A Coach")], tables=("coach",))
    out = ex.build_coach_board(conn)
    row, idx = out["coaches"][0], {f: i for i, f in enumerate(out["fields"])}
    assert row[idx["wins_above_expected"]] == 2.5, "the unattributed 9.0 must not be added"
    assert row[idx["seasons_measured"]] == 1
    assert row[idx["seasons_unmeasured"]] == 1
    assert row[idx["games"]] == 12, "nor its games"
