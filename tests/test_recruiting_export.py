"""
Recruiting on its way from the archive to a board.

81,262 signees and 4,577 classes had been in the database since 2000 and
reached the browser only as a hometown heat map on a COACH page. These drive
the two payloads that put them on a team page and a ranking board.

The browser side is tests/recruiting_board.test.cjs.
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import export_recruiting as ex  # noqa: E402


def db(*, classes=(), recruits=(), players=(), with_tables=True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    if not with_tables:
        return conn
    conn.executescript("""
      CREATE TABLE recruiting_classes (source_team TEXT, season_year INTEGER,
        team_id INTEGER, class_rank INTEGER, points REAL);
      CREATE TABLE recruits (recruit_id TEXT, season_year INTEGER, athlete_id INTEGER,
        name TEXT, position TEXT, recruit_type TEXT, height INTEGER, weight INTEGER,
        stars INTEGER, rating REAL, national_rank INTEGER, committed_team_id INTEGER,
        committed_to TEXT, high_school TEXT, city TEXT, state_province TEXT,
        country TEXT, hometown_latitude REAL, hometown_longitude REAL,
        hometown_county_fips TEXT);
      CREATE TABLE players (player_id INTEGER PRIMARY KEY, name TEXT);
    """)
    conn.executemany("INSERT INTO recruiting_classes VALUES (?,?,?,?,?)", classes)
    conn.executemany("INSERT INTO recruits (recruit_id, season_year, athlete_id, name, position, "
                     "stars, rating, national_rank, committed_team_id, high_school, city, "
                     "state_province) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", recruits)
    conn.executemany("INSERT INTO players VALUES (?,?)", players)
    return conn


def recruit(rid="r1", season=2024, athlete=None, name="A Player", pos="QB", stars=4,
            rating=0.9, rank=100, team=10, hs="Central", city="Mobile", state="AL"):
    return (rid, season, athlete, name, pos, stars, rating, rank, team, hs, city, state)


# ---- classes --------------------------------------------------------------

def test_a_class_this_database_cannot_place_is_counted_not_dropped():
    # load_recruiting.py deliberately stores Harvard and Northern Iowa with a
    # NULL team_id. A board keyed by team cannot show them, so the count has to
    # travel -- otherwise rank 30 on a board of 25 rows just looks broken.
    out = ex.build_classes(db(classes=[("Alabama", 2024, 10, 1, 300.0),
                                       ("Harvard", 2024, None, 30, 90.0),
                                       ("Yale", 2024, None, 31, 88.0),
                                       ("Auburn", 2023, 20, 4, 250.0)]))
    assert [c[0] for c in out["classes"]] == [20, 10], "placed classes only, season then rank"
    assert out["unplaced"] == {2024: 2}, "and the ones that could not be placed, by season"


def test_the_field_order_is_pinned():
    assert ex.CLASS_FIELDS == ["team_id", "season_year", "class_rank", "points"]
    assert ex.SIGNEE_FIELDS == ["name", "position", "stars", "rating", "national_rank",
                                "athlete_id", "city", "state_province", "high_school"]


def test_an_unranked_class_survives_rather_than_sorting_as_rank_zero():
    out = ex.build_classes(db(classes=[("A", 2024, 10, None, 12.0), ("B", 2024, 20, 2, 99.0)]))
    assert [c[2] for c in out["classes"]] == [2, None], "a real rank first, the unranked after"


def test_a_database_with_no_recruiting_tables_publishes_nothing():
    assert ex.build_classes(db(with_tables=False)) == {}
    assert ex.build_signees(db(with_tables=False)) == {}
    assert ex.build_classes(db()) == {}, "an empty table is not a payload either"


# ---- signees --------------------------------------------------------------

def test_a_signee_links_to_a_player_page_only_when_one_exists():
    # CFBD carries athlete ids for recruits this database has no player row
    # for. A name that looks like a link and goes nowhere is worse than text.
    conn = db(recruits=[recruit("r1", athlete=500), recruit("r2", athlete=999, name="B", rank=2)],
              players=[(500, "A Player")])
    rows = ex.build_signees(conn)[2024]["teams"]["10"]
    linked = {r[0]: r[5] for r in rows}
    assert linked == {"A Player": 500, "B": None}


def test_a_recruit_with_no_team_is_left_out_of_a_payload_about_teams():
    conn = db(recruits=[recruit("r1"), recruit("r2", team=None, name="Nobody")])
    rows = ex.build_signees(conn)[2024]["teams"]
    assert list(rows) == ["10"]
    assert len(rows["10"]) == 1


def test_seasons_and_teams_are_separate_keys():
    conn = db(recruits=[recruit("r1", season=2023, team=10), recruit("r2", season=2024, team=10),
                        recruit("r3", season=2024, team=20)])
    out = ex.build_signees(conn)
    assert sorted(out) == [2023, 2024]
    assert sorted(out[2024]["teams"]) == ["10", "20"]
    assert out[2024]["fields"] == ex.SIGNEE_FIELDS


def test_signees_come_back_in_national_rank_order():
    conn = db(recruits=[recruit("r1", name="Third", rank=300),
                        recruit("r2", name="First", rank=1),
                        recruit("r3", name="Second", rank=20)])
    assert [r[0] for r in ex.build_signees(conn)[2024]["teams"]["10"]] == \
        ["First", "Second", "Third"]


def test_an_unrated_signee_keeps_its_gaps_rather_than_becoming_zero():
    # A recruit the service never rated is not a nought-star recruit, and a
    # mean over a zero would rank his class last on a scale it never sat on.
    conn = db(recruits=[recruit("r1", stars=None, rating=None, rank=None, city=None, state=None)])
    row = ex.build_signees(conn)[2024]["teams"]["10"][0]
    assert row[2] is None and row[3] is None and row[4] is None
    assert row[6] is None and row[7] is None


def test_a_rating_is_not_rounded_away():
    # Composite ratings separate recruits in the fourth decimal; two decimals
    # would collapse most of a class into ties.
    conn = db(recruits=[recruit("r1", rating=0.97531)])
    assert ex.build_signees(conn)[2024]["teams"]["10"][0][3] == 0.9753


def test_a_database_with_no_players_table_still_publishes_the_signees():
    # A bootstrap that loaded recruiting before people: every signee is text,
    # nothing is a broken link, and nobody is dropped.
    conn = sqlite3.connect(":memory:")
    conn.executescript("""CREATE TABLE recruits (recruit_id TEXT, season_year INTEGER,
        athlete_id INTEGER, name TEXT, position TEXT, stars INTEGER, rating REAL,
        national_rank INTEGER, committed_team_id INTEGER, high_school TEXT, city TEXT,
        state_province TEXT);""")
    conn.execute("INSERT INTO recruits VALUES ('r1',2024,500,'A','QB',4,0.9,1,10,'HS','M','AL')")
    assert ex.build_signees(conn)[2024]["teams"]["10"][0][5] is None
