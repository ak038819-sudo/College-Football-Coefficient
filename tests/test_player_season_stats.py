"""Season player statistics: what the fetcher keeps, and what it refuses to keep.

The rule these exist to hold: a statistic is attached to a person because the
source numbered them, never because two names looked alike.
"""
import pytest

from fetch_cfbd_player_season_stats import FIRST_SEASON, stat_rows


def _row(**over):
    row = {"playerId": 4361182, "player": "Bryce Young", "team": "Alabama",
           "conference": "SEC", "category": "passing", "statType": "YDS", "stat": 4872}
    row.update(over)
    return row


def test_a_stat_row_keeps_the_athlete_id_the_feed_gave():
    [row] = stat_rows([_row()], 2021)
    assert row == {"athlete_id": "4361182", "name": "Bryce Young", "team": "Alabama",
                   "conference": "SEC", "season_year": 2021, "category": "passing",
                   "stat_type": "YDS", "stat": 4872}


def test_a_row_with_no_athlete_id_is_dropped_rather_than_matched_by_name():
    """The whole reason this endpoint is used instead of the box-score archive.
    A row the source will not number cannot be attached to a person, and a page
    showing one player's yards under another player's name is worse than a page
    showing none."""
    assert stat_rows([_row(playerId=None)], 2021) == []
    assert stat_rows([{"player": "Bryce Young", "category": "passing",
                       "statType": "YDS", "stat": 1}], 2021) == []


@pytest.mark.parametrize("missing", ["player", "category", "statType"])
def test_a_row_missing_what_identifies_the_statistic_is_dropped(missing):
    """A stat with no category or type cannot be labelled on a page, and an
    unlabelled number is not a fact about anybody."""
    assert stat_rows([_row(**{missing: None})], 2021) == []


@pytest.mark.parametrize("value,expected", [
    (4872, 4872), ("4872", 4872), (12.5, 12.5), ("12.5", 12.5),
    # Not seen in 2025 -- every row that season is numeric -- but a value the
    # feed writes as text must survive as text rather than become a number
    # nobody sent. Only 2025 has been fetched so far.
    ("19/30", "19/30"), ("T80", "T80"),
    (None, None), ("", None),
])
def test_a_stat_is_a_number_only_where_the_source_gave_one(value, expected):
    [row] = stat_rows([_row(stat=value)], 2021)
    assert row["stat"] == expected


def test_the_feeds_own_key_spellings_are_both_read():
    """CFBD has shipped both camelCase and snake_case for these fields. Reading
    one spelling only would archive an empty season and look like real coverage."""
    [row] = stat_rows([{"athlete_id": 99, "name": "Snake Case", "school": "Oregon",
                        "category": "rushing", "stat_type": "YDS", "value": 100}], 2015)
    assert (row["athlete_id"], row["name"], row["team"], row["category"],
            row["stat_type"], row["stat"]) == ("99", "Snake Case", "Oregon", "rushing",
                                               "YDS", 100)


def test_a_payload_that_is_not_a_list_is_an_error_not_an_empty_season():
    """An error object written as a snapshot would look like a season in which
    nobody recorded a statistic."""
    with pytest.raises(ValueError):
        stat_rows({"error": "unauthorized"}, 2021)


def test_the_coverage_boundary_is_stated_rather_than_assumed():
    assert FIRST_SEASON == 2004


# --- the loader -------------------------------------------------------------

import json
import sqlite3

import person_identity as identity
from load_player_season_stats import load_season_stats

TEAMS = {1: "Clemson", 2: "Oregon"}


@pytest.fixture
def conn(repo_root):
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    c.executescript((repo_root / "sql" / "schema.sql").read_text(encoding="utf-8"))
    c.executescript((repo_root / "sql" / "person_tables.sql").read_text(encoding="utf-8"))
    for team_id, name in TEAMS.items():
        c.execute("INSERT INTO teams (team_id, team_name) VALUES (?, ?)", (team_id, name))
    return c


def _person(conn, athlete_id, name):
    pid = identity.create_player(conn, name, player_id=identity.source_player_id(athlete_id))
    identity.link_external_id(conn, pid, "cfbd", str(athlete_id), "id")
    return pid


def _snapshot(tmp_path, year, rows):
    path = tmp_path / f"{year}.json"
    path.write_text(json.dumps([{"season_year": year, "conference": "ACC", **r}
                                for r in rows]), encoding="utf-8")
    return path


def test_a_statistic_lands_on_the_person_the_athlete_id_names(conn, tmp_path):
    pid = _person(conn, 4361182, "Bryce Young")
    stats = load_season_stats(conn, _snapshot(tmp_path, 2021, [
        {"athlete_id": "4361182", "name": "Bryce Young", "team": "Clemson",
         "category": "passing", "stat_type": "YDS", "stat": 4872},
        {"athlete_id": "4361182", "name": "Bryce Young", "team": "Clemson",
         "category": "passing", "stat_type": "TD", "stat": 47}]))
    assert stats["loaded"] == 2 and stats["people"] == 1
    assert conn.execute("SELECT stat_type, stat FROM player_season_stats WHERE player_id = ? "
                        "ORDER BY stat_type", (pid,)).fetchall() == [("TD", 47), ("YDS", 4872)]


def test_an_unknown_athlete_id_is_recorded_rather_than_matched_by_name(conn, tmp_path):
    """The rule this loader exists to keep. A person with the same name is
    already here, and the stat row must NOT land on them: a roster says who was
    present, but a statistic says what somebody did, so a wrong attachment puts
    one player's yards under another player's name."""
    pid = _person(conn, 111, "Bryce Young")
    stats = load_season_stats(conn, _snapshot(tmp_path, 2021, [
        {"athlete_id": "999999", "name": "Bryce Young", "team": "Clemson",
         "category": "passing", "stat_type": "YDS", "stat": 4872}]))
    assert stats["loaded"] == 0 and stats["unknown_athlete"] == 1
    assert conn.execute("SELECT COUNT(*) FROM player_season_stats").fetchone()[0] == 0
    assert conn.execute("SELECT reason FROM person_unresolved WHERE entity = 'player'") \
        .fetchall() == [("athlete id with no roster row in this database",)]
    assert conn.execute("SELECT COUNT(*) FROM player_season_stats WHERE player_id = ?",
                        (pid,)).fetchone()[0] == 0


def test_a_school_outside_this_database_is_recorded_not_dropped_silently(conn, tmp_path):
    """15,568 rows in the real 2025 season are a person this database knows at a
    school it does not -- a player who moved to an FCS programme. The row cannot
    be stored against a team_id that does not exist, so it is recorded."""
    _person(conn, 222, "Transferred Down")
    stats = load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "222", "name": "Transferred Down", "team": "Not In This Database",
         "category": "rushing", "stat_type": "YDS", "stat": 500}]))
    assert stats["loaded"] == 0 and stats["unknown_team"] == 1
    assert conn.execute("SELECT reason, source_team FROM person_unresolved").fetchall() == \
        [("unknown team", "Not In This Database")]


def test_reloading_a_season_replaces_its_statistics_rather_than_accumulating(conn, tmp_path):
    """The snapshot is the whole season, so a corrected statistic must change.
    Accumulating would leave a page showing two different totals as both true."""
    pid = _person(conn, 333, "Corrected Guy")
    load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "333", "name": "Corrected Guy", "team": "Oregon",
         "category": "rushing", "stat_type": "YDS", "stat": 900}]))
    load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "333", "name": "Corrected Guy", "team": "Oregon",
         "category": "rushing", "stat_type": "YDS", "stat": 1000}]))
    assert conn.execute("SELECT stat FROM player_season_stats WHERE player_id = ?",
                        (pid,)).fetchall() == [(1000,)]


def test_a_statistic_that_disappears_from_a_corrected_snapshot_goes_with_it(conn, tmp_path):
    """Replacement has to mean the season's rows, not just the rows the new
    snapshot happens to name, or a statistic CFBD retracted would live forever."""
    pid = _person(conn, 444, "Retracted Guy")
    load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "444", "name": "Retracted Guy", "team": "Oregon",
         "category": "rushing", "stat_type": "YDS", "stat": 900},
        {"athlete_id": "444", "name": "Retracted Guy", "team": "Oregon",
         "category": "rushing", "stat_type": "TD", "stat": 9}]))
    load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "444", "name": "Retracted Guy", "team": "Oregon",
         "category": "rushing", "stat_type": "YDS", "stat": 900}]))
    assert conn.execute("SELECT stat_type FROM player_season_stats WHERE player_id = ?",
                        (pid,)).fetchall() == [("YDS",)]


def test_loading_one_season_leaves_another_alone(conn, tmp_path):
    pid = _person(conn, 555, "Two Seasons")
    for year in (2024, 2025):
        load_season_stats(conn, _snapshot(tmp_path, year, [
            {"athlete_id": "555", "name": "Two Seasons", "team": "Oregon",
             "category": "receiving", "stat_type": "REC", "stat": year - 2000}]))
    assert conn.execute("SELECT season_year, stat FROM player_season_stats "
                        "WHERE player_id = ? ORDER BY season_year", (pid,)).fetchall() == \
        [(2024, 24), (2025, 25)]


def test_a_text_statistic_survives_as_text_and_a_number_as_a_number(conn, tmp_path):
    """The column has NUMERIC affinity so a leaderboard sorts correctly. TEXT
    affinity would store 4872 as '4872' and sort 500 above it."""
    pid = _person(conn, 666, "Mixed Stats")
    load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "666", "name": "Mixed Stats", "team": "Oregon",
         "category": "passing", "stat_type": "YDS", "stat": 4872},
        {"athlete_id": "666", "name": "Mixed Stats", "team": "Oregon",
         "category": "passing", "stat_type": "COMP", "stat": "19/30"}]))
    rows = dict(conn.execute("SELECT stat_type, stat FROM player_season_stats "
                             "WHERE player_id = ?", (pid,)))
    assert rows["YDS"] == 4872 and isinstance(rows["YDS"], int)
    assert rows["COMP"] == "19/30"
    # The ordering a leaderboard depends on, which TEXT affinity would break.
    assert conn.execute("SELECT stat FROM player_season_stats WHERE stat_type = 'YDS' "
                        "AND stat > 1000").fetchone() == (4872,)


def test_an_unresolved_person_is_recorded_once_not_once_per_statistic(conn, tmp_path):
    """One player at a school outside this database is ONE fact. The real 2025
    season has 40,291 such rows for about 4,400 people, so recording per row
    would make the review list forty thousand lines of the same thing."""
    load_season_stats(conn, _snapshot(tmp_path, 2025, [
        {"athlete_id": "777777", "name": "Fcs Player", "team": "Clemson",
         "category": "rushing", "stat_type": stat_type, "stat": 1}
        for stat_type in ("YDS", "TD", "CAR", "LONG", "YPC")]))
    assert conn.execute("SELECT COUNT(*) FROM person_unresolved").fetchone()[0] == 1
