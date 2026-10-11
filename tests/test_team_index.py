"""
Milestone 1 (team pages): the router's data comes from
export_dashboard_data.build_team_index() and load_team_records_by_year().
These pin down the properties the dashboard relies on.
"""
import re
import sqlite3

from export_dashboard_data import build_team_index, load_team_records_by_year, slugify


def test_slugify_known_awkward_names():
    assert slugify("Miami (FL)") == "miami-fl"
    assert slugify("Miami (OH)") == "miami-oh"
    assert slugify("Texas A&M") == "texas-am"
    assert slugify("Hawai'i") == "hawaii"
    assert slugify("North Dakota State") == "north-dakota-state"


def test_team_index_covers_every_team_with_unique_url_safe_slugs(db_path):
    index = build_team_index(str(db_path))
    conn = sqlite3.connect(str(db_path))
    db_ids = {r[0] for r in conn.execute("SELECT team_id FROM teams")}
    conn.close()

    assert {t["id"] for t in index} == db_ids, "every team needs exactly one index row"
    slugs = [t["slug"] for t in index]
    assert len(slugs) == len(set(slugs)), "slugs must be unique or URLs become ambiguous"
    assert all(re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", s) for s in slugs)


def _tiny_db(tmp_path):
    db = tmp_path / "tiny.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE games (season_year INTEGER, home_team_id INTEGER, away_team_id INTEGER,"
        " home_score INTEGER, away_score INTEGER)"
    )
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?)", [
        (2020, 1, 2, 30, 20),     # team 1 win, team 2 loss
        (2020, 2, 1, 17, 17),     # tie for both
        (2020, 3, 1, 10, 24),     # team 1 win (away), team 3 loss
        (2020, 1, 3, None, None), # scheduled / incomplete: must NOT count
    ])
    conn.commit()
    conn.close()
    return str(db)


def test_records_count_only_completed_games(tmp_path):
    rec = load_team_records_by_year(_tiny_db(tmp_path))["2020"]
    assert rec["1"] == [2, 0, 1]
    assert rec["2"] == [0, 1, 1]
    assert rec["3"] == [0, 1, 0]


def test_records_total_matches_completed_games_on_real_db(db_path):
    rec = load_team_records_by_year(str(db_path))
    conn = sqlite3.connect(str(db_path))
    completed = conn.execute(
        "SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchone()[0]
    conn.close()
    # every completed game produces exactly two team results
    total_results = sum(sum(v) for season in rec.values() for v in season.values())
    assert total_results == 2 * completed


# ---- games against unrated opponents (mostly FCS) ----
# These live in their own table so that no rating can read them. The export
# below exists so the record beside a team's name can count them anyway, which
# means the two things worth testing are that it counts the same games
# export_team_pages counts, and that nothing it touches reaches `games`.

def test_unrated_records_match_the_team_page_export(db_path):
    """One definition of an unrated record, used in two exports.

    export_team_pages.py builds the same per-team, per-season tally for the
    Schedule tab and the Analytics card. If this one ever counted differently,
    the header and the body of the same page would disagree again -- which is
    the bug it was added to fix.
    """
    from collections import defaultdict

    from export_dashboard_data import load_non_fbs_records_by_year
    from export_team_pages import load_non_fbs_games

    conn = sqlite3.connect(str(db_path))
    try:
        rows = load_non_fbs_games(conn)
    finally:
        conn.close()
    if not rows:
        import pytest
        pytest.skip("this database holds no games against unrated opponents")

    expected = defaultdict(lambda: [0, 0, 0])
    for _gid, season, _date, _wk, tid, _opp, mine, theirs, *_rest in rows:
        expected[(tid, season)][0 if mine > theirs else 1 if mine < theirs else 2] += 1

    got = load_non_fbs_records_by_year(str(db_path))
    assert {(int(t), int(y)): v for y, d in got.items() for t, v in d.items()} == dict(expected)


def test_unrated_games_are_not_in_the_rated_records(db_path):
    """The two exports count disjoint sets of games.

    Not a tautology: both are keyed by (season, team) and summed the same way,
    so the only thing stopping a double count is that `games` never held these
    rows. This asserts that separation from the outside.
    """
    from export_dashboard_data import load_non_fbs_records_by_year

    rated_total = sum(sum(v) for d in load_team_records_by_year(str(db_path)).values()
                      for v in d.values())
    unrated_total = sum(sum(v) for d in load_non_fbs_records_by_year(str(db_path)).values()
                        for v in d.values())

    conn = sqlite3.connect(str(db_path))
    try:
        games = conn.execute(
            "SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
        ).fetchone()[0]
        non_fbs = conn.execute("SELECT COUNT(*) FROM non_fbs_games").fetchone()[0]
    finally:
        conn.close()
    # Two team-seasons per rated game, one per unrated game: an unrated game has
    # only one side this project holds.
    assert rated_total == 2 * games
    assert unrated_total == non_fbs


def test_unrated_records_are_absent_when_the_table_is(tmp_path):
    """A database without the non-FBS table exports nothing, rather than failing."""
    from export_dashboard_data import load_non_fbs_records_by_year

    db = tmp_path / "bare.db"
    sqlite3.connect(str(db)).close()
    assert load_non_fbs_records_by_year(str(db)) == {}
