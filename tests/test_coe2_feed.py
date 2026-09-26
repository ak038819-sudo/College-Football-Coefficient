"""
The website feed for CoE 2.0's conference five-season (entering) value, and the
bonus magnitudes a season total was built with.

build_coe2_rollups.py computes these; nothing showed them. These tests pin the
two things that can silently go wrong on the way to the browser: the ORDER (which
belongs to conference_coe2_rank(), not to the exporter) and the WINDOW that
travels with each number (without it, a reader cannot tell which seasons a value
covers, and the frozen CoE 2.0 value is indistinguishable from CoE v1's rolling
one, which includes the current season).
"""
import json
import sqlite3
import subprocess
import sys

import pytest

from build_coe2_rollups import conference_coe2_rank
from export_dashboard_data import build_coe2_bonuses, build_conference_coe2_5yr

ROLLUP_SCHEMA = """
    CREATE TABLE conference_coe2_5yr_by_season (
        season_year INTEGER, conference TEXT, coe2_5yr REAL, window_start_year INTEGER,
        window_end_year INTEGER, seasons_counted INTEGER, external_games INTEGER,
        formula_version TEXT);
    CREATE TABLE conference_standings_by_year (season_year INTEGER, conference TEXT,
        team_id INTEGER, conf_rank INTEGER);
"""


def _db(tmp_path, rows, standings=()):
    path = tmp_path / "feed.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(ROLLUP_SCHEMA)
    conn.executemany("INSERT INTO conference_coe2_5yr_by_season VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.executemany("INSERT INTO conference_standings_by_year VALUES (?,?,?,?)", standings)
    conn.commit()
    conn.close()
    return str(path)


def test_no_rollup_table_is_an_empty_feed_not_a_crash(tmp_path):
    """A database built before build_coe2_rollups.py has run must export nothing
    rather than fail -- the page then says the value is unavailable."""
    path = tmp_path / "bare.db"
    sqlite3.connect(str(path)).close()
    assert build_conference_coe2_5yr(str(path), "v1") == {}


def test_the_window_travels_with_every_value(tmp_path):
    path = _db(tmp_path, [(2020, "SEC", 40.0, 2015, 2019, 5, 120, "v1")])
    row = build_conference_coe2_5yr(path, "v1")["2020"][0]
    assert row["window"] == [2015, 2019]
    assert row["window"][1] == 2019, "the window must end the season BEFORE the one it enters"
    assert (row["seasons_counted"], row["external_games"]) == (5, 120)


def test_order_and_exclusions_are_the_bid_allocation_contract(tmp_path):
    """The exporter must not invent an ordering: conference_coe2_rank() is the
    canonical one, and it drops Independents and conferences with no members."""
    rows = [(2020, "SEC", 40.0, 2015, 2019, 5, 120, "v1"),
            (2020, "Big Ten", 55.0, 2015, 2019, 5, 130, "v1"),
            (2020, "FBS Independents", 99.0, 2015, 2019, 5, 30, "v1"),
            (2020, "Pac-12", 10.0, 2015, 2019, 5, 90, "v1")]
    # Pac-12 has no members in 2020's standings, so it cannot hold a rank in it.
    standings = [(2020, "SEC", 1, 1), (2020, "Big Ten", 2, 1)]
    path = _db(tmp_path, rows, standings)
    conn = sqlite3.connect(path)
    expected = conference_coe2_rank(conn, 2020)
    conn.close()
    out = build_conference_coe2_5yr(path, "v1")["2020"]
    assert [r["conference"] for r in out] == [c for c, _ in expected] == ["Big Ten", "SEC"]
    assert "FBS Independents" not in {r["conference"] for r in out}
    assert "Pac-12" not in {r["conference"] for r in out}


def test_conferences_on_the_same_value_share_a_rank(tmp_path):
    """Competition ranking (1, 2, 2, 4): float ordering must never present a tie
    as a gap the data does not support."""
    rows = [(2020, c, v, 2015, 2019, 5, 100, "v1")
            for c, v in [("A Conf", 30.0), ("B Conf", 20.0), ("C Conf", 20.0), ("D Conf", 10.0)]]
    out = build_conference_coe2_5yr(_db(tmp_path, rows), "v1")["2020"]
    assert [r["rank"] for r in out] == [1, 2, 2, 4]


def test_a_season_with_no_rankable_conference_is_absent_not_empty(tmp_path):
    """Absence must stay distinguishable from 'existed and earned nothing'."""
    path = _db(tmp_path, [(2020, "FBS Independents", 99.0, 2015, 2019, 5, 30, "v1")])
    assert "2020" not in build_conference_coe2_5yr(path, "v1")


def test_bonus_values_are_exported_with_whether_they_are_all_zero(tmp_path):
    cfg = tmp_path / "model_config.json"
    cfg.write_text(json.dumps({"coe2_bonuses": {"version": "t_v1"}}), encoding="utf-8")
    zero = build_coe2_bonuses(cfg)
    assert zero["version"] == "t_v1" and zero["all_zero"] is True
    assert set(zero["values"]) and all(v == 0.0 for v in zero["values"].values())

    cfg.write_text(json.dumps({"coe2_bonuses": {"version": "t_v2", "bowl_win": 1.5}}), encoding="utf-8")
    paid = build_coe2_bonuses(cfg)
    assert paid["all_zero"] is False and paid["values"]["bowl_win"] == 1.5


def test_the_shipped_config_is_what_the_site_reports(repo_root):
    """The site says bonuses are unweighted; that claim must come from the config
    the rollups were actually built with, never from a hardcoded sentence."""
    shipped = build_coe2_bonuses(repo_root / "config" / "model_config.json")
    assert shipped["all_zero"] is (not any(shipped["values"].values()))


# ---------------------------------------------- against the real database

def test_real_feed_matches_the_rollup_table_row_for_row(db_path):
    version = build_coe2_bonuses()["version"]
    feed = build_conference_coe2_5yr(str(db_path), version)
    conn = sqlite3.connect(str(db_path))
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                        "name='conference_coe2_5yr_by_season'").fetchone():
        conn.close()
        pytest.skip("CoE 2.0 rollups have not been built in this database")
    table = {(s, c): rest for s, c, *rest in conn.execute(
        """SELECT season_year, conference, coe2_5yr, window_start_year, window_end_year,
                  seasons_counted, external_games FROM conference_coe2_5yr_by_season""")}
    for season, rows in feed.items():
        expected = conference_coe2_rank(conn, int(season), version)
        assert [r["conference"] for r in rows] == [c for c, _ in expected]
        for r in rows:
            value, start, end, counted, external = table[(int(season), r["conference"])]
            assert r["coe2_5yr"] == pytest.approx(value, abs=5e-4)
            assert r["window"] == [start, end] and end == int(season) - 1
            assert (r["seasons_counted"], r["external_games"]) == (counted, external)
    conn.close()


def test_the_committed_site_data_carries_the_feed(repo_root):
    """The exported file the page actually loads, not just the builder."""
    data = json.loads((repo_root / "ui" / "dashboard_data.json").read_text(encoding="utf-8"))
    feed = data.get("conference_coe2_5yr_by_year")
    assert feed, "ui/dashboard_data.json must carry conference_coe2_5yr_by_year"
    for season, rows in feed.items():
        assert rows, f"{season} would render as an empty ranking"
        assert [r["rank"] for r in rows] == sorted(r["rank"] for r in rows)
        assert all(r["window"][1] == int(season) - 1 for r in rows)
        assert "FBS Independents" not in {r["conference"] for r in rows}
    assert set(data["coe2_bonuses"]) == {"version", "values", "all_zero"}


# ---------------------------------------------- one formula version only

def test_only_the_configured_version_is_exported(tmp_path):
    """
    build_coe2_rollups.py keeps an older formula_version's rows on purpose, so
    the export must name the version it wants. Reading unfiltered would list a
    conference once per version and pair its value with another version's window,
    which is a wrong number with nothing on the page to reveal it.
    """
    rows = [(2020, "SEC", 40.0, 2015, 2019, 5, 120, "v1"),
            (2020, "SEC", 99.0, 2018, 2019, 2, 44, "v2"),
            (2020, "Big Ten", 30.0, 2015, 2019, 5, 110, "v1"),
            (2020, "Big Ten", 10.0, 2018, 2019, 2, 40, "v2")]
    standings = [(2020, "SEC", 1, 1), (2020, "Big Ten", 2, 1)]
    path = _db(tmp_path, rows, standings)

    v1 = build_conference_coe2_5yr(path, "v1")["2020"]
    assert [r["conference"] for r in v1] == ["SEC", "Big Ten"], "no conference may appear twice"
    assert [r["coe2_5yr"] for r in v1] == [40.0, 30.0]
    assert all(r["window"] == [2015, 2019] and r["seasons_counted"] == 5 for r in v1), \
        "the window must come from the same version as the value"

    v2 = build_conference_coe2_5yr(path, "v2")["2020"]
    assert [(r["conference"], r["coe2_5yr"], r["rank"]) for r in v2] == [("SEC", 99.0, 1), ("Big Ten", 10.0, 2)]
    assert all(r["window"] == [2018, 2019] for r in v2)


def test_rollups_built_for_another_version_export_nothing(tmp_path):
    """An absence the page reports honestly beats a mixture nobody can spot."""
    path = _db(tmp_path, [(2020, "SEC", 40.0, 2015, 2019, 5, 120, "old_v1")])
    assert build_conference_coe2_5yr(path, "new_v2") == {}


# ---------------------------------------------- partial windows are real

def test_a_window_shorter_than_five_seasons_is_published(tmp_path):
    """
    build_conference_5yr publishes a window of whatever prior seasons exist, down
    to one. The feed must carry those, and seasons_counted must say how few they
    are -- the page's explanation of an absent season depends on it.
    """
    rows = [(1986, "SEC", 5.0, 1985, 1985, 1, 20, "v1"),
            (1987, "SEC", 9.0, 1985, 1986, 2, 41, "v1")]
    feed = build_conference_coe2_5yr(_db(tmp_path, rows), "v1")
    assert feed["1986"][0]["seasons_counted"] == 1
    assert feed["1986"][0]["window"] == [1985, 1985]
    assert feed["1987"][0]["seasons_counted"] == 2


def test_the_committed_feed_starts_with_a_partial_window(repo_root):
    """
    Pins the fact the empty-state wording rests on: the earliest value counts
    fewer than five seasons, so "it needs five prior seasons" is never the reason
    an early season has none. The reason is that no earlier season has awards.
    """
    feed = json.loads((repo_root / "ui" / "dashboard_data.json").read_text(
        encoding="utf-8"))["conference_coe2_5yr_by_year"]
    earliest = min(feed, key=int)
    counts = {r["seasons_counted"] for r in feed[earliest]}
    assert counts and max(counts) < 5, \
        f"{earliest} is the first season with a value and must use a partial window"
    first_award = min(r["window"][0] for rows in feed.values() for r in rows)
    assert int(earliest) == first_award + 1, \
        "the first value is the season after the first season with game awards"


def test_the_export_reproduces_byte_for_byte(repo_root):
    """
    load_bonus_config builds its values from a set, so its order varies between
    processes. The deploy job commits whatever changed on every scheduled run, so
    an export that reshuffles itself would produce an endless meaningless diff.
    """
    config = repo_root / "config" / "model_config.json"
    runs = [json.dumps(build_coe2_bonuses(config)) for _ in range(2)]
    assert runs[0] == runs[1]
    out = subprocess.run([sys.executable, "-c",
                          "import sys; sys.path[:0] = sys.argv[1:3]; import json;"
                          "from export_dashboard_data import build_coe2_bonuses;"
                          "print(json.dumps(build_coe2_bonuses(__import__('pathlib').Path(sys.argv[3]))))",
                          str(repo_root / "src"), str(repo_root / "src" / "coefficients"), str(config)],
                         capture_output=True, text=True, check=True)
    assert out.stdout.strip() == runs[0], "a fresh process must serialize the same bytes"
