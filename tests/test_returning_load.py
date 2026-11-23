"""
Returning production, from the committed archive to a season payload.

The feed was MEASURED as an Elo preseason adjustment and rejected, so these
numbers are a description of a roster and nothing else reads them. That makes
the loader's strictness the thing worth testing: this endpoint is FBS-only, so
a school that will not resolve is a hole in the identity map, and the loader
refuses the file instead of writing a row with no team.

The browser side is tests/returning_production.test.cjs.
"""
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import export_static_data as ex          # noqa: E402
import load_returning_production as lrp  # noqa: E402

HEADER = ("season_year,team,conference,total_ppa,total_passing_ppa,"
          "total_receiving_ppa,total_rushing_ppa,percent_ppa,percent_passing_ppa,"
          "percent_receiving_ppa,percent_rushing_ppa,usage,passing_usage,"
          "receiving_usage,rushing_usage")


def row(team, season=2024, percent_ppa="0.6", total_ppa="120.5", usage="0.55"):
    return (f"{season},{team},SEC,{total_ppa},50.0,40.0,30.0,{percent_ppa},"
            f"0.5,0.4,0.3,{usage},0.5,0.4,0.3")


def archive(tmp_path, *lines, season=2024):
    path = tmp_path / f"returning_production_{season}.csv"
    path.write_text("\n".join((HEADER,) + lines) + "\n", encoding="utf-8")
    return path


def db(teams=(("Alabama", 1), ("Auburn", 2))):
    conn = sqlite3.connect(":memory:")
    conn.executescript("CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);"
                       "CREATE TABLE team_aliases (alias TEXT, team_name TEXT);")
    conn.executemany("INSERT INTO teams VALUES (?,?)",
                     [(team_id, name) for name, team_id in teams])
    conn.executescript(lrp.SCHEMA.read_text(encoding="utf-8"))
    return conn


# ---- the loader -----------------------------------------------------------

def test_a_season_loads_and_keeps_the_providers_own_school_string(tmp_path):
    conn = db()
    assert lrp.load_file(conn, archive(tmp_path, row("Alabama"))) == 1
    stored = conn.execute("SELECT season_year, team_id, source_team, conference, "
                          "percent_ppa, usage FROM team_returning_production").fetchone()
    assert stored == (2024, 1, "Alabama", "SEC", 0.6, 0.55)


def test_an_unresolvable_school_refuses_the_file_rather_than_dropping_a_row(tmp_path):
    # The opposite of src/load_recruiting.py's rule, and deliberately so: this
    # endpoint is FBS-only, so a name that will not place is a DEFECT.
    conn = db()
    with pytest.raises(ValueError, match="does not resolve"):
        lrp.load_file(conn, archive(tmp_path, row("Alabama"), row("Hogwarts")))


def test_a_blank_metric_stays_null_and_is_never_read_as_a_zero(tmp_path):
    # A team that returned nothing is a real 0. "Not reported" is not.
    conn = db()
    lrp.load_file(conn, archive(tmp_path, row("Alabama", percent_ppa=""),
                                row("Auburn", percent_ppa="0")))
    assert dict(conn.execute("SELECT source_team, percent_ppa "
                             "FROM team_returning_production")) == {
        "Alabama": None, "Auburn": 0.0}


def test_the_file_name_decides_the_season_and_a_row_disagreeing_is_refused(tmp_path):
    conn = db()
    with pytest.raises(ValueError, match="season 2023"):
        lrp.load_file(conn, archive(tmp_path, row("Alabama", season=2023)))


def test_reloading_the_same_archive_updates_rather_than_duplicates(tmp_path):
    conn = db()
    lrp.load_file(conn, archive(tmp_path, row("Alabama", percent_ppa="0.6")))
    lrp.load_file(conn, archive(tmp_path, row("Alabama", percent_ppa="0.7")))
    assert conn.execute("SELECT COUNT(*), percent_ppa "
                        "FROM team_returning_production").fetchone() == (1, 0.7)


def test_a_file_without_a_season_in_its_name_is_not_guessed_at(tmp_path):
    with pytest.raises(ValueError):
        lrp.season_of(tmp_path / "returning_production_latest.csv")
    assert lrp.season_of(tmp_path / "returning_production_2014.csv") == 2014


# ---- the export -----------------------------------------------------------

def built(conn):
    return ex.build_returning_production(conn)


def test_each_season_is_its_own_payload_in_the_pinned_field_order(tmp_path):
    conn = db()
    lrp.load_file(conn, archive(tmp_path, row("Alabama"), row("Auburn"), season=2024))
    lrp.load_file(conn, archive(tmp_path, row("Alabama", season=2025), season=2025))
    payloads = built(conn)
    assert sorted(payloads) == [2024, 2025]
    assert payloads[2024]["fields"] == ["team_id", "percent_ppa", "percent_passing_ppa",
                                        "percent_rushing_ppa", "percent_receiving_ppa",
                                        "usage", "total_ppa"]
    assert [team[0] for team in payloads[2024]["teams"]] == [1, 2]
    assert len(payloads[2025]["teams"]) == 1


def test_a_not_reported_metric_travels_as_null_to_the_browser(tmp_path):
    conn = db()
    lrp.load_file(conn, archive(tmp_path, row("Alabama", percent_ppa="")))
    fields = ex.RETURNING_FIELDS
    team = dict(zip(fields, built(conn)[2024]["teams"][0]))
    assert team["percent_ppa"] is None
    assert team["total_ppa"] == 120.5, "the absolute figure is the point of keeping it"


def test_a_database_without_the_table_exports_nothing():
    assert ex.build_returning_production(sqlite3.connect(":memory:")) == {}


def test_only_the_seasons_measured_cost_a_reader_a_request(tmp_path):
    conn = db()
    lrp.load_file(conn, archive(tmp_path, row("Alabama")))
    files = ex.write_returning_production(tmp_path, built(conn))
    assert list(files) == ["2024"]
    assert files["2024"].startswith("data/returning/2024.js?v=")
    written = (tmp_path / "returning" / "2024.js").read_text(encoding="utf-8")
    assert written.startswith("(window.__CFB_RETURNING__=window.__CFB_RETURNING__||{})[2024]=")
    # Nothing at all means no directory of orphans either.
    assert ex.write_returning_production(tmp_path, {}) == {}
    assert not (tmp_path / "returning").exists()


def test_the_archive_loader_walks_every_committed_season(tmp_path):
    conn = db()
    archive(tmp_path, row("Alabama"), season=2024)
    archive(tmp_path, row("Alabama", season=2025), season=2025)
    assert lrp.load_archive(conn, tmp_path) == (2, 2)
