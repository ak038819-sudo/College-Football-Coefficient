"""
The AP/CFP backfill fetcher.

`poll_rankings` held 325 rows, all of 2026, because the only thing that ever
fetched a poll was `src/fetch_cfbd_games.py`, as a side effect of fetching that
season's games. CFBD serves the AP poll back to 1936.

Nothing downstream changes: `run_pipeline.py` already globs every
`data/raw/rankings_*.csv` and `src/elo_timeline.py` already builds a per-season
poll timeline. So the fetcher IS the feature, and these are its guards.
"""
import csv
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import fetch_cfbd_rankings as fr  # noqa: E402
import load_rankings  # noqa: E402


def payload(week=1, season_type="regular", polls=None):
    return [{"season": 1950, "seasonType": season_type, "week": week,
             "polls": polls if polls is not None else [
                 {"poll": "AP Top 25", "ranks": [
                     {"rank": 1, "school": "Oklahoma", "conference": "Big Seven",
                      "firstPlaceVotes": 40, "points": 1672},
                     {"rank": 2, "school": "Army", "conference": "Independent",
                      "firstPlaceVotes": None, "points": 1597}]}]}]


@pytest.fixture
def api(monkeypatch):
    """Stand in for CFBD. Returns the list of (year, seasonType) asked for."""
    calls, answers = [], {}

    def fake_get_json(url, params=None, **kwargs):
        calls.append((params["year"], params["seasonType"]))
        return answers.get(params["seasonType"], [])

    monkeypatch.setattr(fr.cfbd_http, "get_json", fake_get_json)
    return calls, answers


# ---- what it writes -------------------------------------------------------

def test_a_season_is_written_in_the_columns_the_loader_reads(tmp_path, api):
    calls, answers = api
    answers["regular"] = payload()
    assert fr.write_season(1950, {}, tmp_path) == 2
    rows = list(csv.DictReader((tmp_path / "rankings_1950.csv").open(encoding="utf-8")))
    assert list(rows[0]) == fr.CSV_COLUMNS
    assert rows[0]["school"] == "Oklahoma" and rows[0]["poll"] == "ap"
    assert rows[0]["season_year"] == "1950" and rows[0]["week"] == "1"
    assert calls == [(1950, "regular"), (1950, "postseason")], "both season types, always"


def test_the_columns_are_the_ones_the_loader_actually_wants():
    # The games fetcher writes this same file for the current season, and
    # load_rankings reads both. A column added to one and not the other would
    # mean a 1959 row and a 2026 row are not the same kind of row.
    header = (REPO / "data" / "raw" / "rankings_2026.csv").read_text(
        encoding="utf-8").splitlines()[0]
    assert header.split(",") == fr.CSV_COLUMNS


def test_a_blank_vote_count_stays_blank_and_never_becomes_zero(tmp_path, api):
    # The CFP poll reports no first-place votes at all. A 0 there would be a
    # claim that nobody voted a team first.
    _, answers = api
    answers["regular"] = payload()
    fr.write_season(1950, {}, tmp_path)
    rows = list(csv.DictReader((tmp_path / "rankings_1950.csv").open(encoding="utf-8")))
    assert rows[0]["first_place_votes"] == "40"
    assert rows[1]["first_place_votes"] == "", "not reported is not zero"


def test_a_season_with_no_release_writes_no_file_at_all(tmp_path, api):
    # An empty CSV would make load_rankings DELETE that season's rows and
    # write none, which is how real data gets replaced with an absence.
    _, answers = api
    assert fr.write_season(1941, {}, tmp_path) == 0
    assert not (tmp_path / "rankings_1941.csv").exists()


def test_a_poll_with_no_ranks_does_not_make_a_file_either(tmp_path, api):
    _, answers = api
    answers["regular"] = payload(polls=[{"poll": "AP Top 25", "ranks": []}])
    assert fr.write_season(1941, {}, tmp_path) == 0
    assert not (tmp_path / "rankings_1941.csv").exists()


def test_both_season_types_land_in_one_file(tmp_path, api):
    _, answers = api
    answers["regular"] = payload(week=15)
    answers["postseason"] = payload(week=1, season_type="postseason")
    fr.write_season(1950, {}, tmp_path)
    rows = list(csv.DictReader((tmp_path / "rankings_1950.csv").open(encoding="utf-8")))
    assert {r["season_type"] for r in rows} == {"regular", "postseason"}


# ---- resumability ---------------------------------------------------------

def test_a_season_already_committed_is_skipped_so_a_run_can_resume(tmp_path, api):
    calls, answers = api
    answers["regular"] = payload()
    fr.write_season(1950, {}, tmp_path)
    before = len(calls)
    assert fr.write_season(1950, {}, tmp_path) == 0
    assert len(calls) == before, "a skipped season must not cost a request either"


def test_force_refetches_a_season_that_is_already_there(tmp_path, api):
    calls, answers = api
    answers["regular"] = payload()
    fr.write_season(1950, {}, tmp_path)
    assert fr.write_season(1950, {}, tmp_path, force=True) == 2
    assert len(calls) == 4


# ---- the range ------------------------------------------------------------

def test_a_year_before_the_poll_existed_is_refused_rather_than_requested(capsys):
    assert fr.main(["1900", "1910"]) == 2
    assert "1936" in capsys.readouterr().out


def test_a_missing_key_is_refused_before_any_request(monkeypatch, capsys):
    monkeypatch.delenv("CFBD_API_KEY", raising=False)
    monkeypatch.delenv("COLLEGEFOOTBALLDATA_API_KEY", raising=False)
    assert fr.main(["1950"]) == 2
    assert "CFBD_API_KEY" in capsys.readouterr().out


def test_a_range_walks_every_season_in_it(tmp_path, monkeypatch, api):
    calls, answers = api
    answers["regular"] = payload()
    monkeypatch.setenv("CFBD_API_KEY", "x")
    assert fr.main(["1950", "1952", "--out-dir", str(tmp_path)]) == 0
    assert sorted({year for year, _ in calls}) == [1950, 1951, 1952]
    assert sorted(p.name for p in tmp_path.glob("*.csv")) == \
        ["rankings_1950.csv", "rankings_1951.csv", "rankings_1952.csv"]


def test_probe_writes_nothing(tmp_path, monkeypatch, api):
    _, answers = api
    answers["regular"] = payload()
    monkeypatch.setenv("CFBD_API_KEY", "x")
    assert fr.main(["1950", "--probe", "--out-dir", str(tmp_path)]) == 0
    assert list(tmp_path.glob("*.csv")) == []


# ---- what it hands the loader ---------------------------------------------

def test_a_fetched_season_loads_without_a_change_to_the_loader(tmp_path, api):
    """The point of reusing the games fetcher's own row builder: a 1950 file
    goes into poll_rankings through exactly the path a 2026 file does."""
    import sqlite3
    _, answers = api
    answers["regular"] = payload()
    fr.write_season(1950, {}, tmp_path)
    conn = sqlite3.connect(":memory:")
    conn.executescript("CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);"
                       "CREATE TABLE team_aliases (alias TEXT, team_name TEXT);")
    conn.execute("INSERT INTO teams VALUES (1, 'Oklahoma')")
    load_rankings.load_rankings(conn, str(tmp_path / "rankings_1950.csv"))
    stored = conn.execute("SELECT season_year, poll, rank, school, team_id, first_place_votes "
                          "FROM poll_rankings ORDER BY rank").fetchall()
    assert stored[0] == (1950, "ap", 1, "Oklahoma", 1, 40)
    assert stored[1][4] is None, "an unmatched school keeps its name and a NULL team_id"
    assert stored[1][5] is None, "and its blank vote count stays blank"
