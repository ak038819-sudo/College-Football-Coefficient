"""Award winners: the fetch's shape, and the identity work in the loader.

No test here touches the network. What they pin down:

  * the fetcher's parsing, driven by a recorded ESPN response rather than a
    live one -- which ids it pulls out of a `$ref`, and that a winner whose
    athlete record will not load is still recorded as a winner;
  * that a season ESPN does not carry writes NO file, because an empty file
    would say "no awards were given", which is a different claim;
  * the loader's resolution order: an ESPN id this project has already linked
    beats a name, and a name is only accepted when exactly one person on that
    roster answers to it;
  * that nothing is ever created from an award. A winner with no roster row
    stays a name, and the row says why;
  * that a coach-of-the-year award is not reported as a failed player match.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import fetch_espn_awards as fetch  # noqa: E402
import load_player_awards as load  # noqa: E402
import person_identity as pid  # noqa: E402

ATHLETES = "https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/athletes"
TEAMS = "https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/teams"


class FakeEspn:
    """ESPN's three endpoint shapes, recorded rather than called.

    Keyed by the path so a test can assert WHICH requests were made: the
    athlete and team records are fetched once each per run however many awards
    a person wins, and a test that did not check that would not notice the
    cache going away.
    """

    def __init__(self, index=None, awards=None, athletes=None, teams=None, fail=()):
        self.index = index if index is not None else {
            "pageCount": 1,
            "items": [{"$ref": "https://e/awards/1"}, {"$ref": "https://e/awards/2"}],
        }
        self.awards = awards if awards is not None else {
            "https://e/awards/1": {
                "id": 1, "name": "Heisman Trophy", "description": "Most outstanding player",
                "winners": [{"athlete": {"$ref": f"{ATHLETES}/4426339?lang=en"},
                             "team": {"$ref": f"{TEAMS}/38?lang=en"}}]},
            "https://e/awards/2": {
                "id": 2, "name": "Maxwell Award", "description": "Player of the year",
                "winners": [{"athlete": {"$ref": f"{ATHLETES}/4426339?lang=en"},
                             "team": {"$ref": f"{TEAMS}/38?lang=en"}}]},
        }
        self.athletes = athletes if athletes is not None else {
            f"{ATHLETES}/4426339": {"displayName": "Travis Hunter", "firstName": "Travis",
                                    "lastName": "Hunter", "jersey": "12",
                                    "position": {"abbreviation": "WR"}},
        }
        self.teams = teams if teams is not None else {
            f"{TEAMS}/38": {"location": "Colorado", "displayName": "Colorado Buffaloes",
                            "abbreviation": "COLO"},
        }
        self.fail = set(fail)
        self.calls = []

    def __call__(self, url, params=None, describe=None):
        self.calls.append(url)
        bare = url.split("?", 1)[0]
        if bare in self.fail:
            raise RuntimeError("ESPN said no")
        if bare.endswith("/awards"):
            return self.index
        for table in (self.awards, self.athletes, self.teams):
            if bare in table:
                return table[bare]
        raise AssertionError("unexpected request: " + url)


@pytest.fixture
def espn(monkeypatch):
    fake = FakeEspn()
    monkeypatch.setattr(fetch, "get", fake)
    return fake


class TestReadingEspnsShape:
    def test_an_id_is_taken_from_the_end_of_a_ref_path(self):
        assert fetch.ref_id(f"{ATHLETES}/4426339?lang=en&region=us") == "4426339"
        assert fetch.ref_id(f"{TEAMS}/38/") == "38"
        # Not a number, not an id: better None than a word in an id column.
        assert fetch.ref_id(f"{ATHLETES}/unknown") is None
        assert fetch.ref_id(None) is None
        assert fetch.ref_id(12) is None

    def test_a_ref_is_only_read_out_of_an_object_that_has_one(self):
        assert fetch.ref_of({"$ref": "https://e/x"}) == "https://e/x"
        assert fetch.ref_of({}) is None
        assert fetch.ref_of(None) is None
        assert fetch.ref_of("https://e/x") is None

    def test_a_season_is_flattened_to_awards_and_named_winners(self, espn):
        payload = fetch.fetch_season(2024)
        assert payload["season"] == 2024 and payload["source"] == "espn"
        assert [a["name"] for a in payload["awards"]] == ["Heisman Trophy", "Maxwell Award"]
        winner = payload["awards"][0]["winners"][0]
        assert winner["display_name"] == "Travis Hunter"
        assert winner["espn_athlete_id"] == "4426339"
        assert winner["espn_team_id"] == "38"
        assert winner["position"] == "WR"
        # The SCHOOL, not the display name: it is what the project's team
        # aliases resolve, and 'Colorado Buffaloes' is not in them.
        assert winner["school"] == "Colorado"

    def test_one_athlete_winning_two_awards_is_fetched_once(self, espn):
        fetch.fetch_season(2024)
        athlete_calls = [u for u in espn.calls if "/athletes/" in u]
        assert len(athlete_calls) == 1, athlete_calls
        team_calls = [u for u in espn.calls if "/teams/" in u]
        assert len(team_calls) == 1, team_calls

    def test_a_winner_whose_athlete_record_fails_is_still_a_winner(self, monkeypatch):
        fake = FakeEspn(fail={f"{ATHLETES}/4426339"})
        monkeypatch.setattr(fetch, "get", fake)
        payload = fetch.fetch_season(2024)
        winner = payload["awards"][0]["winners"][0]
        # The award happened and the id is known; only the name is not.
        assert winner["espn_athlete_id"] == "4426339"
        assert winner["display_name"] is None
        assert winner["school"] == "Colorado"

    def test_an_award_with_no_winner_is_kept_with_an_empty_list(self, monkeypatch):
        fake = FakeEspn(awards={
            "https://e/awards/1": {"id": 1, "name": "Heisman Trophy", "winners": []},
            "https://e/awards/2": {"id": 2, "name": "Maxwell Award"},
        })
        monkeypatch.setattr(fetch, "get", fake)
        payload = fetch.fetch_season(2024)
        assert [a["winners"] for a in payload["awards"]] == [[], []]

    def test_paging_is_followed(self, monkeypatch):
        fake = FakeEspn(index={"pageCount": 2, "items": [{"$ref": "https://e/awards/1"}]})
        # Page two returns the second award. The fake answers any /awards call
        # with the same index, so the loop must ASK for page two for the second
        # award to appear at all.
        pages = {1: {"pageCount": 2, "items": [{"$ref": "https://e/awards/1"}]},
                 2: {"pageCount": 2, "items": [{"$ref": "https://e/awards/2"}]}}

        def get(url, params=None, describe=None):
            if url.split("?", 1)[0].endswith("/awards"):
                return pages[(params or {}).get("page", 1)]
            return fake(url, params, describe)

        monkeypatch.setattr(fetch, "get", get)
        payload = fetch.fetch_season(2024)
        assert [a["name"] for a in payload["awards"]] == ["Heisman Trophy", "Maxwell Award"]

    def test_a_season_espn_does_not_carry_writes_no_file(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(fetch, "get", FakeEspn(index={"pageCount": 1, "items": []}))
        monkeypatch.setattr(fetch, "OUT_DIR", tmp_path / "awards")
        assert fetch.fetch_season(1946) is None
        assert fetch.main(["1946"]) == 0
        # No file at all: an empty one would read as "no awards were given".
        assert not (tmp_path / "awards").exists() or not list((tmp_path / "awards").iterdir())
        assert "no award index" in capsys.readouterr().out

    def test_the_archive_is_written_as_readable_gzipped_json(self, espn, tmp_path):
        payload = fetch.fetch_season(2024)
        path = fetch.write_season(payload, tmp_path)
        assert path.name == "2024.json.gz"
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            assert json.load(fh) == payload


# --- the loader -------------------------------------------------------------

def _people_db(tmp_path) -> sqlite3.Connection:
    """Two seasons of a tiny roster, through the project's own person tables."""
    conn = sqlite3.connect(str(tmp_path / "league.db"))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
        -- The alias table the project's team resolution reads; empty here, so a
        -- school resolves by its own name or not at all.
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT NOT NULL);
        INSERT INTO teams VALUES (1, 'Colorado'), (2, 'Ohio State');
    """)
    pid.apply_schema(conn)
    load.apply_schema(conn)
    return conn


def _roster(conn, player_id, name, team_id, season, position=None, cfbd_id=None):
    pid.create_player(conn, name, None, None, position, player_id=player_id)
    pid.add_alias(conn, player_id, name)
    if cfbd_id is not None:
        pid.link_external_id(conn, player_id, "cfbd", str(cfbd_id), "id")
    conn.execute(
        "INSERT OR REPLACE INTO player_team_seasons (player_id, season_year, team_id,"
        " position, source) VALUES (?, ?, ?, ?, 'roster')",
        (player_id, season, team_id, position))
    conn.commit()


def _season_file(tmp_path, season, awards) -> Path:
    path = tmp_path / f"{season}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump({"season": season, "source": "espn", "awards": awards}, fh)
    return path


HEISMAN = {"espn_award_id": "1", "name": "Heisman Trophy",
           "description": "The most outstanding player"}


def _heisman(name="Travis Hunter", school="Colorado", athlete="4426339", position="WR"):
    return {**HEISMAN, "winners": [{"display_name": name, "school": school,
                                    "espn_athlete_id": athlete, "espn_team_id": "38",
                                    "position": position}]}


class TestResolvingAWinnerToAPerson:
    def test_a_name_team_and_season_that_match_one_person_link_them(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 4426339, "Travis Hunter", 1, 2024, "WR")
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [_heisman()]))
        assert stats == {"awards": 1, "winners": 1, "linked": 1, "by_espn_id": 0,
                         "coach_awards": 0, "unresolved": 0, "no_winner": 0}
        row = conn.execute("SELECT player_id, team_id, resolution FROM player_awards").fetchone()
        assert row == (4426339, 1, "context")

    def test_the_contextual_match_links_the_espn_id_for_next_time(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 77, "Travis Hunter", 1, 2024, "WR")
        load.load_awards(conn, _season_file(tmp_path, 2024, [_heisman()]))
        assert pid.find_by_external_id(conn, "espn", "4426339") == 77
        # So a later season resolves by id even where the roster would not: the
        # award names no position here and the roster row is for another year.
        later = _heisman(name="T. Hunter", school="Colorado", position=None)
        load.load_awards(conn, _season_file(tmp_path, 2025, [later]))
        row = conn.execute("SELECT player_id, resolution FROM player_awards "
                           "WHERE season_year = 2025").fetchone()
        assert row == (77, "espn id")

    def test_two_players_of_one_name_on_one_roster_stay_two_people(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 10, "Travis Hunter", 1, 2024)
        _roster(conn, 11, "Travis Hunter", 1, 2024)
        stats = load.load_awards(conn, _season_file(
            tmp_path, 2024, [_heisman(position=None)]))
        assert stats["linked"] == 0 and stats["unresolved"] == 1
        row = conn.execute("SELECT player_id, resolution FROM player_awards").fetchone()
        assert row == (None, "name collision")
        # And it is on the review list, not silently dropped.
        assert conn.execute("SELECT COUNT(*) FROM person_unresolved WHERE context = 'awards'"
                            ).fetchone()[0] == 1

    def test_a_winner_with_no_roster_row_stays_a_name(self, tmp_path):
        # Every winner before 2009, where the roster archive begins.
        conn = _people_db(tmp_path)
        stats = load.load_awards(conn, _season_file(
            tmp_path, 1997, [_heisman(name="Charles Woodson", school="Colorado")]))
        assert stats["linked"] == 0 and stats["unresolved"] == 1
        row = conn.execute("SELECT winner_name, player_id, team_id, resolution "
                           "FROM player_awards").fetchone()
        assert row == ("Charles Woodson", None, 1, "no match")
        # Nothing was invented to hold the award.
        assert conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 0

    def test_a_school_this_project_does_not_carry_is_not_a_crash(self, tmp_path):
        conn = _people_db(tmp_path)
        stats = load.load_awards(conn, _season_file(
            tmp_path, 2024, [_heisman(name="Someone", school="North Dakota State")]))
        assert stats["unresolved"] == 1
        row = conn.execute("SELECT team_id, player_id, resolution FROM player_awards").fetchone()
        assert row == (None, None, "unknown team")

    def test_a_coach_award_is_not_reported_as_a_failed_player_match(self, tmp_path):
        conn = _people_db(tmp_path)
        award = {"espn_award_id": "9", "name": "Coach of the Year",
                 "winners": [{"display_name": "Kalen DeBoer", "school": "Colorado",
                              "espn_athlete_id": None, "espn_team_id": "38"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [award]))
        assert stats["coach_awards"] == 1 and stats["unresolved"] == 0
        assert conn.execute("SELECT resolution FROM player_awards").fetchone()[0] == "coach award"
        assert conn.execute("SELECT COUNT(*) FROM person_unresolved").fetchone()[0] == 0

    def test_a_winner_the_source_did_not_name_is_recorded_as_such(self, tmp_path):
        conn = _people_db(tmp_path)
        award = {**HEISMAN, "winners": [{"display_name": None, "school": "Colorado",
                                         "espn_athlete_id": "4426339", "espn_team_id": "38"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [award]))
        assert stats["unresolved"] == 1
        row = conn.execute("SELECT winner_name, resolution FROM player_awards").fetchone()
        assert row == ("(unnamed)", "no name from the source")

    def test_co_winners_are_both_kept(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 10, "One Player", 1, 2024)
        _roster(conn, 11, "Two Player", 2, 2024)
        award = {**HEISMAN, "winners": [
            {"display_name": "One Player", "school": "Colorado",
             "espn_athlete_id": "1", "espn_team_id": "38"},
            {"display_name": "Two Player", "school": "Ohio State",
             "espn_athlete_id": "2", "espn_team_id": "194"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [award]))
        assert stats["winners"] == 2 and stats["linked"] == 2
        assert conn.execute("SELECT COUNT(*) FROM player_awards").fetchone()[0] == 2


class TestTheAwardsTable:
    def test_a_key_is_derived_from_the_name_not_from_espns_id(self):
        assert load.award_key("Heisman Trophy") == "heisman-trophy"
        assert load.award_key("Davey O'Brien Award") == "davey-o-brien-award"
        assert load.award_key("  Mixed   Case / Slashes ") == "mixed-case-slashes"
        assert load.award_key("") == ""

    def test_an_award_seen_in_several_seasons_is_one_row_spanning_them(self, tmp_path):
        conn = _people_db(tmp_path)
        for season in (2015, 2024, 2019):
            load.load_awards(conn, _season_file(tmp_path, season, [_heisman()]))
        rows = conn.execute("SELECT award_key, first_season, last_season FROM awards").fetchall()
        assert rows == [("heisman-trophy", 2015, 2024)]

    def test_an_award_with_no_name_is_skipped_rather_than_keyed_as_blank(self, tmp_path):
        conn = _people_db(tmp_path)
        stats = load.load_awards(conn, _season_file(
            tmp_path, 2024, [{"espn_award_id": "1", "name": None, "winners": []}]))
        assert stats["awards"] == 0
        assert conn.execute("SELECT COUNT(*) FROM awards").fetchone()[0] == 0

    def test_an_award_with_no_winner_is_counted_but_adds_no_winner_row(self, tmp_path):
        conn = _people_db(tmp_path)
        stats = load.load_awards(conn, _season_file(
            tmp_path, 2026, [{**HEISMAN, "winners": []}]))
        assert stats["awards"] == 1 and stats["no_winner"] == 1 and stats["winners"] == 0
        assert conn.execute("SELECT COUNT(*) FROM player_awards").fetchone()[0] == 0
        # The award itself is still known, so the page can say it has no winner
        # yet rather than that the award does not exist.
        assert conn.execute("SELECT name FROM awards").fetchone()[0] == "Heisman Trophy"

    def test_reloading_a_season_replaces_it_rather_than_doubling_it(self, tmp_path):
        conn = _people_db(tmp_path)
        load.load_awards(conn, _season_file(tmp_path, 2024, [_heisman(name="Wrong Name")]))
        load.load_awards(conn, _season_file(tmp_path, 2024, [_heisman(name="Travis Hunter")]))
        rows = conn.execute("SELECT winner_name FROM player_awards").fetchall()
        assert rows == [("Travis Hunter",)]
