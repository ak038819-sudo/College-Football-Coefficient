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
import build_awards_payload as payloads  # noqa: E402
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
                         "by_surname": 0, "coach_awards": 0, "outside": 0, "weekly": 0,
                         "unresolved": 0, "no_winner": 0,
                         "uncategorised": ["Heisman Trophy"]}
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
        # Named from the table, not from the word "coach": not one of ESPN's
        # four coach awards has that word in its name, which is why the pattern
        # this replaced matched none of them and reported every coach as a
        # player the project had failed to identify.
        conn = _people_db(tmp_path)
        award = {"espn_award_id": "9", "name": "The Home Depot Award",
                 "winners": [{"display_name": "Kalen DeBoer", "school": "Colorado",
                              "espn_athlete_id": None, "espn_team_id": "38"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [award]))
        assert stats["coach_awards"] == 1 and stats["unresolved"] == 0
        assert conn.execute("SELECT resolution FROM player_awards").fetchone()[0] == "coach award"
        assert conn.execute("SELECT COUNT(*) FROM person_unresolved").fetchone()[0] == 0
        assert conn.execute("SELECT category FROM awards").fetchone()[0] == "coach"

    def test_each_of_the_four_coach_awards_is_one(self, tmp_path):
        for name in ("The Home Depot Award", "Eddie Robinson Award",
                     "Frank Broyles Award", "Buddy Teevens Award"):
            assert load.AWARD_CATEGORY[load.award_key(name)] == "coach", name

    def test_a_division_two_winner_is_not_counted_as_one_we_failed_to_find(self, tmp_path):
        # The Harlon Hill goes to a Division II player. He is not in an FBS
        # roster archive and never will be, so counting him among the winners
        # this project could not identify would make the gap look like a defect.
        conn = _people_db(tmp_path)
        award = {"espn_award_id": "7", "name": "Harlon Hill Trophy",
                 "winners": [{"display_name": "Hank Goff", "school": "Ferris State",
                              "espn_athlete_id": "1", "espn_team_id": "9"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2015, [award]))
        assert stats["outside"] == 1 and stats["unresolved"] == 0
        assert conn.execute("SELECT resolution FROM player_awards").fetchone()[0] \
            == "outside this archive"

    def test_a_weekly_honour_is_kept_and_not_counted_as_a_season_award(self, tmp_path):
        conn = _people_db(tmp_path)
        award = {"espn_award_id": "8", "name": "Offensive Player of the Week",
                 "winners": [{"display_name": "Nobody At All", "school": "Colorado",
                              "espn_athlete_id": None, "espn_team_id": "38"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [award]))
        assert stats["weekly"] == 1 and stats["unresolved"] == 0
        assert conn.execute("SELECT COUNT(*) FROM player_awards").fetchone()[0] == 1

    def test_a_trophy_espn_adds_later_is_named_in_the_run(self, tmp_path):
        conn = _people_db(tmp_path)
        award = {"espn_award_id": "99", "name": "Brand New Trophy",
                 "winners": [{"display_name": "Travis Hunter", "school": "Colorado",
                              "espn_athlete_id": "4426339", "espn_team_id": "38"}]}
        stats = load.load_awards(conn, _season_file(tmp_path, 2024, [award]))
        # Treated as a player award so a real new trophy still reaches the page,
        # and named so it shows up in the log rather than silently.
        assert stats["uncategorised"] == ["Brand New Trophy"]
        assert conn.execute("SELECT category FROM awards").fetchone()[0] == "player"


class TestPositionIsATieBreakerAndNotAFilter:
    """The bug that cost 86 winners, all of them defenders and linemen.

    ESPN and the roster feed draw on one vocabulary and still disagree per
    player: Manti Te'o is a LB to one and an ILB to the other, Aaron Donald a
    DT and a DL, Jahdae Barron a CB and a DB. Matching the strings exactly
    threw away the only candidate; the offensive skill positions resolved
    because those are the words the two feeds happen to spell the same way.
    """

    def test_a_linebacker_the_roster_calls_an_inside_linebacker_still_resolves(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 517633, "Manti Te'o", 1, 2012, "ILB")
        stats = load.load_awards(conn, _season_file(tmp_path, 2012, [
            _heisman(name="Manti Te'o", athlete="1", position="LB")]))
        assert stats["linked"] == 1, "a LB and an ILB are the same player"
        assert conn.execute("SELECT player_id FROM player_awards").fetchone()[0] == 517633

    @pytest.mark.parametrize("espn_position,roster_position", [
        ("DT", "DL"), ("DE", "DL"), ("EDGE", "DL"), ("EDGE", "OLB"),
        ("CB", "DB"), ("S", "DB"), ("C", "OL"), ("OT", "OL"), ("PK", "P"),
    ])
    def test_two_words_for_one_part_of_a_roster_are_compatible(self, espn_position,
                                                               roster_position):
        assert load.positions_compatible(espn_position, roster_position)

    def test_a_quarterback_and_a_cornerback_are_not_compatible(self):
        assert not load.positions_compatible("QB", "CB")
        assert not load.positions_compatible("OL", "DL")

    def test_a_position_nobody_recorded_rules_nobody_out(self):
        # ESPN writes a position it does not know as a dash.
        assert load.positions_compatible("-", "DL")
        assert load.positions_compatible(None, "QB")
        assert load.positions_compatible("QB", None)
        assert load.positions_compatible("ATH", "WR")

    def test_a_position_still_separates_two_players_of_one_name(self, tmp_path):
        # The tie-break has to do its job, or widening the first query would
        # trade 86 found winners for a crop of wrong ones.
        conn = _people_db(tmp_path)
        _roster(conn, 20, "Jordan Davis", 1, 2021, "DL")
        _roster(conn, 21, "Jordan Davis", 1, 2021, "WR")
        stats = load.load_awards(conn, _season_file(tmp_path, 2021, [
            _heisman(name="Jordan Davis", athlete="2", position="DT")]))
        assert stats["linked"] == 1
        assert conn.execute("SELECT player_id FROM player_awards").fetchone()[0] == 20

    def test_two_players_of_one_name_in_one_position_stay_two_people(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 30, "D'Onta Foreman", 1, 2016, "RB")
        _roster(conn, 31, "D'Onta Foreman", 1, 2016, "RB")
        stats = load.load_awards(conn, _season_file(tmp_path, 2016, [
            _heisman(name="D'Onta Foreman", athlete="3", position="RB")]))
        assert stats["linked"] == 0
        assert conn.execute("SELECT resolution FROM player_awards").fetchone()[0] \
            == "name collision"


class TestTheNameHeIsAnnouncedBy:
    def test_a_surname_resolves_a_winner_the_roster_enrolled_differently(self, tmp_path):
        # ESPN: "Olu Oluwatimi" won the 2022 Rimington. The roster that season
        # says Olusegun.
        conn = _people_db(tmp_path)
        _roster(conn, 4370807, "Olusegun Oluwatimi", 1, 2022, "OL")
        stats = load.load_awards(conn, _season_file(tmp_path, 2022, [
            _heisman(name="Olu Oluwatimi", athlete="4", position="C")]))
        assert stats["linked"] == 1 and stats["by_surname"] == 1
        row = conn.execute("SELECT player_id, resolution FROM player_awards").fetchone()
        assert row == (4370807, "surname")

    def test_a_surname_two_players_share_resolves_neither(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 40, "Olusegun Oluwatimi", 1, 2022, "OL")
        _roster(conn, 41, "Chidi Oluwatimi", 1, 2022, "OT")
        stats = load.load_awards(conn, _season_file(tmp_path, 2022, [
            _heisman(name="Olu Oluwatimi", athlete="5", position="C")]))
        assert stats["linked"] == 0
        assert conn.execute("SELECT resolution FROM player_awards").fetchone()[0] \
            == "name collision"

    def test_a_surname_nobody_on_that_roster_has_resolves_nobody(self, tmp_path):
        conn = _people_db(tmp_path)
        _roster(conn, 50, "Somebody Else", 1, 2022, "OL")
        stats = load.load_awards(conn, _season_file(tmp_path, 2022, [
            _heisman(name="Olu Oluwatimi", athlete="6", position="C")]))
        assert stats["linked"] == 0
        assert conn.execute("SELECT resolution FROM player_awards").fetchone()[0] == "no match"

    def test_a_full_name_is_preferred_to_a_surname(self, tmp_path):
        # Two Oluwatimis, one of whom the full name names exactly: the full
        # name wins outright and the surname rule is never reached.
        conn = _people_db(tmp_path)
        _roster(conn, 60, "Olu Oluwatimi", 1, 2022, "OL")
        _roster(conn, 61, "Chidi Oluwatimi", 1, 2022, "OL")
        stats = load.load_awards(conn, _season_file(tmp_path, 2022, [
            _heisman(name="Olu Oluwatimi", athlete="7", position="C")]))
        assert stats["linked"] == 1 and stats["by_surname"] == 0
        row = conn.execute("SELECT player_id, resolution FROM player_awards").fetchone()
        assert row == (60, "context")

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


# --- the payload the pages read --------------------------------------------

class TestThePayloadThePagesRead:
    def _loaded(self, tmp_path, season, awards):
        conn = _people_db(tmp_path)
        _roster(conn, 4426339, "Travis Hunter", 1, season, "WR")
        load.load_awards(conn, _season_file(tmp_path, season, awards))
        return conn

    def test_a_trophy_and_its_winner_are_keyed_both_ways(self, tmp_path):
        conn = self._loaded(tmp_path, 2024, [_heisman()])
        payload = payloads.build_payload(conn)
        assert [a["name"] for a in payload["awards"]] == ["Heisman Trophy"]
        assert payload["winners"][0]["name"] == "Travis Hunter"
        # The player index points AT the winner row rather than repeating it,
        # so a player page and the awards view cannot disagree about a trophy.
        [index] = payload["by_player"]["4426339"]
        assert payload["winners"][index]["key"] == "heisman-trophy"

    def test_a_winner_with_no_page_carries_no_player(self, tmp_path):
        conn = _people_db(tmp_path)
        load.load_awards(conn, _season_file(tmp_path, 1997, [
            _heisman(name="Charles Woodson", school="Colorado")]))
        payload = payloads.build_payload(conn)
        assert "player" not in payload["winners"][0]
        assert payload["winners"][0]["name"] == "Charles Woodson"
        assert payload["by_player"] == {}

    def test_the_school_is_this_projects_own_name_where_one_was_resolved(self, tmp_path):
        # So a link and the text on it agree. ESPN's own string is the fallback
        # and not the preference, which only shows where the two differ.
        conn = _people_db(tmp_path)
        conn.execute("INSERT INTO team_aliases VALUES ('Colo. Buffaloes', 'Colorado')")
        _roster(conn, 4426339, "Travis Hunter", 1, 2024, "WR")
        load.load_awards(conn, _season_file(tmp_path, 2024, [
            _heisman(school="Colo. Buffaloes")]))
        winner = payloads.build_payload(conn)["winners"][0]
        assert winner["team"] == 1
        assert winner["school"] == "Colorado", "the label must match the link"

    def test_a_school_outside_this_project_keeps_the_sources_own_string(self, tmp_path):
        conn = _people_db(tmp_path)
        load.load_awards(conn, _season_file(tmp_path, 2024, [
            _heisman(name="Someone", school="North Dakota State")]))
        winner = payloads.build_payload(conn)["winners"][0]
        assert winner["school"] == "North Dakota State" and "team" not in winner

    def test_a_dash_is_not_a_position(self, tmp_path):
        conn = self._loaded(tmp_path, 2024, [_heisman(position="-")])
        assert "position" not in payloads.build_payload(conn)["winners"][0]

    def test_the_non_fbs_and_weekly_trophies_are_held_back_from_the_pages(self, tmp_path):
        conn = _people_db(tmp_path)
        load.load_awards(conn, _season_file(tmp_path, 2024, [
            _heisman(),
            {"espn_award_id": "7", "name": "Harlon Hill Trophy",
             "winners": [{"display_name": "A Winner", "school": "Ferris State"}]},
            {"espn_award_id": "8", "name": "Offensive Player of the Week",
             "winners": [{"display_name": "Another", "school": "Colorado"}]},
            {"espn_award_id": "9", "name": "The Home Depot Award",
             "winners": [{"display_name": "A Coach", "school": "Colorado"}]}]))
        payload = payloads.build_payload(conn)
        # The coach award is shown, because a reader looking for the national
        # coach of the year expects to find it. The Division II trophy and the
        # weekly honour are not: their winners can never have a page here.
        assert sorted(a["name"] for a in payload["awards"]) == \
            ["Heisman Trophy", "The Home Depot Award"]
        assert sorted(w["name"] for w in payload["winners"]) == ["A Coach", "Travis Hunter"]
        # Still in the database, though: the archive does not pretend they
        # were never given.
        assert conn.execute("SELECT COUNT(*) FROM player_awards").fetchone()[0] == 4

    def test_a_database_with_no_award_tables_is_not_a_crash(self, tmp_path):
        # The static export runs against a database built before these tables
        # existed, and still has to produce a dashboard.
        conn = sqlite3.connect(str(tmp_path / "bare.db"))
        assert payloads.build_payload(conn) == {"awards": [], "winners": [], "by_player": {}}
