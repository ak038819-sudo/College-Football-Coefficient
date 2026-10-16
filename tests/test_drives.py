"""
The drive archive: CFBD's payload in, home-perspective rows out.

Two things depend on these rows being right, and they fail differently. A game
page's scoring summary would show the wrong team ahead; the live
win-probability fit would be measured against states that never happened. So
the tests here concentrate on the two conversions the loader does rather than
on the copying: offense/defense scores into home/away, and a period and clock
into regulation seconds remaining.
"""
from __future__ import annotations

import csv
import gzip
import sqlite3

import pytest

import fetch_cfbd_drives as fd
from load_drives import home_away, load_drives, regulation_seconds_remaining

# Shaped like CFBD's /drives: camelCase, a clock object, running scores from the
# offense's point of view. The second drive is the away team's, which is where
# the home/away conversion earns its place.
PAYLOAD = [
    {"gameId": 101, "id": 10101, "driveNumber": 1, "offense": "Alpha", "defense": "Bravo",
     "isHomeOffense": True, "scoring": True, "driveResult": "TD", "week": 3, "season": 2024,
     "startPeriod": 1, "startTime": {"minutes": 15, "seconds": 0},
     "endPeriod": 1, "endTime": {"minutes": 11, "seconds": 30},
     "startYardsToGoal": 75, "endYardsToGoal": 0, "plays": 8, "yards": 75,
     "startOffenseScore": 0, "startDefenseScore": 0,
     "endOffenseScore": 7, "endDefenseScore": 0},
    {"gameId": 101, "id": 10102, "driveNumber": 2, "offense": "Bravo Alias", "defense": "Alpha",
     "isHomeOffense": False, "scoring": False, "driveResult": "PUNT", "week": 3, "season": 2024,
     "startPeriod": 1, "startTime": {"minutes": 11, "seconds": 30},
     "endPeriod": 2, "endTime": {"minutes": 14, "seconds": 0},
     "startYardsToGoal": 80, "endYardsToGoal": 65, "plays": 4, "yards": 15,
     "startOffenseScore": 0, "startDefenseScore": 7,
     "endOffenseScore": 0, "endDefenseScore": 7},
    # Overtime, and a snake_case payload: both have been served by this API.
    {"game_id": 101, "drive_id": 10103, "drive_number": 3, "offense": "Alpha", "defense": "Bravo",
     "is_home_offense": True, "scoring": True, "drive_result": "FG", "week": 3, "season": 2024,
     "start_period": 5, "start_time": {"minutes": 0, "seconds": 0},
     "end_period": 5, "end_time": {"minutes": 0, "seconds": 0},
     "plays": 3, "yards": 12, "start_offense_score": 7, "start_defense_score": 7,
     "end_offense_score": 10, "end_defense_score": 7},
    # A game this project does not hold (its games table is FBS-vs-FBS).
    {"gameId": 999, "driveNumber": 1, "offense": "Alpha", "defense": "Bravo",
     "isHomeOffense": True, "startPeriod": 1, "startTime": {"minutes": 15, "seconds": 0},
     "endPeriod": 1, "endTime": {"minutes": 9, "seconds": 0},
     "startOffenseScore": 0, "startDefenseScore": 0, "endOffenseScore": 3, "endDefenseScore": 0},
]


class TestTheFetcher:
    def test_both_key_styles_are_read(self):
        rows, _ = fd.drive_rows(PAYLOAD, 2024, "regular")
        by_number = {r["drive_number"]: r for r in rows if r["game_id"] == 101}
        assert set(by_number) == {1, 2, 3}
        assert by_number[3]["drive_result"] == "FG"          # snake_case payload
        assert by_number[3]["end_period"] == 5
        assert by_number[1]["is_home_offense"] == 1 and by_number[2]["is_home_offense"] == 0

    def test_a_clock_object_becomes_seconds_left_in_the_period(self):
        drives, _ = fd.drive_rows(PAYLOAD, 2024, "regular")
        rows = {r["drive_number"]: r for r in drives if r["game_id"] == 101}
        assert rows[1]["end_clock"] == 11 * 60 + 30
        assert rows[2]["start_clock"] == 11 * 60 + 30

    def test_a_drive_that_cannot_be_placed_is_dropped_and_counted(self):
        rows, dropped = fd.drive_rows([
            {"driveNumber": 1, "offense": "Alpha"},            # no game
            {"gameId": 5, "offense": "Alpha"},                 # no drive number
            "not a drive",
        ], 2024, "regular")
        assert rows == [] and dropped == 3

    @pytest.mark.parametrize("value,expected", [
        ({"minutes": 12, "seconds": 7}, 727),
        ({"minutes": 0, "seconds": 0}, 0),
        ({"seconds": 42}, 42),
        ("12:07", 727),
        ("727", 727),
        (727, 727),
        (None, None),
        ({}, None),
        ("quarter", None),
        ("12:70", None),
        (True, None),
        (-5, None),
    ])
    def test_every_clock_shape_this_api_has_served(self, value, expected):
        assert fd.clock_seconds(value) == expected

    def test_a_missing_clock_is_blank_and_not_zero(self):
        """Zero seconds left is a real state -- the end of a period."""
        rows, _ = fd.drive_rows([dict(PAYLOAD[0], endTime=None)], 2024, "regular")
        assert rows[0]["end_clock"] == ""
        rows, _ = fd.drive_rows([dict(PAYLOAD[0], endTime={"minutes": 0, "seconds": 0})],
                                2024, "regular")
        assert rows[0]["end_clock"] == 0

    def test_a_season_before_the_play_by_play_era_never_calls_the_api(self):
        def explode(*a, **k):
            raise AssertionError("CFBD called for a season with no drive data")
        original = fd.cfbd_http.get_json
        fd.cfbd_http.get_json = explode
        try:
            assert fd.fetch_drives(2000, {}) is None
        finally:
            fd.cfbd_http.get_json = original

    def test_a_failed_regular_season_keeps_the_existing_file(self):
        """A half season must never be published as if it were whole."""
        original = fd.cfbd_http.get_json
        fd.cfbd_http.get_json = lambda *a, **k: (_ for _ in ()).throw(TimeoutError("reset"))
        try:
            assert fd.fetch_drives(2024, {}) is None
        finally:
            fd.cfbd_http.get_json = original

    def test_a_failed_postseason_keeps_the_existing_file_too(self):
        """The season file is written whole, so a postseason timeout that
        returned the regular season alone would replace a complete archive with
        one holding no bowl or playoff drives -- and the backfill, seeing a
        success, would commit it and move on."""
        calls = []

        def answer(url, params=None, **kwargs):
            calls.append(params["seasonType"])
            if params["seasonType"] == "postseason":
                raise TimeoutError("reset")
            return PAYLOAD[:1]

        original = fd.cfbd_http.get_json
        fd.cfbd_http.get_json = answer
        try:
            rows = fd.fetch_drives(2024, {})
        finally:
            fd.cfbd_http.get_json = original
        assert calls == ["regular", "postseason"]
        assert rows is None

    def test_a_season_with_no_postseason_yet_is_not_a_failure(self):
        """An empty answer is a successful fetch of nothing, which is what a
        season still being played returns; only a request that got no answer
        stops the season."""
        def answer(url, params=None, **kwargs):
            return PAYLOAD[:1] if params["seasonType"] == "regular" else []

        original = fd.cfbd_http.get_json
        fd.cfbd_http.get_json = answer
        try:
            rows = fd.fetch_drives(2026, {})
        finally:
            fd.cfbd_http.get_json = original
        assert rows is not None and len(rows) == 1


def _league(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "league.db"))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER);
        INSERT INTO teams VALUES (1, 'Alpha'), (2, 'Bravo');
        INSERT INTO team_aliases VALUES ('Bravo Alias', 'Bravo');
        INSERT INTO games VALUES (101, 2024);
    """)
    path = tmp_path / "2024.csv.gz"
    rows, _ = fd.drive_rows(PAYLOAD, 2024, "regular")
    with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fd.CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return conn, str(path)


class TestTheLoader:
    def test_scores_are_stored_from_the_home_teams_point_of_view(self, tmp_path):
        conn, path = _league(tmp_path)
        load_drives(conn, path)
        rows = dict((r[0], r[1:]) for r in conn.execute(
            "SELECT drive_number, home_score_after, away_score_after FROM game_drives"))
        assert rows[1] == (7, 0)
        # The away team had the ball on drive 2, so CFBD's 0-7 is the home
        # team's 7-0. This is the conversion that would put the wrong team
        # ahead on a game page if it were dropped.
        assert rows[2] == (7, 0)
        assert rows[3] == (10, 7)

    def test_a_drive_with_no_home_side_keeps_no_score_rather_than_a_guess(self):
        row = {"start_offense_score": 7, "start_defense_score": 0, "is_home_offense": ""}
        assert home_away(row, "start") == (None, None)

    def test_teams_resolve_through_the_same_aliases_as_every_other_loader(self, tmp_path):
        conn, path = _league(tmp_path)
        load_drives(conn, path)
        offense = dict(conn.execute(
            "SELECT drive_number, offense_team_id FROM game_drives").fetchall())
        assert offense[1] == 1 and offense[2] == 2      # 'Bravo Alias' -> Bravo

    def test_an_unresolved_name_keeps_the_drive_and_reports_the_name(self, tmp_path):
        conn, path = _league(tmp_path)
        extra = dict(PAYLOAD[0], driveNumber=4, offense="Nowhere State")
        rows, _ = fd.drive_rows([extra], 2024, "regular")
        with gzip.open(path, "at", newline="", encoding="utf-8") as fh:
            csv.DictWriter(fh, fieldnames=fd.CSV_FIELDS).writerows(rows)
        stats = load_drives(conn, path)
        assert stats["unmatched"] == ["Nowhere State"]
        kept = conn.execute(
            "SELECT offense_name, offense_team_id FROM game_drives WHERE drive_number = 4").fetchone()
        assert kept == ("Nowhere State", None)

    def test_a_drive_from_a_game_this_project_does_not_hold_is_skipped(self, tmp_path):
        conn, path = _league(tmp_path)
        stats = load_drives(conn, path)
        assert stats["skipped_games"] == [999]
        assert conn.execute("SELECT COUNT(*) FROM game_drives WHERE game_id = 999").fetchone()[0] == 0

    def test_regulation_seconds_come_from_the_period_and_clock(self, tmp_path):
        conn, path = _league(tmp_path)
        load_drives(conn, path)
        seconds = dict(conn.execute(
            "SELECT drive_number, seconds_remaining FROM game_drives").fetchall())
        assert seconds[1] == 2700 + 11 * 60 + 30        # end of a first-quarter drive
        assert seconds[2] == 1800 + 14 * 60             # into the second quarter
        assert seconds[3] == 0                          # overtime: regulation is spent

    def test_an_unreadable_clock_is_null_not_zero(self, tmp_path):
        assert regulation_seconds_remaining(2, None) is None
        assert regulation_seconds_remaining(None, 600) is None
        assert regulation_seconds_remaining(4, 0) == 0
        conn, path = _league(tmp_path)
        rows, _ = fd.drive_rows([dict(PAYLOAD[0], driveNumber=5, endTime=None)], 2024, "regular")
        with gzip.open(path, "at", newline="", encoding="utf-8") as fh:
            csv.DictWriter(fh, fieldnames=fd.CSV_FIELDS).writerows(rows)
        stats = load_drives(conn, path)
        assert stats["untimed"] == 1
        assert conn.execute(
            "SELECT seconds_remaining FROM game_drives WHERE drive_number = 5").fetchone()[0] is None

    def test_loading_twice_leaves_one_copy_of_each_drive(self, tmp_path):
        conn, path = _league(tmp_path)
        first = load_drives(conn, path)
        second = load_drives(conn, path)
        assert first["loaded"] == second["loaded"] == 3
        assert conn.execute("SELECT COUNT(*) FROM game_drives").fetchone()[0] == 3

    def test_the_loader_never_needs_an_http_library(self):
        """CI's rebuild step runs without `requests` installed."""
        import subprocess
        import sys
        from pathlib import Path
        code = ("import sys; sys.modules['requests'] = None; "
                "sys.path.insert(0, 'src'); import load_drives")
        result = subprocess.run([sys.executable, "-c", code],
                                cwd=str(Path(__file__).resolve().parent.parent),
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
