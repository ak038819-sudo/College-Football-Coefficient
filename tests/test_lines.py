"""
The betting-line archive: CFBD's payload in, this database's sign out.

One conversion matters more than everything else here. CFBD's spread is the
home team's handicap and runs NEGATIVE when the home team is favoured; every
margin in this database is home minus away. Get it backwards and the site
reports the model beating the market on exactly the games it lost to it, which
is the one error a benchmark must not make. So the tests concentrate there, and
on keeping an unpriced game out of the data as a gap rather than as a pick'em.
"""
from __future__ import annotations

import csv
import gzip
import sqlite3

import pytest

import fetch_cfbd_lines as fl
from load_lines import load_lines, market_home_margin

# Shaped like CFBD's /lines: camelCase, a list of providers per game, a spread
# that is negative when the home team is favoured.
PAYLOAD = [
    {"id": 101, "season": 2024, "seasonType": "regular", "week": 3,
     "homeTeam": "Alpha", "awayTeam": "Bravo", "homeScore": 31, "awayScore": 17,
     "lines": [
         {"provider": "consensus", "spread": -7.5, "spreadOpen": -6.5,
          "overUnder": 55.5, "overUnderOpen": 54.0,
          "homeMoneyline": -280, "awayMoneyline": 230,
          "formattedSpread": "Alpha -7.5"},
         {"provider": "Bovada", "spread": -7.0, "overUnder": 56.0,
          "formattedSpread": "Alpha -7"},
     ]},
    # The away team favoured: CFBD's spread is POSITIVE here.
    {"id": 102, "season": 2024, "seasonType": "regular", "week": 3,
     "homeTeam": "Bravo", "awayTeam": "Alpha", "homeScore": 10, "awayScore": 24,
     "lines": [{"provider": "consensus", "spread": 10.5, "overUnder": 48.0}]},
    # A pick'em, and a game nobody priced. They must not end up looking alike.
    {"id": 103, "season": 2024, "lines": [{"provider": "consensus", "spread": 0.0}]},
    {"id": 104, "season": 2024, "lines": [{"provider": "consensus", "overUnder": 44.0}]},
    # A game CFBD lists with no lines at all contributes nothing.
    {"id": 105, "season": 2024, "lines": []},
]


class TestTheFetcher:
    def test_one_row_per_provider_with_the_game_repeated(self):
        rows, dropped = fl.line_rows(PAYLOAD, 2024, "regular")
        assert dropped == 0
        assert len(rows) == 5          # 2 + 1 + 1 + 1, and nothing for game 105
        first = [r for r in rows if r["game_id"] == 101]
        assert sorted(r["provider"] for r in first) == ["Bovada", "consensus"]
        assert {r["home_team"] for r in first} == {"Alpha"}

    def test_the_spread_is_copied_exactly_as_served(self):
        """The flip belongs to the loader; a fetcher that also flipped would
        double it back."""
        rows, _ = fl.line_rows(PAYLOAD, 2024, "regular")
        consensus = {r["game_id"]: r for r in rows if r["provider"] == "consensus"}
        assert consensus[101]["spread"] == -7.5
        assert consensus[102]["spread"] == 10.5

    def test_an_unpriced_game_is_blank_and_not_zero(self):
        rows, _ = fl.line_rows(PAYLOAD, 2024, "regular")
        by_game = {r["game_id"]: r for r in rows}
        assert by_game[103]["spread"] == 0.0     # a pick'em is a real line
        assert by_game[104]["spread"] == ""      # nobody priced it
        assert by_game[104]["over_under"] == 44.0

    def test_a_game_with_no_id_is_dropped_and_counted(self):
        rows, dropped = fl.line_rows(
            [{"season": 2024, "lines": [{"provider": "consensus", "spread": -3}]},
             "not a game",
             {"id": 7, "season": 2024, "lines": [{"provider": "consensus", "spread": -3}]}],
            2024, "regular")
        assert dropped == 2
        assert [r["game_id"] for r in rows] == [7]

    @pytest.mark.parametrize("value,expected", [
        (-7.5, -7.5), ("-7.5", -7.5), (0, 0.0), ("0", 0.0),
        (None, ""), ("", ""), ("off", ""), (True, ""),
    ])
    def test_every_number_shape_this_api_has_served(self, value, expected):
        assert fl._number(value) == expected

    def test_both_key_styles_are_read(self):
        rows, _ = fl.line_rows(
            [{"game_id": 9, "season": 2024, "home_team": "Alpha", "away_team": "Bravo",
              "lines": [{"provider": "consensus", "spread_open": -4.5,
                         "over_under": 52, "home_moneyline": -200}]}],
            2024, "regular")
        assert rows[0]["spread_open"] == -4.5
        assert rows[0]["over_under"] == 52.0
        assert rows[0]["home_moneyline"] == -200
        assert rows[0]["home_team"] == "Alpha"

    def test_a_failed_request_keeps_the_existing_file(self, monkeypatch):
        def boom(*args, **kwargs):
            raise RuntimeError("502")
        monkeypatch.setattr(fl.cfbd_http, "get_json", boom)
        assert fl.fetch_lines(2024, {}) is None

    def test_a_failed_postseason_keeps_the_existing_file_too(self, monkeypatch):
        """The whole season file is rewritten from what this returns, so a
        half-fetched season must not look like a success."""
        calls = []

        def answer(url, params=None, **kwargs):
            calls.append(params["seasonType"])
            if params["seasonType"] == "postseason":
                raise RuntimeError("timeout")
            return PAYLOAD
        monkeypatch.setattr(fl.cfbd_http, "get_json", answer)
        assert fl.fetch_lines(2024, {}) is None
        assert calls == ["regular", "postseason"]

    def test_a_season_with_no_lines_is_not_a_failure(self, monkeypatch, tmp_path):
        """Most of the 1980s has none, and an empty answer must leave any
        existing file alone rather than truncating it."""
        monkeypatch.setattr(fl.cfbd_http, "get_json", lambda *a, **k: [])
        assert fl.write_lines(1981, {}, out_dir=tmp_path) == "empty"
        assert not list(tmp_path.glob("*.csv.gz"))

    def test_a_season_with_lines_is_written(self, monkeypatch, tmp_path):
        monkeypatch.setattr(fl.cfbd_http, "get_json",
                            lambda url, params=None, **k:
                            PAYLOAD if params["seasonType"] == "regular" else [])
        assert fl.write_lines(2024, {}, out_dir=tmp_path) == "written"
        with gzip.open(tmp_path / "2024.csv.gz", "rt", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 5


class TestTheSign:
    """The one conversion that decides whether the comparison means anything."""

    def test_a_home_favourite_becomes_a_positive_expected_margin(self):
        assert market_home_margin(-7.5) == 7.5

    def test_an_away_favourite_becomes_a_negative_one(self):
        assert market_home_margin(10.5) == -10.5

    def test_a_pickem_stays_zero_and_a_missing_line_stays_missing(self):
        assert market_home_margin(0) == 0.0
        assert market_home_margin("") is None
        assert market_home_margin(None) is None


def _league(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "league.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                            home_score INTEGER, away_score INTEGER);
        INSERT INTO games VALUES (101, 2024, 31, 17), (102, 2024, 10, 24),
                                 (103, 2024, 20, 20), (104, 2024, 7, 3);
    """)
    path = tmp_path / "2024.csv.gz"
    rows, _ = fl.line_rows(PAYLOAD, 2024, "regular")
    with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fl.CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return conn, str(path)


class TestTheLoader:
    def test_the_stored_margin_runs_the_same_way_as_every_other_margin(self, tmp_path):
        conn, path = _league(tmp_path)
        load_lines(conn, path)
        stored = dict(conn.execute(
            "SELECT game_id, market_home_margin FROM game_lines WHERE provider = 'consensus'"))
        # Game 101: home won by 14 and was favoured by 7.5, so both numbers are
        # positive. Game 102: home lost by 14 and was a 10.5-point underdog, so
        # both are negative. A flipped sign would make these disagree.
        assert stored[101] == 7.5
        assert stored[102] == -10.5
        margins = dict(conn.execute(
            "SELECT game_id, home_score - away_score FROM games"))
        for game_id in (101, 102):
            assert (stored[game_id] > 0) == (margins[game_id] > 0)

    def test_cfbds_own_spread_is_kept_beside_it(self, tmp_path):
        conn, path = _league(tmp_path)
        load_lines(conn, path)
        assert conn.execute(
            "SELECT spread FROM game_lines WHERE game_id = 101 AND provider = 'consensus'"
        ).fetchone()[0] == -7.5

    def test_an_unpriced_game_keeps_a_null_spread_not_a_pickem(self, tmp_path):
        conn, path = _league(tmp_path)
        load_lines(conn, path)
        stored = dict(conn.execute(
            "SELECT game_id, market_home_margin FROM game_lines WHERE provider = 'consensus'"))
        assert stored[103] == 0.0       # the market really did call this one even
        assert stored[104] is None      # nobody priced it

    def test_every_provider_is_kept_rather_than_averaged(self, tmp_path):
        conn, path = _league(tmp_path)
        stats = load_lines(conn, path)
        assert stats["providers"] == ["Bovada", "consensus"]
        assert conn.execute(
            "SELECT COUNT(*) FROM game_lines WHERE game_id = 101").fetchone()[0] == 2

    def test_a_line_for_a_game_this_project_does_not_hold_is_skipped(self, tmp_path):
        conn, path = _league(tmp_path)
        conn.execute("DELETE FROM games WHERE game_id = 102")
        stats = load_lines(conn, path)
        assert stats["skipped_games"] == [102]
        assert conn.execute(
            "SELECT COUNT(*) FROM game_lines WHERE game_id = 102").fetchone()[0] == 0

    def test_loading_twice_leaves_one_copy_of_each_line(self, tmp_path):
        conn, path = _league(tmp_path)
        first = load_lines(conn, path)
        second = load_lines(conn, path)
        assert first["loaded"] == second["loaded"] == 5
        assert conn.execute("SELECT COUNT(*) FROM game_lines").fetchone()[0] == 5

    def test_the_loader_never_needs_an_http_library(self):
        """CI's rebuild step runs without `requests` installed."""
        import subprocess
        import sys
        from pathlib import Path
        code = ("import sys; sys.modules['requests'] = None; "
                "sys.path.insert(0, 'src'); import load_lines")
        result = subprocess.run([sys.executable, "-c", code],
                                cwd=str(Path(__file__).resolve().parent.parent),
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
