"""
Per-period scores through the three places they pass: the games fetcher, the
loader's additive migration, and the game-detail export.

The scores were already in the /games payload -- compute_went_ot() has read
them since the overtime fix -- and were discarded after that check. These keep
them.
"""
import csv
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from fetch_cfbd_games import line_score_text

REPO = Path(__file__).resolve().parents[1]


def test_periods_are_recorded_in_order():
    g = {"homeLineScores": [7, 14, 0, 3], "awayLineScores": [3, 7, 7, 0]}
    assert line_score_text(g, "home") == "7-14-0-3"
    assert line_score_text(g, "away") == "3-7-7-0"


def test_overtime_periods_are_kept():
    g = {"homeLineScores": [7, 14, 0, 6, 7, 3], "awayLineScores": [0, 10, 10, 7, 7, 0]}
    assert line_score_text(g, "home") == "7-14-0-6-7-3"


def test_snake_case_payload_is_read_too():
    assert line_score_text({"home_line_scores": [0, 0, 7, 0]}, "home") == "0-0-7-0"


@pytest.mark.parametrize("payload", [
    {"homeLineScores": None},
    {"homeLineScores": []},
    {},
    # A source that sends something other than whole numbers is not guessed at.
    {"homeLineScores": [7, None, 0, 3]},
    {"homeLineScores": "7-0-0-0"},
])
def test_nothing_is_invented_when_the_source_has_no_periods(payload):
    assert line_score_text(payload, "home") == ""


def _schema(conn):
    conn.executescript((REPO / "sql" / "schema.sql").read_text(encoding="utf-8"))


def test_loader_adds_the_columns_and_fills_them_on_a_refetch(tmp_path):
    """A season already loaded gains its line scores when it is fetched again."""
    db = tmp_path / "league.db"
    conn = sqlite3.connect(db)
    _schema(conn)
    conn.execute("INSERT INTO teams (team_id, team_name) VALUES (1, 'Alpha'), (2, 'Beta')")
    conn.commit()
    conn.close()

    header = ["game_id", "season_year", "week", "date", "season_type", "home_team", "away_team",
              "home_score", "away_score", "went_ot", "game_type", "game_phase", "neutral_site", "notes"]
    row = ["99", "2026", "1", "2026-09-05", "regular", "Alpha", "Beta",
           "21", "17", "0", "regular", "regular", "0", ""]

    def write(path, extra_header=(), extra=()):
        with Path(path).open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header + list(extra_header))
            w.writerow(row + list(extra))
        return str(path)

    def load(path):
        subprocess.run([sys.executable, str(REPO / "src" / "load_games.py"), path],
                       cwd=tmp_path, check=True, capture_output=True,
                       env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(REPO / "src")})

    # The loader resolves db/league.db relative to the repo, so point it at ours.
    (tmp_path / "db").mkdir(exist_ok=True)
    db.rename(tmp_path / "db" / "league.db")

    first = write(tmp_path / "a.csv")
    load(first)
    conn = sqlite3.connect(tmp_path / "db" / "league.db")
    columns = {r[1] for r in conn.execute("PRAGMA table_info(games)")}
    assert {"home_line_scores", "away_line_scores"} <= columns, "the migration is additive"
    assert conn.execute("SELECT home_line_scores FROM games").fetchone()[0] is None

    second = write(tmp_path / "b.csv", ["home_line_scores", "away_line_scores"], ["7-7-0-7", "3-7-7-0"])
    load(second)
    conn = sqlite3.connect(tmp_path / "db" / "league.db")
    assert conn.execute("SELECT home_line_scores, away_line_scores FROM games").fetchone() == ("7-7-0-7", "3-7-7-0")
    assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 1, "the re-fetch updates, never duplicates"


def test_the_detail_export_carries_the_periods(tmp_path):
    """build_game_details appends the two fields without moving the others."""
    sys.path.insert(0, str(REPO / "src"))
    import export_static_data as ex

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
      CREATE TABLE games (game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
        away_team_id INTEGER, home_score INTEGER, away_score INTEGER,
        home_line_scores TEXT, away_line_scores TEXT);
      CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER, elo_z REAL,
        coe_z REAL, hybrid_rating REAL, hybrid_expectation REAL, result_type TEXT);
      CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER);
      INSERT INTO games VALUES (7, 2026, 1, 2, 21, 17, '7-7-0-7', '3-7-7-0');
      INSERT INTO hybrid_game_ratings VALUES (7, 1, 0.5, 0.5, 1600, 0.6, 'W'), (7, 2, 0.1, 0.1, 1500, 0.4, 'L');
    """)
    payload = ex.build_game_details(conn)[2026]
    assert payload["fields"][-2:] == ["home_line_scores", "away_line_scores"]
    row = dict(zip(payload["fields"], payload["games"][0]))
    assert (row["home_line_scores"], row["away_line_scores"]) == ("7-7-0-7", "3-7-7-0")
    # Appending must not move an existing position.
    assert payload["fields"].index("home_result") == 5


def test_the_detail_export_runs_against_a_database_without_the_columns():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
      CREATE TABLE games (game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
        away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
      CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER, elo_z REAL,
        coe_z REAL, hybrid_rating REAL, hybrid_expectation REAL, result_type TEXT);
      CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER);
      INSERT INTO games VALUES (7, 2026, 1, 2, 21, 17);
      INSERT INTO hybrid_game_ratings VALUES (7, 1, 0.5, 0.5, 1600, 0.6, 'W'), (7, 2, 0.1, 0.1, 1500, 0.4, 'L');
    """)
    sys.path.insert(0, str(REPO / "src"))
    import export_static_data as ex
    row = dict(zip(ex.DETAIL_FIELDS, ex.build_game_details(conn)[2026]["games"][0]))
    assert row["home_line_scores"] is None
