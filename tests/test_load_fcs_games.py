"""
Loading games against opponents this project does not hold.

The load is only allowed to do one thing: record the FBS side's result against
a named opponent. The tests that matter are the ones about what it must NOT do
-- reach the games table, invent an opponent team, or double-count a game the
ratings already have.
"""
import csv
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import load_fcs_games  # noqa: E402

HEADER = [
    "game_id", "season_year", "week", "date", "season_type",
    "home_team", "away_team", "home_conference", "away_conference",
    "home_classification", "away_classification",
    "home_score", "away_score", "went_ot", "game_phase", "neutral_site", "notes",
]


def row(**over):
    base = dict(
        game_id="1", season_year="2024", week="1", date="2024-08-31",
        season_type="regular", home_team="Alabama", away_team="Mercer",
        home_conference="SEC", away_conference="Southern",
        home_classification="fbs", away_classification="fcs",
        home_score="42", away_score="7", went_ot="0", game_phase="regular",
        neutral_site="0", notes="",
    )
    base.update(over)
    return base


@pytest.fixture
def db(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.executescript(
        """
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL,
                            short_name TEXT, state TEXT);
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER);
        INSERT INTO teams (team_id, team_name) VALUES (1, 'Alabama'), (2, 'Auburn');
        """
    )
    conn.commit()
    return conn


def write(tmp_path, rows):
    path = tmp_path / "fcs_games_2024.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    return path


def test_the_fbs_side_is_recorded_against_a_named_opponent(db, tmp_path):
    result = load_fcs_games.load(db, write(tmp_path, [row()]), 2024)
    assert result["loaded"] == 1
    got = db.execute(
        "SELECT team_id, opponent_name, team_score, opponent_score, is_home,"
        " opponent_classification, opponent_conference FROM non_fbs_games").fetchone()
    assert got == (1, "Mercer", 42, 7, 1, "fcs", "Southern")


def test_an_away_fbs_team_keeps_its_own_score(db, tmp_path):
    # The FBS side can be either side of the fixture, and getting this backwards
    # would turn every road game into the opposite result.
    load_fcs_games.load(db, write(tmp_path, [row(
        home_team="Mercer", away_team="Alabama", home_score="7", away_score="42",
        home_classification="fcs", away_classification="fbs",
        home_conference="Southern", away_conference="SEC")]), 2024)
    assert db.execute(
        "SELECT team_id, opponent_name, team_score, opponent_score, is_home"
        " FROM non_fbs_games").fetchone() == (1, "Mercer", 42, 7, 0)


def test_nothing_is_written_to_games_and_no_opponent_team_is_invented(db, tmp_path):
    load_fcs_games.load(db, write(tmp_path, [row()]), 2024)
    assert db.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 0
    assert [r[0] for r in db.execute("SELECT team_name FROM teams ORDER BY team_name")] \
        == ["Alabama", "Auburn"]


def test_a_game_between_two_known_teams_is_refused(db, tmp_path):
    # The ratings already hold this one. Loading it here would show it twice on
    # the schedule and count it twice in the record.
    result = load_fcs_games.load(db, write(tmp_path, [row(away_team="Auburn")]), 2024)
    assert result["loaded"] == 0
    assert result["both_known"] == ["Auburn at Alabama"]
    assert db.execute("SELECT COUNT(*) FROM non_fbs_games").fetchone()[0] == 0


def test_a_game_with_no_known_team_is_refused(db, tmp_path):
    result = load_fcs_games.load(db, write(tmp_path, [row(home_team="Mercer", away_team="Samford")]), 2024)
    assert result["loaded"] == 0
    assert result["neither_known"] == ["Samford at Mercer"]


def test_reloading_a_season_replaces_it_rather_than_accumulating(db, tmp_path):
    load_fcs_games.load(db, write(tmp_path, [row(), row(game_id="2", away_team="Samford")]), 2024)
    assert db.execute("SELECT COUNT(*) FROM non_fbs_games").fetchone()[0] == 2
    # The source dropped one game. It must not survive the reload.
    load_fcs_games.load(db, write(tmp_path, [row()]), 2024)
    assert [r[0] for r in db.execute("SELECT game_id FROM non_fbs_games")] == [1]


def test_a_season_with_no_file_clears_that_season_and_leaves_others_alone(db, tmp_path):
    load_fcs_games.load(db, write(tmp_path, [row()]), 2024)
    load_fcs_games.load(db, write(tmp_path, [row(game_id="9", season_year="2023")]), 2023)
    load_fcs_games.load(db, tmp_path / "absent.csv", 2024)
    assert [r[0] for r in db.execute("SELECT game_id FROM non_fbs_games")] == [9]


def test_an_alias_resolves_the_fbs_side(db, tmp_path):
    db.execute("INSERT INTO team_aliases (alias, team_name) VALUES ('Bama', 'Alabama')")
    db.commit()
    load_fcs_games.load(db, write(tmp_path, [row(home_team="Bama")]), 2024)
    assert db.execute("SELECT team_id FROM non_fbs_games").fetchone()[0] == 1
