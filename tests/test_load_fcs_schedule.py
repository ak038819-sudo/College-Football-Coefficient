"""
Loading the games an FBS team has still to play against an unrated opponent.

This table exists to be counted, so the tests are about the count being right:
one row per game, on the right team, gone once the game has been played, and
never leaking into scheduled_games, where it would be asked to be predicted.
"""
import csv
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import load_fcs_schedule  # noqa: E402

HEADER = [
    "game_id", "season_year", "week", "kickoff_utc", "start_time_tbd", "season_type",
    "home_team", "away_team", "neutral_site", "game_phase", "notes", "venue", "venue_id",
    "home_conference", "away_conference", "home_classification", "away_classification",
]


def row(**over):
    base = dict(
        game_id="1", season_year="2026", week="3",
        kickoff_utc="2026-09-12T16:00:00.000Z", start_time_tbd="0",
        season_type="regular", home_team="Alabama", away_team="Mercer",
        neutral_site="0", game_phase="regular", notes="", venue="Bryant-Denny",
        venue_id="900", home_conference="SEC", away_conference="Southern",
        home_classification="fbs", away_classification="fcs",
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
        INSERT INTO teams (team_id, team_name) VALUES (1, 'Alabama'), (2, 'Idaho');
        """
    )
    conn.commit()
    return conn


def write(tmp_path, rows, year=2026):
    path = tmp_path / f"fcs_schedule_{year}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    return path


def test_the_fixture_is_recorded_against_the_fbs_side(db, tmp_path):
    result = load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    assert result["loaded"] == 1
    got = db.execute(
        "SELECT team_id, opponent_name, opponent_conference, opponent_classification,"
        " is_home, week, season_type, kickoff_utc, game_phase, venue"
        " FROM scheduled_non_fbs_games").fetchone()
    assert got == (1, "Mercer", "Southern", "fcs", 1, 3, "regular",
                   "2026-09-12T16:00:00.000Z", "regular", "Bryant-Denny")


def test_an_away_fbs_team_is_still_the_side_the_row_hangs_off(db, tmp_path):
    load_fcs_schedule.load(db, write(tmp_path, [row(
        home_team="Mercer", away_team="Alabama",
        home_classification="fcs", away_classification="fbs")]), 2026)
    assert db.execute("SELECT team_id, opponent_name, is_home FROM scheduled_non_fbs_games"
                      ).fetchone() == (1, "Mercer", 0)


def test_nothing_is_written_to_scheduled_games(db, tmp_path):
    # The whole reason for a second table: predict_upcoming.py reads
    # scheduled_games and has no rating to price one of these with.
    load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    tables = {name for (name,) in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "scheduled_games" not in tables


def test_no_opponent_team_is_invented(db, tmp_path):
    load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    assert db.execute("SELECT COUNT(*) FROM teams").fetchone()[0] == 2


def test_a_fixture_the_ratings_have_since_taken_is_dropped(db, tmp_path):
    # A game the source has reclassified as FBS-vs-FBS since the snapshot is in
    # `games`. Counting it as still to come would overstate the schedules it
    # is on and have it priced twice.
    db.execute("INSERT INTO games (game_id, season_year) VALUES (1, 2026)")
    result = load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    assert result["loaded"] == 0
    assert result["already_played"] == ["Mercer at Alabama"]
    assert db.execute("SELECT COUNT(*) FROM scheduled_non_fbs_games").fetchone()[0] == 0


def test_a_fixture_that_has_since_been_played_is_dropped(db, tmp_path):
    # A completed FBS-vs-FCS game goes to non_fbs_games, never to `games`, and
    # load_fcs_games.py runs first in the pipeline. So that is where "it has
    # been played" shows up for one of these, and a stale schedule file must
    # not report it as still to come.
    load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)   # creates the schema
    db.execute("""
        INSERT INTO non_fbs_games (game_id, season_year, team_id, opponent_name,
                                   team_score, opponent_score, is_home, went_ot, game_phase)
        VALUES (1, 2026, 1, 'Mercer', 42, 7, 1, 0, 'regular')""")
    result = load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    assert result["loaded"] == 0
    assert result["already_played"] == ["Mercer at Alabama"]
    assert db.execute("SELECT COUNT(*) FROM scheduled_non_fbs_games").fetchone()[0] == 0


def test_a_season_is_replaced_wholesale_so_a_cancelled_game_disappears(db, tmp_path):
    load_fcs_schedule.load(db, write(tmp_path, [row(), row(game_id="2", away_team="Furman")]), 2026)
    assert db.execute("SELECT COUNT(*) FROM scheduled_non_fbs_games").fetchone()[0] == 2
    load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    assert [g for (g,) in db.execute("SELECT game_id FROM scheduled_non_fbs_games")] == [1]


def test_another_season_is_left_alone(db, tmp_path):
    load_fcs_schedule.load(db, write(tmp_path, [row(game_id="7", season_year="2025")], 2025), 2025)
    load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    assert sorted(y for (y,) in db.execute(
        "SELECT season_year FROM scheduled_non_fbs_games")) == [2025, 2026]


def test_a_missing_file_clears_the_season_rather_than_keeping_it(db, tmp_path):
    load_fcs_schedule.load(db, write(tmp_path, [row()]), 2026)
    result = load_fcs_schedule.load(db, tmp_path / "nope.csv", 2026)
    assert result == {"loaded": 0, "already_played": [], "ambiguous": [],
                      "neither_known": [], "read": 0}
    assert db.execute("SELECT COUNT(*) FROM scheduled_non_fbs_games").fetchone()[0] == 0


def test_a_game_between_two_teams_we_hold_needs_the_source_to_name_the_fbs_side(db, tmp_path):
    # Idaho is in `teams` for its FBS seasons and is an unrated opponent in the
    # others, so both names resolving does not say which side was FBS. Only
    # the per-game classification does.
    both = row(away_team="Idaho", away_classification="", home_classification="")
    result = load_fcs_schedule.load(db, write(tmp_path, [both]), 2026)
    assert result["loaded"] == 0
    assert result["ambiguous"] == ["Idaho at Alabama"]


def test_a_game_between_two_teams_we_hold_loads_when_it_does(db, tmp_path):
    result = load_fcs_schedule.load(db, write(tmp_path, [row(
        away_team="Idaho", away_classification="fcs")]), 2026)
    assert result["loaded"] == 1
    assert db.execute("SELECT team_id, opponent_name FROM scheduled_non_fbs_games"
                      ).fetchone() == (1, "Idaho")


def test_a_game_with_neither_team_known_is_not_loaded(db, tmp_path):
    result = load_fcs_schedule.load(db, write(tmp_path, [row(
        home_team="Mercer", away_team="Furman",
        home_classification="fcs", away_classification="fcs")]), 2026)
    assert result["loaded"] == 0
    assert result["neither_known"] == ["Furman at Mercer"]


def test_an_unknown_kickoff_or_week_is_null_not_zero(db, tmp_path):
    # A bowl fixture can have neither. Storing 0 would put it in week zero and
    # at the epoch, both of which read as facts.
    load_fcs_schedule.load(db, write(tmp_path, [row(week="", kickoff_utc="")]), 2026)
    assert db.execute("SELECT week, kickoff_utc FROM scheduled_non_fbs_games"
                      ).fetchone() == (None, None)
