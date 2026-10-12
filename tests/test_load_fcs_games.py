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


def test_a_game_the_ratings_already_hold_is_refused(db, tmp_path):
    # Loading it here would show it twice on the schedule and count it twice in
    # the record. The test is the game's id, not its names: a name cannot know
    # which division a team was in for one season.
    db.execute("INSERT INTO games (game_id, season_year) VALUES (1, 2024)")
    db.commit()
    result = load_fcs_games.load(db, write(tmp_path, [row(away_team="Auburn")]), 2024)
    assert result["loaded"] == 0
    assert result["already_rated"] == ["Auburn at Alabama"]
    assert db.execute("SELECT COUNT(*) FROM non_fbs_games").fetchone()[0] == 0


def test_a_known_opponent_in_another_division_is_still_loaded(db, tmp_path):
    # Utah 66, Idaho 14 (2026). Idaho is in `teams` for the FBS seasons it
    # played, and this game is in no rating, so refusing it over the name is
    # how 649 real games went missing from 1980-2026.
    result = load_fcs_games.load(db, write(tmp_path, [row(
        away_team="Auburn", away_classification="fcs")]), 2024)
    assert result["loaded"] == 1
    assert db.execute(
        "SELECT team_id, opponent_name, team_score, opponent_score, is_home"
        " FROM non_fbs_games").fetchone() == (1, "Auburn", 42, 7, 1)


def test_the_source_decides_which_known_side_was_fbs(db, tmp_path):
    # Reading the FBS side off whichever name happens to resolve would put the
    # game on the wrong team's schedule with the scores swapped.
    load_fcs_games.load(db, write(tmp_path, [row(
        home_team="Auburn", away_team="Alabama",
        home_classification="fcs", away_classification="fbs",
        home_score="7", away_score="42")]), 2024)
    assert db.execute(
        "SELECT team_id, opponent_name, team_score, opponent_score, is_home"
        " FROM non_fbs_games").fetchone() == (1, "Auburn", 42, 7, 0)


def test_two_known_teams_with_no_fbs_side_named_is_refused(db, tmp_path):
    # Nothing in the row says which side the game belongs to, and guessing
    # would hang it off a team at random.
    for home_c, away_c in (("fbs", "fbs"), ("", ""), ("fbs", "")):
        result = load_fcs_games.load(db, write(tmp_path, [row(
            away_team="Auburn", home_classification=home_c,
            away_classification=away_c)]), 2024)
        assert result["loaded"] == 0, (home_c, away_c)
        assert result["ambiguous"] == ["Auburn at Alabama"], (home_c, away_c)


def test_a_game_with_no_known_team_is_refused(db, tmp_path):
    result = load_fcs_games.load(db, write(tmp_path, [row(
        home_team="Mercer", away_team="Samford", home_classification="fcs")]), 2024)
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


def test_one_known_side_is_used_whatever_the_source_calls_it(db, tmp_path):
    # Some 1980s rows label a program `fbs` that this project does not carry
    # (Cal State Fullerton, Pacific). The side this project holds is the only
    # side a row can hang off, so the game still reaches that team's schedule.
    result = load_fcs_games.load(db, write(tmp_path, [row(
        home_team="Pacific", away_team="Alabama",
        home_classification="fbs", away_classification="fcs",
        home_score="7", away_score="42")]), 2024)
    assert result["loaded"] == 1
    assert db.execute(
        "SELECT team_id, opponent_name, team_score, is_home FROM non_fbs_games"
    ).fetchone() == (1, "Pacific", 42, 0)


def test_an_alias_resolves_the_fbs_side(db, tmp_path):
    db.execute("INSERT INTO team_aliases (alias, team_name) VALUES ('Bama', 'Alabama')")
    db.commit()
    load_fcs_games.load(db, write(tmp_path, [row(home_team="Bama")]), 2024)
    assert db.execute("SELECT team_id FROM non_fbs_games").fetchone()[0] == 1


def test_the_real_archive_keeps_every_game_with_an_fbs_side_and_no_rating(db_path):
    """
    The archive itself, not a fixture: a game whose source classification names
    one FBS side, where that side is a team this project holds, and which no
    rating holds, has to be on that team's schedule.

    649 of them were not, because the loader refused any game whose two names
    both resolved -- which is every game a both-divisions program (Idaho,
    Delaware, Jacksonville State) played against the FBS after it moved down.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='non_fbs_games'"
        ).fetchone():
            pytest.skip("no non_fbs_games table -- load data/raw/fcs_games_*.csv first")
        cur = conn.cursor()
        rated = {r[0] for r in cur.execute("SELECT game_id FROM games")}
        loaded = {r[0] for r in cur.execute("SELECT game_id FROM non_fbs_games")}
        missing = []
        for path in sorted((ROOT / "data/raw").glob("fcs_games_*.csv")):
            for csv_row in csv.DictReader(path.open(encoding="utf-8")):
                fbs = load_fcs_games.fbs_side_by_classification(csv_row)
                if fbs is None:
                    continue
                if not load_fcs_games.resolve_database_name(
                        cur, csv_row[f"{fbs}_team"].strip()):
                    continue
                gid = int(csv_row["game_id"])
                if gid in rated or gid in loaded:
                    continue
                missing.append(
                    f"{path.name}: {csv_row['away_team']} at {csv_row['home_team']}")
        assert missing == []
    finally:
        conn.close()


def test_the_real_archive_has_utah_66_idaho_14(db_path):
    # The game Austin reported missing, which is the whole class in one row:
    # Idaho is in `teams` for its FBS seasons and played this one from the FCS.
    conn = sqlite3.connect(str(db_path))
    try:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='non_fbs_games'"
        ).fetchone():
            pytest.skip("no non_fbs_games table -- load data/raw/fcs_games_*.csv first")
        got = conn.execute(
            """
            SELECT t.team_name, n.opponent_name, n.team_score, n.opponent_score,
                   n.is_home, n.opponent_classification
            FROM non_fbs_games n JOIN teams t USING (team_id)
            WHERE n.game_id = 401856768
            """
        ).fetchone()
        assert got == ("Utah", "Idaho", 66, 14, 1, "fcs")
    finally:
        conn.close()
