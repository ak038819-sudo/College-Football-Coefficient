"""
Milestone 3: schedule ingestion + Elo predictions.
Pins the roadmap's rules: scheduled games never reach games/Elo/CoE, in-progress
games are never treated as final, predictions use the engine's own formula with
identical home-field / neutral-site handling and offseason regression.
"""
import csv
import sqlite3

import pytest

from build_elo import effective_rating, expected_result, fetch_games_chronological, run_elo
from fetch_cfbd_games import is_completed
from load_schedule import load_schedule
from predict_upcoming import build_upcoming, current_ratings, elo_config, game_expectation

CFG = elo_config()


# ---------------- fetch: only truly final games count as games ----------------

def test_in_progress_game_with_scores_is_not_final():
    assert is_completed({"homePoints": 14, "awayPoints": 10, "completed": False}) is False


def test_completed_game_is_final_and_legacy_payloads_fall_back_to_scores():
    assert is_completed({"homePoints": 31, "awayPoints": 10, "completed": True}) is True
    assert is_completed({"home_points": 31, "away_points": 10}) is True        # no flag: historical payload
    assert is_completed({"homePoints": None, "awayPoints": None, "completed": False}) is False


# ---------------- same formula, same home field, same neutral handling ----------------

def test_neutral_site_applies_no_home_field():
    assert game_expectation(1600, 1600, True, CFG) == pytest.approx(0.5)


def test_home_game_applies_engine_home_field():
    expected = expected_result(1600 + CFG["home_field"], 1600, CFG["scale"])
    assert game_expectation(1600, 1600, False, CFG) == pytest.approx(expected)
    assert expected > 0.5


# ---------------- tiny synthetic league ----------------

def _league(tmp_path, games):
    db = tmp_path / "league.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, week INTEGER, game_date TEXT,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER,
            went_ot INTEGER DEFAULT 0, neutral_site INTEGER DEFAULT 0, game_phase TEXT DEFAULT 'regular');
        INSERT INTO teams VALUES (1,'Alpha'),(2,'Bravo'),(3,'Charlie');
    """)
    conn.executemany("INSERT INTO games (game_id, season_year, game_date, home_team_id, away_team_id, home_score, away_score, neutral_site)"
                     " VALUES (?,?,?,?,?,?,?,?)", games)
    conn.commit()
    return conn


def _schedule_csv(tmp_path, rows, season=2026):
    path = tmp_path / f"schedule_{season}.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["game_id", "season_year", "week", "kickoff_utc", "start_time_tbd", "season_type",
                    "home_team", "away_team", "neutral_site", "game_phase", "notes"])
        w.writerows(rows)
    return str(path)


GAMES_2025 = [(1, 2025, "2025-09-01", 1, 2, 35, 10, 0), (2, 2025, "2025-09-08", 2, 1, 21, 24, 0)]


def test_scheduled_games_never_reach_games_or_elo(tmp_path):
    conn = _league(tmp_path, GAMES_2025)
    before = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    load_schedule(conn, _schedule_csv(tmp_path, [
        [10, 2026, 1, "2026-09-05T16:00:00.000Z", 0, "regular", "Alpha", "Charlie", 0, "regular", ""]]))
    assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == before
    conn.row_factory = sqlite3.Row
    assert 10 not in {g["game_id"] for g in fetch_games_chronological(conn)}   # the Elo engine's input
    cols = {r[1] for r in conn.execute("PRAGMA table_info(scheduled_games)")}
    assert not {"home_score", "away_score"} & cols, "scheduled_games must not be able to hold scores"


def test_load_schedule_skips_final_games_and_replaces_the_season(tmp_path):
    conn = _league(tmp_path, GAMES_2025)
    row = lambda gid, h, a: [gid, 2026, 1, "2026-09-05T16:00:00.000Z", 0, "regular", h, a, 0, "regular", ""]
    stats = load_schedule(conn, _schedule_csv(tmp_path, [row(1, "Alpha", "Bravo"), row(11, "Alpha", "Charlie"),
                                                         row(12, "Bravo", "Nobody FCS")]))
    assert stats == {"inserted": 1, "already_final": 1, "unresolved": 1}
    load_schedule(conn, _schedule_csv(tmp_path, [row(13, "Charlie", "Bravo")]))      # re-fetch: game 11 gone
    assert [r[0] for r in conn.execute("SELECT game_id FROM scheduled_games")] == [13]


def test_regression_matches_run_elo_and_happens_once_even_after_a_skipped_season(tmp_path):
    conn = _league(tmp_path, GAMES_2025)
    conn.row_factory = sqlite3.Row
    games = list(fetch_games_chronological(conn))
    probe = dict(games[-1])
    probe.update(game_id=-1, season_year=2026, game_date="2026-09-01", home_team_id=1, away_team_id=2,
                 home_score=1, away_score=0)
    engine_pre = [r[2] for r in run_elo(games + [probe], CFG)[0] if r[0] == -1 and r[1] == 1][0]
    conn.row_factory = None
    assert current_ratings(conn, CFG, 2026)[0][1] == pytest.approx(engine_pre)
    assert current_ratings(conn, CFG, 2028)[0][1] == pytest.approx(engine_pre)   # run_elo regresses once


def test_build_upcoming_predictions(tmp_path):
    conn = _league(tmp_path, GAMES_2025)
    load_schedule(conn, _schedule_csv(tmp_path, [
        [20, 2026, 3, "2026-09-19T20:00:00.000Z", 0, "regular", "Alpha", "Bravo", 1, "regular", ""],
        [21, 2026, 4, "2026-09-26T20:00:00.000Z", 0, "regular", "Alpha", "Charlie", 0, "regular", ""]]))
    up = build_upcoming(conn, CFG)
    g20, g21 = up["games"]
    ratings = current_ratings(conn, CFG, 2026)[0]
    assert g20[11] == pytest.approx(game_expectation(ratings[1], ratings[2], True, CFG), abs=1e-4)
    assert g20[12:] == [0, 0]
    assert g21[13] == 1 and g21[10] == CFG["initial_rating"]      # Charlie never played: flagged provisional
    assert [r[0] for r in up["current_elo"]][:1] == [max(ratings, key=ratings.get)]


# ---------------- real database ----------------

@pytest.fixture(scope="module")
def real_conn(db_path):
    conn = sqlite3.connect(str(db_path))
    n = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='elo_game_history'").fetchone()[0]
    if not n or not conn.execute("SELECT COUNT(*) FROM elo_game_history").fetchone()[0]:
        pytest.skip("elo_game_history not built -- run src/build_elo.py first")
    yield conn
    conn.close()


def test_prediction_formula_reproduces_every_stored_engine_expectation(real_conn):
    rows = real_conn.execute("""
        SELECT e.pregame_elo, e.opponent_pregame_elo, e.elo_expectation, g.neutral_site
        FROM elo_game_history e JOIN games g USING(game_id) WHERE e.team_id = g.home_team_id""").fetchall()
    assert rows
    for pre, opp, stored, neutral in rows:
        assert game_expectation(pre, opp, neutral, CFG) == pytest.approx(stored, abs=1e-12)


def test_current_ratings_equal_last_stored_postgame(real_conn):
    season = real_conn.execute("SELECT MAX(season_year) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
    ratings, _, _ = current_ratings(real_conn, CFG, season)
    last = real_conn.execute("""
        SELECT team_id, postgame_elo FROM (
          SELECT e.team_id, e.postgame_elo, ROW_NUMBER() OVER (PARTITION BY e.team_id
                 ORDER BY g.season_year DESC, g.game_date DESC, e.game_id DESC) rn
          FROM elo_game_history e JOIN games g USING(game_id) WHERE g.season_year = ?) WHERE rn = 1""",
        (season,)).fetchall()
    for tid, post in last:
        assert ratings[tid] == pytest.approx(post, abs=1e-9)
