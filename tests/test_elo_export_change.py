"""The Elo arrow uses the latest audited game, not rank movement or a live estimate."""
import sqlite3

from export_dashboard_data import load_elo_by_season


def test_latest_game_change_follows_latest_rating_within_each_season(tmp_path):
    path = tmp_path / "elo.db"
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
            CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT);
            CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER,
                postgame_elo REAL, elo_change REAL);
            INSERT INTO teams VALUES (1, 'Florida'), (2, 'Georgia');
            INSERT INTO games VALUES (1, 2025, '2025-12-01'),
                (2, 2026, '2026-09-20'), (3, 2026, '2026-09-26'),
                (4, 2026, '2026-09-26');
            INSERT INTO elo_game_history VALUES (1, 1, 1500, -10),
                (2, 1, 1510, 10), (3, 1, 1493.25, -16.75),
                (4, 1, 1500.25, 7), (2, 2, 1550, 5);
        """)
    exported = load_elo_by_season(str(path))
    assert exported[2025] == [{"team": "Florida", "elo": 1500, "change": -10}]
    assert next(r for r in exported[2026] if r["team"] == "Florida") == {
        "team": "Florida", "elo": 1500.2, "change": 7,
    }
    assert next(r for r in exported[2026] if r["team"] == "Georgia")["change"] == 5
