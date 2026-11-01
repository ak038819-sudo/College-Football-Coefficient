"""Regression coverage for CFBD's true rushing and passing game splits."""
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fetch_cfbd_game_advanced import CSV_FIELDS, game_advanced_rows
from load_game_advanced import ensure_split_columns, SPLIT_COLS


def test_cfbd_nested_split_fields_preserve_zero_and_missing():
    payload = [{
        "season": 2026, "gameId": 123, "team": "Alpha", "opponent": "Beta",
        "offense": {
            "successRate": 0.50,
            "rushingPlays": {"successRate": 0.60, "ppa": 0.11, "explosiveness": 1.2},
            "passingPlays": {"successRate": 0.0, "ppa": -0.2},
        },
        "defense": {
            "rushingPlays": {"successRate": 0.3, "ppa": -0.1, "explosiveness": 0.7},
            "passingPlays": {"successRate": 0.45, "ppa": 0.12, "explosiveness": 1.1},
        },
    }]
    rows = game_advanced_rows(payload, 2026)
    assert len(rows) == 1
    row = rows[0]
    assert row["off_rush_success_rate"] == 0.60
    assert row["off_pass_success_rate"] == 0.0
    assert row["off_pass_explosiveness"] == ""
    assert row["def_rush_ppa"] == -0.1
    assert row["def_pass_explosiveness"] == 1.1
    assert set(SPLIT_COLS).issubset(CSV_FIELDS)


def test_split_migration_on_legacy_table_is_idempotent():
    c = sqlite3.connect(":memory:")
    c.execute("""CREATE TABLE game_team_advanced (
        game_id INTEGER NOT NULL, team_id INTEGER NOT NULL,
        off_success_rate REAL, PRIMARY KEY (game_id, team_id))""")
    c.execute("INSERT INTO game_team_advanced VALUES (100, 1, 0.55)")
    ensure_split_columns(c)
    ensure_split_columns(c)
    have = {r[1] for r in c.execute("PRAGMA table_info(game_team_advanced)")}
    assert set(SPLIT_COLS).issubset(have)
    assert c.execute("SELECT off_success_rate FROM game_team_advanced").fetchone()[0] == 0.55


def test_parser_skips_missing_game_id():
    assert game_advanced_rows([{"team": "Alpha", "offense": {"rushingPlays": {"ppa": 0.3}}}], 2026) == []
