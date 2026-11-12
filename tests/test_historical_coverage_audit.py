"""Smoke tests for read-only historical audits (synthetic DB, no CFBD key)."""
import importlib.util
import sqlite3
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "audit_historical_coverage.py"
spec = importlib.util.spec_from_file_location("historical_audit", SCRIPT)
audit_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_module)


def test_historical_audit_counts_ties_null_ids_and_2020():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE games(game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
                           away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        CREATE TABLE team_membership_by_season(team_id INTEGER, season_year INTEGER);
        INSERT INTO games VALUES (1, 2020, 1, 2, 7, 7);
        INSERT INTO games VALUES (1, 2020, 1, 2, 7, 7);
        INSERT INTO games VALUES (NULL, 2020, 3, 3, NULL, NULL);
        INSERT INTO team_membership_by_season VALUES (1, 2020);
    """)
    row = next(x for x in audit_module.audit(db) if x["season"] == 2020)
    assert row["games"] == 3
    assert row["distinct_game_ids"] == 1
    assert row["missing_game_ids"] == 1
    assert row["duplicate_game_ids"] == 1
    assert row["missing_scores"] == 1
    assert row["ties"] == 2
    assert row["self_matchups"] == 1
    assert row["teams_without_membership"] == 2


def test_empty_season_is_reported_not_silently_omitted():
    db = sqlite3.connect(":memory:")
    db.execute("""CREATE TABLE games(game_id INTEGER, season_year INTEGER,
                 home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER)""")
    rows = audit_module.audit(db)
    assert len(rows) == 47
    assert rows[0]["season"] == 1980 and rows[-1]["season"] == 2026
    assert all(row["games"] == 0 for row in rows)
