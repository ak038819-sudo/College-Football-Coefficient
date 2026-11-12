"""Phase 3 backfill audit tests using synthetic Phase 2 staging schema."""
import importlib.util
import sqlite3
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "scripts/audit_historical_backfill.py"
spec = importlib.util.spec_from_file_location("backfill_audit", path)
audit_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_module)


def make_db():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE archive_fetches (season INTEGER, season_type TEXT, status TEXT, record_count INTEGER);
        CREATE TABLE archive_games (
            source_game_id TEXT, season INTEGER, season_type TEXT,
            home_name_raw TEXT, away_name_raw TEXT, home_score INTEGER,
            away_score INTEGER, home_classification TEXT,
            away_classification TEXT, start_date TEXT
        );
    """)
    return db


def test_complete_season_preserves_ties_and_unknown_classification():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1946,'regular','complete',1)")
        db.execute("""INSERT INTO archive_games VALUES
                   ('g1',1946,'regular','A','B',7,7,NULL,NULL,'1946-10-01')""")
        rows = audit_module.audit(db, 1946, 1946)
        assert rows[0]["ready_for_source_reconciliation"]
        assert rows[0]["ties"] == 1
        assert rows[0]["unknown_classification"] == 1
        assert rows[1]["blockers"] == "checkpoint_incomplete;source_count_mismatch"


def test_mismatched_count_and_invalid_team_block_partition():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1947,'regular','complete',2)")
        db.execute("""INSERT INTO archive_games VALUES
                   ('g2',1947,'regular','A','A',NULL,NULL,NULL,NULL,NULL)""")
        row = audit_module.audit(db, 1947, 1947)[0]
        assert "source_count_mismatch" in row["blockers"]
        assert "missing_or_invalid_teams" in row["blockers"]
        assert row["missing_scores"] == 1


def test_invalid_range():
    with make_db() as db:
        try:
            audit_module.audit(db, 1945, 1946)
        except ValueError:
            pass
        else:
            raise AssertionError("Out-of-range seasons must be rejected")
