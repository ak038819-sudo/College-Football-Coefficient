"""Phase 3 backfill audit tests using synthetic Phase 2 staging schema."""
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

path = Path(__file__).resolve().parents[1] / "scripts/audit_historical_backfill.py"
spec = importlib.util.spec_from_file_location("backfill_audit", path)
audit_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_module)

SCHEMA = """
    CREATE TABLE archive_fetches (season INTEGER, season_type TEXT, status TEXT, record_count INTEGER);
    CREATE TABLE archive_games (
        source_game_id TEXT, season INTEGER, season_type TEXT,
        home_name_raw TEXT, away_name_raw TEXT, home_score INTEGER,
        away_score INTEGER, home_classification TEXT,
        away_classification TEXT, start_date TEXT
    );
"""


def make_db():
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    return db


def partition(rows, season, season_type):
    """The one audited partition for a season and season type."""
    matches = [r for r in rows if r["season"] == season and r["season_type"] == season_type]
    assert len(matches) == 1, f"expected one {season} {season_type} partition, got {len(matches)}"
    return matches[0]


def test_complete_season_preserves_ties_and_unknown_classification():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1946,'regular','complete',1)")
        db.execute("""INSERT INTO archive_games VALUES
                   ('g1',1946,'regular','A','B',7,7,NULL,NULL,'1946-10-01')""")
        rows = audit_module.audit(db, 1946, 1946)
        regular = partition(rows, 1946, "regular")
        assert regular["ready_for_source_reconciliation"]
        assert regular["blockers"] == ""
        assert regular["ties"] == 1
        assert regular["unknown_classification"] == 1
        assert regular["missing_scores"] == 0
        assert regular["missing_dates"] == 0
        # A season with no postseason fetch at all is blocked, but is not
        # accused of being an empty regular season.
        postseason = partition(rows, 1946, "postseason")
        assert postseason["blockers"] == "checkpoint_incomplete;source_count_mismatch"
        assert postseason["checkpoint"] == "missing"


def test_mismatched_count_and_invalid_team_block_partition():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1947,'regular','complete',2)")
        db.execute("""INSERT INTO archive_games VALUES
                   ('g2',1947,'regular','A','A',NULL,NULL,NULL,NULL,NULL)""")
        row = partition(audit_module.audit(db, 1947, 1947), 1947, "regular")
        assert "source_count_mismatch" in row["blockers"]
        assert "missing_or_invalid_teams" in row["blockers"]
        assert row["fetched_count"] == 2
        assert row["staged_count"] == 1
        assert row["missing_scores"] == 1
        assert row["missing_dates"] == 1
        assert not row["ready_for_source_reconciliation"]


def test_duplicate_source_id_blocks_partition():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1950,'regular','complete',2)")
        db.executemany("INSERT INTO archive_games VALUES (?,1950,'regular','A','B',21,14,'fbs','fbs','1950-10-07')",
                       [("dup",), ("dup",)])
        row = partition(audit_module.audit(db, 1950, 1950), 1950, "regular")
        assert row["duplicate_ids"] == 1
        assert row["missing_ids"] == 0
        assert "invalid_source_ids" in row["blockers"]
        assert not row["ready_for_source_reconciliation"]


def test_blank_source_ids_count_as_missing_not_duplicate():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1951,'regular','complete',2)")
        db.executemany("INSERT INTO archive_games VALUES (?,1951,'regular','A','B',21,14,'fbs','fbs','1951-10-06')",
                       [(None,), (None,)])
        row = partition(audit_module.audit(db, 1951, 1951), 1951, "regular")
        assert row["missing_ids"] == 2
        assert row["duplicate_ids"] == 0
        assert "invalid_source_ids" in row["blockers"]


def test_empty_regular_season_is_flagged_for_source_review():
    with make_db() as db:
        # A fetch that honestly reported zero games is still not evidence that
        # the season had none: 1946-1979 coverage has to be checked by hand.
        db.execute("INSERT INTO archive_fetches VALUES (1952,'regular','complete',0)")
        db.execute("INSERT INTO archive_fetches VALUES (1952,'postseason','complete',0)")
        rows = audit_module.audit(db, 1952, 1952)
        regular = partition(rows, 1952, "regular")
        assert regular["staged_count"] == 0
        assert regular["blockers"] == "empty_regular_season_requires_source_review"
        assert not regular["ready_for_source_reconciliation"]
        # An empty postseason is normal for much of the range, so a complete
        # fetch with no bowl games is not a blocker.
        postseason = partition(rows, 1952, "postseason")
        assert postseason["blockers"] == ""
        assert postseason["ready_for_source_reconciliation"]


def test_failed_checkpoint_blocks_partition():
    with make_db() as db:
        db.execute("INSERT INTO archive_fetches VALUES (1955,'regular','failed',1)")
        db.execute("""INSERT INTO archive_games VALUES
                   ('g3',1955,'regular','A','B',10,3,'fbs','fbs','1955-09-24')""")
        row = partition(audit_module.audit(db, 1955, 1955), 1955, "regular")
        assert row["checkpoint"] == "failed"
        assert "checkpoint_incomplete" in row["blockers"]
        assert "source_count_mismatch" not in row["blockers"]


def test_audit_covers_every_requested_season_and_type():
    with make_db() as db:
        rows = audit_module.audit(db, 1946, 1948)
        assert len(rows) == 6
        assert [(r["season"], r["season_type"]) for r in rows] == [
            (1946, "regular"), (1946, "postseason"),
            (1947, "regular"), (1947, "postseason"),
            (1948, "regular"), (1948, "postseason"),
        ]


@pytest.mark.parametrize("first,last", [(1945, 1946), (1946, 1980), (1950, 1949)])
def test_invalid_range(first, last):
    with make_db() as db:
        with pytest.raises(ValueError):
            audit_module.audit(db, first, last)


def write_staging_db(path):
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    db.execute("INSERT INTO archive_fetches VALUES (1946,'regular','complete',2)")
    db.execute("INSERT INTO archive_fetches VALUES (1946,'postseason','complete',1)")
    db.executescript("""
        INSERT INTO archive_games VALUES ('g1',1946,'regular','A','B',7,7,NULL,NULL,'1946-10-01');
        INSERT INTO archive_games VALUES ('g2',1946,'regular','C','D',14,0,'fbs','fbs',NULL);
        INSERT INTO archive_games VALUES ('g3',1946,'postseason','E','F',21,20,'fbs','fbs','1947-01-01');
    """)
    db.commit()
    db.close()


def run_main(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["audit_historical_backfill.py", *argv])
    audit_module.main()


def test_main_writes_report_and_summary(tmp_path, monkeypatch, capsys):
    staging = tmp_path / "historical_staging.db"
    write_staging_db(staging)
    out = tmp_path / "report"
    run_main(monkeypatch, ["--db", str(staging), "--out", str(out),
                           "--start", "1946", "--end", "1946"])
    summary = json.loads((out / "backfill_summary.json").read_text())
    assert summary["years"] == [1946, 1946]
    assert summary["total_staged_games"] == 3
    assert summary["ties"] == 1
    assert summary["missing_dates"] == 1
    assert summary["unknown_classification"] == 1
    assert summary["production_promotion_allowed"] is False
    # 1946 regular is two staged games against a fetch of two, so it is clean;
    # the postseason partition is clean too, leaving nothing blocked.
    assert summary["blocked_partitions"] == 0
    assert json.loads(capsys.readouterr().out) == summary
    report = (out / "backfill_readiness.csv").read_text().splitlines()
    assert report[0].startswith("season,season_type,checkpoint")
    assert len(report) == 3  # header plus both 1946 partitions


def test_main_refuses_production_database(tmp_path, monkeypatch):
    production = tmp_path / "league.db"
    write_staging_db(production)
    with pytest.raises(SystemExit):
        run_main(monkeypatch, ["--db", str(production), "--out", str(tmp_path / "report")])
    assert not (tmp_path / "report").exists()


def test_main_leaves_the_staging_database_unchanged(tmp_path, monkeypatch):
    staging = tmp_path / "historical_staging.db"
    write_staging_db(staging)
    before = staging.read_bytes()
    run_main(monkeypatch, ["--db", str(staging), "--out", str(tmp_path / "report"),
                           "--start", "1946", "--end", "1946"])
    assert staging.read_bytes() == before


def test_main_opens_a_connection_that_cannot_write(tmp_path, monkeypatch):
    """The connection main() actually opens must itself refuse writes."""
    staging = tmp_path / "historical_staging.db"
    write_staging_db(staging)
    opened = []
    real_connect = sqlite3.connect

    def recording_connect(*args, **kwargs):
        opened.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(audit_module.sqlite3, "connect", recording_connect)
    run_main(monkeypatch, ["--db", str(staging), "--out", str(tmp_path / "report"),
                           "--start", "1946", "--end", "1946"])
    assert len(opened) == 1
    args, kwargs = opened[0]
    probe = real_connect(*args, **kwargs)
    try:
        with pytest.raises(sqlite3.OperationalError):
            probe.execute("INSERT INTO archive_games VALUES "
                          "('x',1946,'regular','X','Y',1,0,'fbs','fbs','1946-10-01')")
    finally:
        probe.close()
