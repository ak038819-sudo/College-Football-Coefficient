"""Smoke tests for read-only historical audits (synthetic DB, no CFBD key)."""
import csv
import importlib.util
import json
import sqlite3
import sys
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


def test_stale_alias_csv_is_rewritten_when_nothing_is_unresolved(tmp_path, monkeypatch):
    """A previous run's unresolved names must not survive beside a fresh empty report."""
    baselines_path = Path(__file__).resolve().parents[1] / "scripts" / "audit_historical_baselines.py"
    spec_b = importlib.util.spec_from_file_location("historical_baselines", baselines_path)
    baselines = importlib.util.module_from_spec(spec_b)
    spec_b.loader.exec_module(baselines)

    db_path = tmp_path / "league.db"
    con = sqlite3.connect(db_path)
    con.executescript("""
        CREATE TABLE games(game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
                           away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        INSERT INTO games VALUES (1, 2025, 1, 2, 21, 14);
    """)
    con.commit()
    con.close()

    out = tmp_path / "out"
    out.mkdir()
    raw = tmp_path / "raw"
    raw.mkdir()
    stale = out / "unresolved_aliases.csv"
    stale.write_text("season,raw_name,games\n2001,Stale Name,5\n", encoding="utf-8")

    monkeypatch.setattr(sys, "argv", ["audit_historical_baselines.py", "--db", str(db_path),
                                      "--raw-dir", str(raw), "--out", str(out)])
    baselines.main()

    rows = list(csv.DictReader(stale.open(newline="", encoding="utf-8")))
    assert rows == []
    assert stale.read_text(encoding="utf-8").splitlines()[0] == "season,raw_name,games"
    report = json.loads((out / "baseline_fingerprints.json").read_text(encoding="utf-8"))
    assert report["unresolved_aliases"] == []
    assert len(list(csv.DictReader((out / "raw_vs_loaded.csv").open(newline="", encoding="utf-8")))) == 47
