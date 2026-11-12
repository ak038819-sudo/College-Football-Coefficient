"""Smoke tests for read-only historical audits (synthetic DB, no CFBD key)."""
import csv
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

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
    baselines = _load_baselines()
    db_path = tmp_path / "league.db"
    _synthetic_db(db_path, baselines.MODELS)

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


def _load_baselines():
    path = Path(__file__).resolve().parents[1] / "scripts" / "audit_historical_baselines.py"
    spec = importlib.util.spec_from_file_location("historical_baselines", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _synthetic_db(path, model_tables=()):
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE games(game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
                           away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        INSERT INTO games VALUES (1, 2025, 1, 2, 21, 14);
    """)
    for table in model_tables:
        con.execute(f'CREATE TABLE "{table}"(team_id INTEGER, value REAL)')
        con.execute(f'INSERT INTO "{table}" VALUES (1, 1.5)')
    con.commit()
    con.close()


def test_legacy_coe_v1_table_is_not_claimed_as_a_baseline():
    """team_coefficient_by_year lives in a SQL file no build runs, so it is not a baseline."""
    baselines = _load_baselines()
    assert "team_coefficient_by_year" not in baselines.MODELS
    assert set(baselines.MODELS) == {"elo_game_history", "hybrid_game_ratings",
                                     "team_coe2_by_season", "team_coe_5yr_by_season"}


def test_absent_baseline_table_fails_instead_of_fingerprinting_none(tmp_path, monkeypatch):
    """A None fingerprint must not pass for a baseline just because nothing complained."""
    baselines = _load_baselines()
    db_path = tmp_path / "league.db"
    # Every baseline table but one, so the refusal names the absent one specifically.
    _synthetic_db(db_path, [t for t in baselines.MODELS if t != "hybrid_game_ratings"])
    out = tmp_path / "out"
    out.mkdir()
    raw = tmp_path / "raw"
    raw.mkdir()
    argv = ["audit_historical_baselines.py", "--db", str(db_path),
            "--raw-dir", str(raw), "--out", str(out)]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as refused:
        baselines.main()
    assert "hybrid_game_ratings" in str(refused.value)

    # The reports are still written, so the refusal is reviewable rather than blind.
    report = json.loads((out / "baseline_fingerprints.json").read_text(encoding="utf-8"))
    assert report["missing_model_tables"] == ["hybrid_game_ratings"]
    assert report["model_tables"]["hybrid_game_ratings"] is None
    assert report["model_tables"]["team_coe2_by_season"]["rows"] == 1

    # --allow-missing-tables downgrades the same state to a warning.
    monkeypatch.setattr(sys, "argv", argv + ["--allow-missing-tables"])
    baselines.main()


def test_a_complete_database_reports_no_missing_baseline_tables(tmp_path, monkeypatch):
    baselines = _load_baselines()
    db_path = tmp_path / "league.db"
    _synthetic_db(db_path, baselines.MODELS)
    out = tmp_path / "out"
    out.mkdir()
    raw = tmp_path / "raw"
    raw.mkdir()
    monkeypatch.setattr(sys, "argv", ["audit_historical_baselines.py", "--db", str(db_path),
                                      "--raw-dir", str(raw), "--out", str(out)])
    baselines.main()
    report = json.loads((out / "baseline_fingerprints.json").read_text(encoding="utf-8"))
    assert report["missing_model_tables"] == []
    assert all(report["model_tables"][t]["rows"] == 1 for t in baselines.MODELS)
