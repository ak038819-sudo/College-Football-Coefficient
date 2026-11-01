"""Descriptive real-data audit must not invent percentiles on empty samples."""
import sqlite3
from src.audit_matchup_descriptive_research import audit


def test_empty_eligible_season_is_explicitly_unavailable():
    db=sqlite3.connect(":memory:")
    db.executescript("""
      CREATE TABLE teams (team_id INTEGER, team_name TEXT);
      CREATE TABLE games (game_id INTEGER, season_year INTEGER, game_date TEXT,
        home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER,
        away_score INTEGER, neutral_site INTEGER);
      CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
      CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER, season_year INTEGER,
        off_rush_success_rate REAL, off_pass_success_rate REAL,
        off_rush_ppa REAL, off_pass_ppa REAL,
        off_rush_explosiveness REAL, off_pass_explosiveness REAL);
    """)
    report=audit(db,season=2025,as_of="2026-02-01",sample_size=2)
    assert all(x["status"]=="no_data" for x in report["metrics"].values())
    db.close()


def test_invalid_research_sample_size_rejected():
    db=sqlite3.connect(":memory:")
    try:
        audit(db, season=2025, as_of="2026-02-01", sample_size=0)
    except ValueError:
        pass
    else:
        raise AssertionError("sample size of zero accepted")
    db.close()
