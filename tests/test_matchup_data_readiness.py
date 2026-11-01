"""Missing rushing/passing source data must fail closed."""
import sqlite3

from src.check_matchup_data_readiness import readiness_report
from src.build_matchup_intelligence import load_observations


def database():
    c = sqlite3.connect(":memory:")
    c.executescript("""
    CREATE TABLE teams (team_id INTEGER, team_name TEXT);
    CREATE TABLE games (game_id INTEGER, season_year INTEGER, game_date TEXT,
      home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER,
      away_score INTEGER, neutral_site INTEGER);
    CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
    CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER,
      season_year INTEGER, off_success_rate REAL, off_ppa REAL,
      off_explosiveness REAL, off_plays INTEGER, off_rush_success_rate REAL);
    INSERT INTO teams VALUES (1,'A'),(2,'B');
    INSERT INTO games VALUES (10,2026,'2026-09-01',1,2,21,14,0);
    INSERT INTO team_membership_by_season VALUES (1,2026,1),(2,2026,1);
    INSERT INTO game_team_advanced VALUES
      (10,1,2026,1.4,0.1,1.2,70,0.0),
      (10,2,2026,0.4,0.0,0.8,65,0.5);
    """)
    return c


def test_reject_invalid_success_rate_but_preserve_real_zero():
    c = database()
    rows, coverage = load_observations(
        c, season=2026, as_of="2026-10-01", metric="success_rate")
    assert len(rows) == 1
    assert rows[0].team == "2"
    assert coverage["invalid_metric_value"] == 1
    splits, _ = load_observations(
        c, season=2026, as_of="2026-10-01", metric="rush_success_rate")
    assert len(splits) == 2
    assert splits[0].value == 0
    c.close()


def test_readiness_requires_every_split_and_never_promises_model_quality():
    c = database()
    report = readiness_report(c, (2026,))
    assert report["production_ready"] is False
    assert report["coverage_gate_passed"] is False
    assert any("pass_ppa" in reason for reason in report["blockers"])
    c.close()
