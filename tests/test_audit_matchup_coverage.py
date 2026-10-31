"""Synthetic data coverage audit: paired metrics and missing schema."""
import sqlite3

from src.audit_matchup_coverage import audit_coverage


def test_coverage_requires_actual_observed_sides():
    c=sqlite3.connect(":memory:")
    c.executescript("""
    CREATE TABLE games (game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
                        away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
    CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
    CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER,
                                    season_year INTEGER, off_success_rate REAL);
    INSERT INTO team_membership_by_season VALUES (1,2026,1),(2,2026,1),(3,2026,1);
    INSERT INTO games VALUES (10,2026,1,2,20,17),(11,2026,1,3,14,7),
                             (12,2026,1,2,NULL,NULL);
    INSERT INTO game_team_advanced VALUES (10,1,2026,.5),(10,2,2026,.4),
                                           (11,1,2026,.6),(12,1,2026,.3);
    """)
    report = audit_coverage(c,2026)
    assert report["completed_fbs_games"] == 2
    assert report["metrics"]["success_rate"]["team_games"] == 3
    assert report["metrics"]["success_rate"]["paired_games"] == 1
    assert report["metrics"]["pass_ppa"]["status"] == "schema_missing"
    assert report["advanced_rows_unmatched_to_fbs_game_side"] == 1
    c.close()


def test_membership_gap_not_assumed_fbs():
    c=sqlite3.connect(":memory:")
    c.executescript("""
    CREATE TABLE games (game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
                        away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
    CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
    CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER,
                                    season_year INTEGER, off_success_rate REAL);
    INSERT INTO team_membership_by_season VALUES (1,2026,1);
    INSERT INTO games VALUES (22,2026,1,2,20,17);
    """)
    report = audit_coverage(c,2026)
    assert report["missing_membership_games"] == 1
    assert report["completed_fbs_games"] == 0
    c.close()
