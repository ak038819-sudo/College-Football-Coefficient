"""Stable, null-safe backend data contract before any UI decisions."""
import sqlite3
import pytest

from src.export_matchup_scouting import build_scouting_bundle


def fixture_db():
    db = sqlite3.connect(":memory:")
    db.executescript("""
    CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
    CREATE TABLE games (
       game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
       home_team_id INTEGER, away_team_id INTEGER,
       home_score INTEGER, away_score INTEGER, neutral_site INTEGER DEFAULT 0);
    CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
    CREATE TABLE game_team_advanced (
       game_id INTEGER, team_id INTEGER, season_year INTEGER,
       off_success_rate REAL, off_rush_success_rate REAL, off_plays INTEGER);
    INSERT INTO teams VALUES (1, 'Alpha'), (2, 'Bravo');
    INSERT INTO team_membership_by_season VALUES (1, 2026, 1), (2, 2026, 1);
    INSERT INTO games VALUES
      (1, 2026, '2026-09-01', 1, 2, 17, 10, 0),
      (2, 2026, '2026-09-08', 2, 1, 24, 20, 0),
      (3, 2026, '2026-09-15', 1, 2, 14, 10, 0),
      (4, 2026, '2026-09-22', 2, 1, 21, 20, 0);
    INSERT INTO game_team_advanced VALUES
      (1,1,2026,.60,.55,70),(1,2,2026,.40,.38,65),
      (2,1,2026,.55,.52,68),(2,2,2026,.42,.40,73),
      (3,1,2026,.58,.57,71),(3,2,2026,.43,.41,62),
      (4,1,2026,.57,.56,69),(4,2,2026,.44,.42,72);
    """)
    return db


def test_research_contract_and_two_opposing_units():
    db = fixture_db()
    data = build_scouting_bundle(
        db, season=2026, as_of="2026-10-01",
        home_team_id=1, away_team_id=2, metrics=("success_rate",),
        min_rated_teams=2)
    assert data["home_team"]["name"] == "Alpha"
    assert data["schema_version"] == "matchup-scouting-v1"
    metric = data["metrics"]["success_rate"]
    assert metric["status"] == "available"
    assert metric["rated_teams"] == 2
    assert metric["calibration"] == "existing_play_weighted_settings"
    assert metric["matchup_reliability"] == "provisional"
    assert metric["eligible_for_public_projection"] is False
    matchup = metric["matchup"]
    assert matchup["home_offense_vs_away_defense"]["estimate"] != matchup["away_offense_vs_home_defense"]["estimate"]
    db.close()


def test_no_misleading_percentiles_with_insufficient_population():
    db = fixture_db()
    data = build_scouting_bundle(
        db, season=2026, as_of="2026-10-01",
        metrics=("rush_success_rate",), min_rated_teams=30)
    metric = data["metrics"]["rush_success_rate"]
    assert metric["status"] == "unavailable"
    assert metric["team_profiles"] == {}
    assert metric["baseline"] is None
    db.close()


def test_reject_incomplete_or_duplicate_request():
    db = fixture_db()
    with pytest.raises(ValueError):
        build_scouting_bundle(db, season=2026, as_of="2026-10-01",
                              home_team_id=1)
    with pytest.raises(ValueError):
        build_scouting_bundle(db, season=2026, as_of="2026-10-01",
                              metrics=("success_rate", "success_rate"))
    db.close()
