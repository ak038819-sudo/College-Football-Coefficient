"""Point-in-time game adapter integration tests on a synthetic SQLite database."""
import sqlite3

import pytest

from src.build_matchup_intelligence import build_snapshot, load_observations


@pytest.fixture
def db():
    c = sqlite3.connect(":memory:")
    c.executescript("""
        CREATE TABLE games (
          game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
          home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER,
          away_score INTEGER
        );
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE team_membership_by_season (
          team_id INTEGER, season_year INTEGER, is_fbs INTEGER
        );
        CREATE TABLE game_team_advanced (
          game_id INTEGER, team_id INTEGER, season_year INTEGER,
          off_success_rate REAL, off_ppa REAL, off_explosiveness REAL
        );
        INSERT INTO teams VALUES (1,'A'),(2,'B'),(3,'C'),(4,'FCS');
        INSERT INTO team_membership_by_season VALUES
          (1,2026,1),(2,2026,1),(3,2026,1),(4,2026,0);
        INSERT INTO games VALUES
          (10,2026,'2026-09-05T15:00:00Z',1,2,24,17),
          (11,2026,'2026-09-12',2,3,17,21),
          (12,2026,'2026-09-19',3,1,10,31),
          (13,2026,'2026-10-10',1,2,NULL,NULL),
          (14,2026,'2026-09-01',1,4,24,7),
          (15,2026,'2026-09-12',1,2,20,17),
          (16,2026,NULL,1,2,20,17),
          (17,2026,'2026-09-20',2,3,28,10);
        INSERT INTO game_team_advanced VALUES
          (10,1,2026,.52,.12,.75),(10,2,2026,.38,-.08,.50),
          (11,2,2026,.42,.01,.60),(11,3,2026,.57,.21,.80),
          (12,3,2026,.39,-.05,.55),(12,1,2026,.61,.30,.90),
          (13,1,2026,.99,.99,.99),(14,1,2026,.95,.95,.95),
          (15,1,2026,.55,.15,.75),(15,2,2026,.40,-.01,.59),
          (16,1,2026,.90,.90,.90),
          (17,2,2026,.80,.70,.70),(17,3,2026,.20,-.50,.40);
    """)
    yield c
    c.close()


def test_pre_game_cutoff_excludes_same_day_and_future(db):
    rows, coverage = load_observations(
        db, season=2026, as_of="2026-09-12", metric="success_rate")
    assert {(r.team, r.opponent) for r in rows} == {("1","2"),("2","1")}
    assert coverage["eligible_rows"] == 2
    assert coverage["future_or_same_day"] >= 4


def test_finished_only_fbs_and_invalid_date(db):
    rows, coverage = load_observations(
        db, season=2026, as_of="2026-10-31", metric="success_rate")
    assert len(rows) == 10
    assert coverage["missing_or_invalid_date"] == 1
    assert all(r.team != "4" and r.opponent != "4" for r in rows)


def test_missing_metric_is_not_zero(db):
    db.execute("UPDATE game_team_advanced SET off_ppa=NULL WHERE game_id=10 AND team_id=1")
    rows, coverage = load_observations(
        db, season=2026, as_of="2026-10-31", metric="ppa")
    assert len(rows) == 9
    assert coverage["missing_value"] == 1


def test_snapshot_is_not_eligible_with_fewer_than_three_games(db):
    result = build_snapshot(db, season=2026, as_of="2026-09-12", metric="success_rate")
    assert result["profiles"] == {}
    assert result["coverage"]["eligible_rows"] == 2


def test_reject_unsupported_metric(db):
    with pytest.raises(ValueError):
        load_observations(db, season=2026, as_of="2026-10-31", metric="off_plays;DROP TABLE games")


def test_reject_invalid_date(db):
    with pytest.raises(ValueError):
        load_observations(db, season=2026, as_of="tomorrow", metric="ppa")


def test_reject_one_sided_matchup(db):
    with pytest.raises(ValueError):
        build_snapshot(db, season=2026, as_of="2026-10-31",
                       metric="ppa", offensive_team_id=1)
