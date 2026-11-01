"""Descriptive-only contract: no forecast, safe missingness, truthful labels."""
import sqlite3
import pytest
from src.export_matchup_descriptive import build_descriptive_contract, compare_units


def db_fixture():
    c=sqlite3.connect(":memory:")
    c.executescript("""
    CREATE TABLE teams(team_id INTEGER PRIMARY KEY,team_name TEXT);
    CREATE TABLE games(game_id INTEGER PRIMARY KEY,season_year INTEGER,game_date TEXT,
      home_team_id INTEGER,away_team_id INTEGER,home_score INTEGER,
      away_score INTEGER,neutral_site INTEGER DEFAULT 0);
    CREATE TABLE team_membership_by_season(team_id INTEGER,season_year INTEGER,is_fbs INTEGER);
    CREATE TABLE game_team_advanced(game_id INTEGER,team_id INTEGER,season_year INTEGER,
      off_success_rate REAL, off_plays INTEGER, off_rush_success_rate REAL);
    INSERT INTO teams VALUES(1,'Alpha'),(2,'Beta'),(3,'Gamma'),(4,'Delta');
    INSERT INTO team_membership_by_season VALUES(1,2026,1),(2,2026,1),(3,2026,1),(4,2026,1);
    INSERT INTO games VALUES
      (1,2026,'2026-09-01',1,2,24,14,0),
      (2,2026,'2026-09-08',3,4,28,20,0),
      (3,2026,'2026-09-15',1,3,21,14,0),
      (4,2026,'2026-09-22',2,4,17,21,0),
      (5,2026,'2026-09-29',1,4,31,20,0),
      (6,2026,'2026-09-30',2,3,21,28,0);
    INSERT INTO game_team_advanced VALUES
      (1,1,2026,.60,80,.56),(1,2,2026,.42,70,.40),
      (2,3,2026,.55,75,.52),(2,4,2026,.43,60,.41),
      (3,1,2026,.58,72,.55),(3,3,2026,.49,66,.48),
      (4,2,2026,.53,77,.51),(4,4,2026,.42,58,.40),
      (5,1,2026,.62,81,.60),(5,4,2026,.41,67,.39),
      (6,2,2026,.50,80,.46),(6,3,2026,.46,64,.44);
    """)
    return c


def test_descriptive_contract_and_opposing_units_without_prediction():
    c=db_fixture()
    bundle=build_descriptive_contract(c,season=2026,as_of="2026-10-01",
                                      team_ids=(1,2),metrics=("success_rate","rush_success_rate"),
                                      min_population=4)
    assert bundle["schema_version"]=="elocast-descriptive-v1"
    assert bundle["metric_definitions"]["success_rate"]["observation_weighting"]=="play_count"
    assert bundle["metric_definitions"]["rush_success_rate"]["observation_weighting"]=="game_equal"
    a=bundle["teams"]["1"]["metrics"]["rush_success_rate"]
    assert a["status"]=="rated"
    assert a["distinct_opponents"]==3
    assert a["reliability"]=="provisional"
    compare=compare_units(bundle,offense_team_id=1,defense_team_id=2)
    assert compare["metrics"]["rush_success_rate"]["projection"] is None
    assert compare["metrics"]["rush_success_rate"]["offense"]["raw"] is not None
    assert "win_probability" not in str(compare)
    c.close()


def test_population_gate_hides_adjustment_but_keeps_raw():
    c=db_fixture()
    r=build_descriptive_contract(c,season=2026,as_of="2026-10-01",
                                 team_ids=(1,),metrics=("rush_success_rate",),min_population=30)
    row=r["teams"]["1"]["metrics"]["rush_success_rate"]
    assert row["raw_offense"] is not None
    assert row["adjusted_offense"] is None
    assert row["offense_percentile"] is None
    assert r["metric_definitions"]["rush_success_rate"]["status"]=="insufficient_population"
    c.close()


def test_team_request_and_comparison_validation():
    c=db_fixture()
    with pytest.raises(ValueError):
        build_descriptive_contract(c,season=2026,as_of="2026-10-01",team_ids=(1,1))
    with pytest.raises(ValueError):
        build_descriptive_contract(c,season=2026,as_of="2026-10-01",team_ids=(999,))
    bundle=build_descriptive_contract(c,season=2026,as_of="2026-10-01",team_ids=(1,2),
                                      metrics=("rush_success_rate",),min_population=4)
    with pytest.raises(ValueError):
        compare_units(bundle,offense_team_id=1,defense_team_id=1)
    c.close()
