"""Expanded descriptive stability research: stable cohorts and ranked sampling."""
import sqlite3
import pytest
from src.analyze_matchup_percentile_stability import (
    percentile, sampled_across_distribution, summarize_sensitivity, analyze
)


def test_quantile_and_rank_stratified_sampling():
    assert percentile([], .5) is None
    assert percentile([10,20,30,40], .5) == 25
    teams={str(i): {"rating_eligible": True,
                     "offensive_percentile": float(i)} for i in range(20)}
    sample=sampled_across_distribution(teams,5)
    assert len(sample)==5
    assert sample[0]=="0" and sample[-1]=="19"


def test_sensitivity_denominator_and_missing_values():
    d=summarize_sensitivity({
      "A":{"status":"evaluated",
           "max_offense_percentile_change":12.0,
           "max_defense_percentile_change":23.0},
      "B":{"status":"insufficient","checks":0},
    })
    assert d["evaluated"]==1 and d["requested"]==2
    assert d["max_offense_percentile_change"]["at_least_10_points"]==1
    assert d["max_defense_percentile_change"]["at_least_20_points"]==1


def test_no_missing_data_fabricated():
    conn=sqlite3.connect(":memory:")
    conn.executescript("""
    CREATE TABLE teams(team_id INTEGER, team_name TEXT);
    CREATE TABLE games(game_id INTEGER, season_year INTEGER, game_date TEXT,
        home_team_id INTEGER, away_team_id INTEGER,
        home_score INTEGER, away_score INTEGER, neutral_site INTEGER);
    CREATE TABLE team_membership_by_season(team_id INTEGER, season_year INTEGER, is_fbs INTEGER);
    CREATE TABLE game_team_advanced(game_id INTEGER, team_id INTEGER,
        season_year INTEGER, off_rush_success_rate REAL, off_pass_success_rate REAL,
        off_rush_ppa REAL, off_pass_ppa REAL,
        off_rush_explosiveness REAL, off_pass_explosiveness REAL);
    """)
    result=analyze(conn, season=2025, dates=("2025-09-01", "2025-10-01"))
    assert all(r["status"]=="no_data" for x in result["metric_reports"].values() for r in x)
    with pytest.raises(ValueError):
        analyze(conn, season=2025, dates=("2025-10-01","2025-09-01"))
    conn.close()
