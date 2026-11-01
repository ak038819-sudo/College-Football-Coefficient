"""Protect against false rushing/passing exposure denominators."""
import pytest
from src.matchup_metric_exposure import METRIC_EXPOSURE, exposure_policy
from src.matchup_intelligence import Observation
from src.build_matchup_intelligence import build_snapshot


def test_all_metric_policies_are_explicit():
    assert len(METRIC_EXPOSURE)==9
    for name in METRIC_EXPOSURE:
        policy=exposure_policy(name,{"off_plays"})
        assert policy["metric"]==name
        if name in ("success_rate","ppa"):
            assert policy["weighting"]=="play_count"
            assert policy["denominator_field"]=="off_plays"
        else:
            assert policy["weighting"]=="game_equal"
            assert policy["denominator_field"] is None
            assert policy["warning"]


def test_explosiveness_is_not_a_mean_per_all_offensive_plays():
    policy=exposure_policy("explosiveness",{"off_plays"})
    assert policy["exposure_unit"]=="conditional_successful_plays"
    assert policy["denominator_status"]=="not_supplied_by_source"


def test_split_rate_never_borrows_overall_count():
    for name in ("rush_success_rate","pass_success_rate","rush_ppa","pass_ppa"):
        p=exposure_policy(name,{"off_plays","off_rush_ppa"})
        assert p["denominator_field"] is None
        assert p["weighting"]=="game_equal"


def test_missing_overall_plays_fails_to_game_equal_without_fake_number():
    p=exposure_policy("success_rate",set())
    assert p["denominator_status"]=="unavailable"
    assert p["weighting"]=="game_equal"
    assert p["denominator_field"] is None


def test_unknown_metric_rejected():
    with pytest.raises(ValueError):
        exposure_policy("score",{"off_plays"})


def test_actual_observation_weights_match_the_correct_metric_denominator():
    import sqlite3
    from src.build_matchup_intelligence import load_observations
    db=sqlite3.connect(":memory:")
    db.executescript("""
       CREATE TABLE teams(team_id INTEGER,team_name TEXT);
       CREATE TABLE games(game_id INTEGER,season_year INTEGER,game_date TEXT,
         home_team_id INTEGER,away_team_id INTEGER,home_score INTEGER,
         away_score INTEGER,neutral_site INTEGER);
       CREATE TABLE team_membership_by_season(team_id INTEGER,season_year INTEGER,is_fbs INTEGER);
       CREATE TABLE game_team_advanced(
         game_id INTEGER,team_id INTEGER,season_year INTEGER,
         off_plays INTEGER, off_success_rate REAL,off_ppa REAL,
         off_explosiveness REAL,off_rush_success_rate REAL,
         off_pass_ppa REAL,off_rush_explosiveness REAL);
       INSERT INTO teams VALUES (1,'A'),(2,'B');
       INSERT INTO team_membership_by_season VALUES (1,2026,1),(2,2026,1);
       INSERT INTO games VALUES (1,2026,'2026-09-01',1,2,21,14,0);
       INSERT INTO game_team_advanced VALUES
          (1,1,2026,95,.60,.14,1.35,.58,.08,1.9),
          (1,2,2026,38,.35,-.09,.70,.36,-.02,.9);
    """)
    expected={
       "success_rate": [95.,38.],
       "ppa":[95.,38.],
       "explosiveness":[1.,1.],
       "rush_success_rate":[1.,1.],
       "pass_ppa":[1.,1.],
       "rush_explosiveness":[1.,1.],
    }
    for metric,weights in expected.items():
        rows,coverage=load_observations(db,season=2026,as_of="2026-09-02",metric=metric)
        assert [r.weight for r in rows]==weights
        assert coverage["exposure"]["weighting"]==("play_count" if metric in ("success_rate","ppa") else "game_equal")
    db.close()
