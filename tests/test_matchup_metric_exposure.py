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
