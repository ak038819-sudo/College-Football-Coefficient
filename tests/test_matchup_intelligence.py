"""Tests for experimental scouting profiles (no CFBD API or database required)."""
import math
import pytest

from src.matchup_intelligence import Observation, fit_profiles, project_matchup


def sample():
    # Strong attack and strong defense are deliberately opposite teams.
    return [
        Observation(a, b, value)
        for a, b, value in [
            ("A", "B", 0.6), ("A", "C", 0.7), ("A", "D", 0.8),
            ("B", "A", 0.2), ("B", "C", 0.3), ("B", "D", 0.4),
            ("C", "A", 0.3), ("C", "B", 0.5), ("C", "D", 0.6),
            ("D", "A", 0.2), ("D", "B", 0.4), ("D", "C", 0.5),
        ]
    ]


def test_profiles_identify_strong_offense_and_defense():
    profiles, avg = fit_profiles(sample())
    assert 0 < avg < 1
    assert profiles["A"].offensive_percentile > profiles["B"].offensive_percentile
    assert profiles["A"].defensive_percentile > profiles["D"].defensive_percentile
    assert 0 <= profiles["A"].offensive_percentile <= 100


def test_projection_is_sum_of_adjustments():
    profiles, avg = fit_profiles(sample())
    p = project_matchup(profiles, avg, "A", "B")
    assert p.projected_value == pytest.approx(avg + profiles["A"].offense + profiles["B"].defense_allowed)


def test_reverse_metric_direction():
    rows = [Observation(r.team, r.opponent, -r.value) for r in sample()]
    profiles, _ = fit_profiles(rows, higher_is_better=False)
    assert profiles["A"].offensive_percentile > profiles["B"].offensive_percentile


def test_missing_coverage_not_fabricated():
    profiles, _ = fit_profiles(sample()[:2], min_games=3)
    assert profiles == {}


def test_reject_bad_observations():
    with pytest.raises(ValueError):
        fit_profiles([Observation("A", "B", math.nan)])
    with pytest.raises(ValueError):
        fit_profiles([Observation("A", "B", 2.0, weight=0)])
    with pytest.raises(ValueError):
        fit_profiles([Observation("A", "A", 2.0)])


def test_projection_requires_two_eligible_teams():
    profiles, avg = fit_profiles(sample())
    with pytest.raises(ValueError):
        project_matchup(profiles, avg, "A", "UNKNOWN")
