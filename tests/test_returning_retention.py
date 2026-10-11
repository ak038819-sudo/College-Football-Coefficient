"""The returning-production retention fit: the parts that decide the answer.

This calibrator reached a NEGATIVE result, and a negative result is only worth
anything if the machinery that produced it is right. Each test here pins one
of the choices that could have manufactured a false null or a false positive:
the per-season centring, the both-teams-early rule, the fallback for seasons
CFBD does not cover, and the slope-zero baseline sharing that fallback.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import calibrate_returning_retention as cal  # noqa: E402


def row(game_id, season, home, away, date='2024-09-01'):
    return {'game_id': game_id, 'season_year': season, 'game_date': date,
            'home_team_id': home, 'away_team_id': away,
            'home_score': 21, 'away_score': 17, 'neutral_site': 0, 'went_ot': 0,
            'home_name': f'T{home}', 'away_name': f'T{away}'}


# ---- the per-season centring -------------------------------------------------

def test_the_pivot_is_that_seasons_median():
    returning = {(1, 2014): 0.5, (2, 2014): 0.6, (3, 2014): 0.7, (1, 2025): 0.3}
    assert cal.season_pivots(returning) == {2014: 0.6, 2025: 0.3}


def test_centring_is_per_season_so_the_era_trend_is_not_read_as_an_effect():
    """The league median fell from 0.604 in 2014 to 0.362 in 2025. A team on
    the median gets the base rate in BOTH seasons, or `base` would absorb a
    decade trend and the slope would be credited for it."""
    returning = {(1, 2014): 0.604, (2, 2014): 0.4, (3, 2014): 0.9,
                 (1, 2025): 0.362, (2, 2025): 0.2, (3, 2025): 0.7}
    pivots = cal.season_pivots(returning)
    assert pivots == {2014: 0.604, 2025: 0.362}
    provider = cal.make_provider(returning, pivots, 0.8, 0.8, 0.5)
    assert provider(1, 2014) == pytest.approx(0.8)
    assert provider(1, 2025) == pytest.approx(0.8)


def test_a_team_above_its_season_median_gets_more_than_the_base():
    returning = {(1, 2024): 0.9, (2, 2024): 0.5}
    pivots = cal.season_pivots(returning)
    provider = cal.make_provider(returning, pivots, 0.8, 0.8, 0.5)
    assert provider(1, 2024) > 0.8
    assert provider(2, 2024) < 0.8


# ---- what happens where CFBD has nothing -------------------------------------

def test_a_season_cfbd_does_not_cover_gets_the_flat_rate():
    """34 of the 47 seasons predate 2014 and can have no team-specific rate.
    They must be untouched, or the comparison measures three decades of
    rewritten history instead of returning production."""
    provider = cal.make_provider({(1, 2024): 0.9}, {2024: 0.9}, 0.8, 0.95, 0.5)
    assert provider(1, 1995) == 0.8
    assert provider(1, 2024) == pytest.approx(0.95)


def test_a_team_with_no_row_in_a_covered_season_gets_the_flat_rate():
    provider = cal.make_provider({(1, 2024): 0.9}, {2024: 0.9}, 0.8, 0.95, 0.5)
    assert provider(99, 2024) == 0.8


def test_slope_zero_keeps_the_same_fallback_as_a_fitted_slope():
    """The baseline must share the machinery, not be a plain constant: that is
    what makes the reported gain attributable to the slope alone."""
    returning = {(1, 2024): 0.9}
    pivots = cal.season_pivots(returning)
    flat_provider = cal.make_provider(returning, pivots, 0.8, 0.85, 0.0)
    assert flat_provider(1, 1995) == 0.8, 'uncovered season still flat'
    assert flat_provider(1, 2024) == pytest.approx(0.85)
    assert flat_provider(99, 2024) == 0.8


# ---- clipping ----------------------------------------------------------------

def test_the_predictor_is_clipped_so_one_outlier_cannot_swing_the_fit():
    """CFBD's 2021 holds a -3.100. It is a real value, but ungated it would
    dominate a linear fit on its own."""
    returning = {(1, 2021): -3.1, (2, 2021): 0.6}
    pivots = cal.season_pivots(returning)
    provider = cal.make_provider(returning, pivots, 0.8, 0.8, 0.1)
    expected = 0.8 + 0.1 * (cal.PREDICTOR_FLOOR - pivots[2021])
    assert provider(1, 2021) == pytest.approx(expected)


def test_returning_production_above_one_is_kept_as_a_real_value():
    """A player back from a redshirt adds production that was not in last
    season's total, so above 100% is meaningful rather than corrupt."""
    assert cal.PREDICTOR_CEILING > 1.0
    returning = {(1, 2026): 1.27, (2, 2026): 0.4}
    pivots = cal.season_pivots(returning)
    provider = cal.make_provider(returning, pivots, 0.8, 0.8, 0.1)
    assert provider(1, 2026) == pytest.approx(0.8 + 0.1 * (1.27 - pivots[2026]))


def test_the_rate_itself_stays_a_fraction():
    """A retention above 1 would grow a rating across an offseason; below 0
    would flip it past the mean."""
    returning = {(1, 2024): 5.0, (2, 2024): -5.0, (3, 2024): 0.5}
    pivots = cal.season_pivots(returning)
    steep = cal.make_provider(returning, pivots, 0.8, 0.9, 10.0)
    assert steep(1, 2024) == 1.0
    assert steep(2, 2024) == 0.0


# ---- which games are scored --------------------------------------------------

def test_a_game_counts_only_when_both_teams_are_early():
    """A week-two team against a week-ten team is half a test of the offseason
    and half a test of ten weeks of results."""
    games = [row(1, 2024, 1, 2), row(2, 2024, 1, 3), row(3, 2024, 1, 4)]
    assert cal.early_game_ids(games, 2) == {1, 2}


def test_the_count_restarts_each_season():
    games = [row(1, 2024, 1, 2), row(2, 2024, 1, 2), row(3, 2025, 1, 2)]
    assert 3 in cal.early_game_ids(games, 1)


def test_a_limit_of_zero_scores_nothing():
    assert cal.early_game_ids([row(1, 2024, 1, 2)], 0) == set()


# ---- name resolution ---------------------------------------------------------

def _db(tmp_path, teams=('Georgia',)):
    path = tmp_path / 'league.db'
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT)')
    conn.execute('CREATE TABLE team_aliases (alias TEXT, team_name TEXT)')
    for index, name in enumerate(teams, start=1):
        conn.execute('INSERT INTO teams VALUES (?, ?)', (index, name))
    conn.commit()
    return conn


def _csv(tmp_path, rows, season=2024):
    path = tmp_path / f'returning_production_{season}.csv'
    header = 'season_year,team,conference,percent_ppa\n'
    path.write_text(header + ''.join(rows), encoding='utf-8')
    return str(tmp_path / 'returning_production_*.csv')


def test_an_unresolvable_school_fails_rather_than_being_dropped(tmp_path):
    """A silent skip would make the fit look weaker than it is -- the dropped
    team falls back to the flat rate and quietly dilutes the effect. This is
    the repository's recurring silent-drop shape wearing a different hat."""
    conn = _db(tmp_path)
    pattern = _csv(tmp_path, ['2024,Georgia,SEC,0.6\n', '2024,Not A School,SEC,0.5\n'])
    with pytest.raises(ValueError, match='Not A School'):
        cal.load_returning(conn, pattern)


def test_a_resolvable_school_is_loaded_against_its_team_id(tmp_path):
    conn = _db(tmp_path)
    pattern = _csv(tmp_path, ['2024,Georgia,SEC,0.6\n'])
    assert cal.load_returning(conn, pattern) == {(1, 2024): 0.6}


def test_a_blank_metric_is_skipped_without_failing(tmp_path):
    """Blank means CFBD did not report it, which is not an identity problem."""
    conn = _db(tmp_path)
    pattern = _csv(tmp_path, ['2024,Georgia,SEC,\n'])
    assert cal.load_returning(conn, pattern) == {}


# ---- the engine hook ---------------------------------------------------------

def test_omitting_the_provider_leaves_the_engine_bit_for_bit():
    """Production must not change because this experiment exists."""
    from build_elo import run_elo
    cfg = {'initial_rating': 1500, 'scale': 400, 'k': 45, 'home_field': 50,
           'offseason_retention': 0.8, 'mov_c': 2.2, 'mov_d': 0.001}
    games = [row(1, 2023, 1, 2), row(2, 2024, 1, 2)]
    without = run_elo(games, cfg)[1]
    flat_provider = run_elo(games, cfg, retention_for_team=lambda t, s: 0.8)[1]
    assert without == flat_provider


def test_the_provider_actually_changes_the_offseason():
    """If a provider could not move a rating, every score here would be equal
    and the whole calibration would be a vacuous null."""
    from build_elo import run_elo
    cfg = {'initial_rating': 1500, 'scale': 400, 'k': 45, 'home_field': 50,
           'offseason_retention': 0.8, 'mov_c': 2.2, 'mov_d': 0.001}
    games = [row(1, 2023, 1, 2), row(2, 2024, 1, 2)]
    keep_all = run_elo(games, cfg, retention_for_team=lambda t, s: 1.0)[1]
    keep_none = run_elo(games, cfg, retention_for_team=lambda t, s: 0.0)[1]
    assert keep_all != keep_none


def test_the_provider_is_asked_per_team_and_per_season():
    from build_elo import run_elo
    cfg = {'initial_rating': 1500, 'scale': 400, 'k': 45, 'home_field': 50,
           'offseason_retention': 0.8, 'mov_c': 2.2, 'mov_d': 0.001}
    asked = []
    games = [row(1, 2023, 1, 2), row(2, 2024, 1, 2), row(3, 2025, 1, 2)]
    run_elo(games, cfg, retention_for_team=lambda t, s: asked.append((t, s)) or 0.8)
    assert sorted(asked) == [(1, 2024), (1, 2025), (2, 2024), (2, 2025)]


# ---- the calibrator never moves the config ----------------------------------

def test_the_calibrator_does_not_write_the_config():
    """Every calibrator here prints a recommendation and leaves the decision to
    a person. A script that edited model_config.json would move published
    ratings without anyone choosing to."""
    source = (Path(__file__).resolve().parents[1] / 'src'
              / 'calibrate_returning_retention.py').read_text(encoding='utf-8')
    assert 'model_config.json' in source, 'it reads the config'
    for writer in ('json.dump', "open(args.config, 'w')", 'write_text'):
        assert writer not in source, f'calibrator must not {writer}'
