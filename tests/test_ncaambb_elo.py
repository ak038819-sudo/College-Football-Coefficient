"""Basketball Elo: the engine, and the honesty of the fit around it.

The plan's rule is that no football parameter may be inherited without
basketball validation. So the tests that matter most here are not "does Elo go
up when you win" but the ones that would let a bad number look good: a holdout
that the fit can see, a k carried across a change of margin layer, a
multiplier whose average is not 1 so k quietly means something else.
"""

from pathlib import Path
import json
import math
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb.elo import (INITIAL_RATING, accuracy, brier, calibration,
                             expected_home, fit_points_per_elo, log_loss,
                             margin_multiplier, mean_winning_margin, regress,
                             replay, score)
from src.ncaambb import fit_elo


def game(key='g1', season=2026, home='A', away='B', home_points=70,
         away_points=60, neutral=0, day='2025-11-07', rateable=1):
    return dict(game_key=key, season=season, start_date_utc=day,
                neutral_site=neutral, home_team_key=home, away_team_key=away,
                home_points=home_points, away_points=away_points,
                status='final', rateable=rateable)


BASE = dict(k=20, home_advantage=60, offseason_retention=0.8,
            margin_model='result_only')


# --- the expectation --------------------------------------------------------

def test_even_teams_on_neutral_ground_are_a_coin_flip():
    assert expected_home(1500, 1500, neutral=True, home_advantage=60) == 0.5


def test_the_home_edge_applies_at_home_and_not_on_neutral_ground():
    at_home = expected_home(1500, 1500, home_advantage=60)
    neutral = expected_home(1500, 1500, neutral=True, home_advantage=60)
    assert at_home > 0.5 and neutral == 0.5


def test_the_expectation_is_symmetric():
    assert expected_home(1600, 1400, home_advantage=0) == pytest.approx(
        1 - expected_home(1400, 1600, home_advantage=0))


# --- the margin multiplier --------------------------------------------------

def test_result_only_ignores_the_margin_entirely():
    assert margin_multiplier('result_only', margin=1, elo_diff=0,
                             reference_margin=11.0) == 1.0
    assert margin_multiplier('result_only', margin=40, elo_diff=0,
                             reference_margin=11.0) == 1.0


def test_an_average_win_scores_one_in_every_margin_model():
    """k only ever acts through k * M, so a layer whose average M is 2.4 makes
    k mean something different from one whose average is 1. Football's config
    records that costing a real comparison; here every model is normalised so
    the models can be compared instead of the k values."""
    reference = 11.0
    assert margin_multiplier('mov', margin=reference, elo_diff=0,
                             reference_margin=reference) == 1.0
    # mov_adjusted: a team that wins by the reference margin against an evenly
    # matched opponent has beaten expectation by exactly the reference margin.
    assert margin_multiplier('mov_adjusted', margin=reference, elo_diff=0,
                             reference_margin=reference,
                             points_per_elo=0.05) == 1.0


def test_a_bigger_win_moves_a_rating_further():
    big = margin_multiplier('mov', margin=25, elo_diff=0, reference_margin=11.0)
    small = margin_multiplier('mov', margin=3, elo_diff=0, reference_margin=11.0)
    assert big > 1.0 > small


def test_an_upset_counts_for_more_than_the_same_margin_by_a_favourite():
    """This is the whole point of adjusting for strength: winning by 10 as a
    400-point underdog is not the same result as winning by 10 as the
    favourite."""
    underdog = margin_multiplier('mov_adjusted', margin=10, elo_diff=-400,
                                 reference_margin=11.0, points_per_elo=0.05)
    favourite = margin_multiplier('mov_adjusted', margin=10, elo_diff=400,
                                  reference_margin=11.0, points_per_elo=0.05)
    assert underdog > favourite


def test_the_multiplier_is_clamped_at_both_ends():
    assert margin_multiplier('mov', margin=400, elo_diff=0,
                             reference_margin=11.0, m_max=4.0) == 4.0
    assert margin_multiplier('mov_adjusted', margin=1, elo_diff=800,
                             reference_margin=11.0, points_per_elo=0.05,
                             m_min=0.25) == 0.25


def test_a_reference_margin_nobody_measured_is_refused():
    """The normalisation is the whole claim. A default of 1.0 slipping through
    would scale every update by the mean margin, about eleven times."""
    with pytest.raises(ValueError, match='measured, not assumed'):
        margin_multiplier('mov', margin=10, elo_diff=0, reference_margin=0)


def test_a_tie_is_not_a_win():
    with pytest.raises(ValueError, match='A winning margin is positive'):
        margin_multiplier('mov', margin=0, elo_diff=0, reference_margin=11.0)


def test_an_unknown_margin_model_is_refused_rather_than_ignored():
    with pytest.raises(ValueError, match='Unknown margin model'):
        margin_multiplier('xsrdiff', margin=10, elo_diff=0,
                          reference_margin=11.0)


# --- measured quantities ----------------------------------------------------

def test_the_mean_winning_margin_ignores_ties_and_unplayed_games():
    games = [game(home_points=70, away_points=60),      # 10
             game(home_points=80, away_points=60),      # 20
             game(home_points=70, away_points=70),      # level
             game(home_points=None, away_points=None)]  # unplayed
    assert mean_winning_margin(games) == 15.0


def test_a_season_with_no_decided_game_cannot_yield_a_margin():
    with pytest.raises(ValueError, match='No decided game'):
        mean_winning_margin([game(home_points=None, away_points=None)])


def test_points_per_elo_recovers_a_slope_it_was_given():
    slope = 0.04
    rows = [dict(home_pregame=1500 + gap, away_pregame=1500, neutral_site=1,
                 home_points=int(round(slope * gap)) + 60, away_points=60)
            for gap in range(-400, 401, 50)]
    assert fit_points_per_elo(rows) == pytest.approx(slope, abs=0.004)


def test_points_per_elo_needs_a_gap_to_fit_against():
    rows = [dict(home_pregame=1500, away_pregame=1500, neutral_site=1,
                 home_points=70, away_points=60)]
    with pytest.raises(ValueError, match='pregame gap is zero'):
        fit_points_per_elo(rows)


# --- the offseason ----------------------------------------------------------

def test_full_retention_keeps_a_rating_and_no_retention_resets_it():
    assert regress(1800, 1.0) == 1800
    assert regress(1800, 0.0) == INITIAL_RATING


def test_retention_outside_zero_to_one_is_refused():
    with pytest.raises(ValueError, match='fraction between 0 and 1'):
        regress(1800, 1.5)


# --- the replay -------------------------------------------------------------

def test_a_win_is_zero_sum():
    rows, ratings = replay([game()], BASE)
    assert ratings['A'] - INITIAL_RATING == pytest.approx(
        INITIAL_RATING - ratings['B'])
    assert rows[0]['rating_change'] > 0


def test_an_upset_moves_ratings_further_than_an_expected_win():
    strong = replay([game(home='A', away='B')], BASE)[0][0]['rating_change']
    # Give the away team a head start, then have the home team win anyway.
    history = [game('g0', home='B', away='C', home_points=99, away_points=50),
               game('g1', home='A', away='B')]
    upset = replay(history, BASE)[0][1]['rating_change']
    assert upset > strong


def test_the_offseason_regresses_once_at_the_season_boundary():
    history = [game('g1', season=2025, home_points=99, away_points=50,
                    day='2024-11-07'),
               game('g2', season=2026, day='2025-11-07')]
    rows, _ = replay(history, dict(BASE, offseason_retention=0.5))
    gained = rows[0]['home_postgame'] - INITIAL_RATING
    carried = rows[1]['home_pregame'] - INITIAL_RATING
    assert carried == pytest.approx(gained * 0.5)


def test_two_games_in_one_season_are_not_regressed_between_them():
    history = [game('g1', home_points=99, away_points=50),
               game('g2', day='2025-11-09')]
    rows, _ = replay(history, dict(BASE, offseason_retention=0.5))
    assert rows[1]['home_pregame'] == pytest.approx(rows[0]['home_postgame'])


def test_an_unplayed_game_changes_nothing_and_says_so(): 
    """A scheduled game scored as a half-win would invent a result. It is kept
    in the record so a caller can see the model declined."""
    rows, ratings = replay([game(home_points=None, away_points=None)], BASE)
    assert rows[0]['counted'] == 0
    assert rows[0]['rating_change'] == 0.0
    assert ratings == {}


def test_a_level_final_changes_nothing():
    rows, _ = replay([game(home_points=70, away_points=70)], BASE)
    assert rows[0]['counted'] == 0


def test_the_record_carries_the_pregame_state_not_the_postgame_one():
    """Anything scoring this model has to see what it knew BEFORE the game."""
    rows, _ = replay([game()], BASE)
    assert rows[0]['home_pregame'] == INITIAL_RATING
    assert rows[0]['home_postgame'] > INITIAL_RATING
    assert 0 < rows[0]['home_expected'] < 1


def test_a_neutral_game_gives_the_nominal_home_team_no_edge():
    home = replay([game()], BASE)[0][0]['home_expected']
    neutral = replay([game(neutral=1)], BASE)[0][0]['home_expected']
    assert home > 0.5 and neutral == 0.5


def test_the_average_multiplier_over_a_season_is_about_one():
    """The normalisation claim, measured on a spread of margins rather than
    asserted on one. If this drifts, k stops meaning what the fit chose."""
    margins = [3, 5, 8, 10, 11, 12, 15, 20, 25, 30]
    games = [game(f'g{n}', home_points=60 + m, away_points=60,
                  day=f'2025-11-{n + 1:02d}')
             for n, m in enumerate(margins)]
    reference = mean_winning_margin(games)
    rows, _ = replay(games, dict(BASE, margin_model='mov',
                                 reference_margin=reference,
                                 margin_exponent=1.0))
    multipliers = [row['multiplier'] for row in rows if row['counted']]
    assert sum(multipliers) / len(multipliers) == pytest.approx(1.0, abs=0.05)


# --- the metrics ------------------------------------------------------------

def scored(probability, home_won):
    return dict(counted=1, home_expected=probability,
                home_points=70 if home_won else 60, away_points=60)


def test_brier_is_the_mean_squared_error():
    assert brier([scored(1.0, True), scored(0.0, False)]) == 0.0
    assert brier([scored(0.0, True)]) == 1.0
    assert brier([scored(0.5, True), scored(0.5, False)]) == 0.25


def test_log_loss_is_finite_even_when_the_model_was_certain_and_wrong():
    value = log_loss([scored(1.0, False)])
    assert math.isfinite(value) and value > 20


def test_accuracy_splits_a_game_the_model_called_a_coin_flip():
    assert accuracy([scored(0.5, True)]) == 0.5
    assert accuracy([scored(0.9, True), scored(0.9, False)]) == 0.5


def test_a_perfectly_calibrated_set_has_no_calibration_error():
    rows = [scored(0.75, True)] * 3 + [scored(0.75, False)]
    error, table = calibration(rows, buckets=10)
    assert error == pytest.approx(0.0)
    assert [row['games'] for row in table] == [4]


def test_calibration_catches_overconfidence_brier_alone_can_hide():
    rows = [scored(0.95, True)] * 7 + [scored(0.95, False)] * 3
    error, _ = calibration(rows)
    assert error == pytest.approx(0.25, abs=0.01)


def test_errors_in_opposite_directions_do_not_cancel():
    """A model over-confident at one end and under-confident at the other is
    badly calibrated, not perfectly calibrated. Summing the signed gaps would
    report nearly zero, which is the worst kind of wrong number."""
    rows = ([scored(0.95, True)] * 7 + [scored(0.95, False)] * 3        # -0.25
            + [scored(0.15, True)] * 4 + [scored(0.15, False)] * 6)     # +0.25
    error, table = calibration(rows)
    assert len(table) == 2
    assert error == pytest.approx(0.25, abs=0.01)


def test_scoring_an_empty_set_is_refused_rather_than_returning_zero():
    for metric in (brier, log_loss, accuracy, calibration, score):
        with pytest.raises(ValueError, match='No counted game'):
            metric([dict(counted=0, home_expected=0.5, home_points=70,
                         away_points=60)])


# --- the fit, and whether it can see its own holdout -----------------------

def season_games(season, teams=('A', 'B', 'C', 'D'), home_bias=0):
    """A small round robin, dated so the order is unambiguous."""
    games, day = [], 1
    for index, home in enumerate(teams):
        for away in teams[index + 1:]:
            games.append(game(f'{season}-{home}{away}', season=season,
                              home=home, away=away,
                              # +1 so no fixture game is level: a level game
                              # is not counted, and a fixture that quietly
                              # halves its own sample is a bad fixture.
                              home_points=61 + home_bias + index,
                              away_points=60,
                              day=f'{season - 1}-11-{day:02d}'))
            day += 1
    return games


def loaded(connection, games):
    from src.ncaambb.store import load_season
    seasons = sorted({g['season'] for g in games})
    teams = sorted({g['home_team_key'] for g in games}
                   | {g['away_team_key'] for g in games})
    # Write straight to the game table: these fixtures are about the fit, not
    # about the loader, which has its own tests.
    for season in seasons:
        for name in teams:
            connection.execute(
                'INSERT OR IGNORE INTO team (team_key, provider_id, school)'
                ' VALUES (?, ?, ?)', (name, abs(hash(name)) % 10_000, name))
            connection.execute(
                'INSERT OR IGNORE INTO team_season (team_key, season,'
                ' season_label, division_one) VALUES (?, ?, ?, 1)',
                (name, season, str(season)))
    for g in games:
        connection.execute(
            'INSERT INTO game (game_key, provider_id, season, season_label,'
            ' season_type, status, start_date, start_date_utc, start_time_tbd,'
            ' neutral_site, home_team_key, away_team_key, home_points,'
            ' away_points, rateable, division_one, exclusions)'
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 1, '[]')",
            (g['game_key'], abs(hash(g['game_key'])) % 1_000_000, g['season'],
             str(g['season']), 'regular', g['status'], g['start_date_utc'],
             g['start_date_utc'], g['neutral_site'], g['home_team_key'],
             g['away_team_key'], g['home_points'], g['away_points'],
             g['rateable']))
    connection.commit()
    return connection


@pytest.fixture
def db(tmp_path):
    from src.ncaambb.store import connect
    connection = connect(tmp_path / 'ncaambb.db')
    yield connection
    connection.close()


def test_games_come_back_in_date_order_not_in_key_order(db):
    """The keys are deliberately named so that alphabetical order is the WRONG
    order. An earlier version of this test used 'early' and 'late', which sort
    correctly by accident and passed even with the date dropped from the
    ORDER BY entirely."""
    loaded(db, [game('a-march', day='2026-03-01'),
                game('z-november', day='2025-11-01')])
    assert [row['game_key'] for row in fit_elo.load_games(db)] == [
        'z-november', 'a-march']


def test_an_unrateable_game_is_not_fitted_against(db):
    loaded(db, [game('rated'), game('unrated', rateable=0, day='2025-11-08')])
    assert [row['game_key'] for row in fit_elo.load_games(db)] == ['rated']
    assert len(fit_elo.load_games(db, rateable_only=False)) == 2


def test_the_measured_quantities_cannot_see_the_holdout(db):
    """THE leakage test. If the mean margin or the margin-per-rating-point
    slope is measured on the scored season, the holdout is not held out and
    every number downstream is optimistic for a reason nobody can see."""
    fit = season_games(2025) + season_games(2026)
    loaded(db, fit + season_games(2027, home_bias=60))
    games = fit_elo.load_games(db)
    with_holdout = fit_elo.measure(games, {2025, 2026})

    # Now make the holdout season wildly different. Nothing measured may move.
    db.execute('UPDATE game SET home_points = 200 WHERE season = 2027')
    db.commit()
    after = fit_elo.measure(fit_elo.load_games(db), {2025, 2026})
    assert after['reference_margin'] == pytest.approx(
        with_holdout['reference_margin'])
    assert after['points_per_elo'] == pytest.approx(
        with_holdout['points_per_elo'])
    assert after['fit_games'] == with_holdout['fit_games']


def test_only_the_seasons_asked_for_are_scored(db):
    loaded(db, season_games(2025) + season_games(2026))
    games = fit_elo.load_games(db)
    measured = fit_elo.measure(games, {2025})
    config = fit_elo.configuration(20, 60, 0.8, 'result_only', 1.0, measured)
    one, _, _ = fit_elo.evaluate(games, config, {2026})
    both, _, _ = fit_elo.evaluate(games, config, {2025, 2026})
    assert one['games'] == len(season_games(2026))
    assert both['games'] == one['games'] * 2


def test_the_margin_layer_refits_k_rather_than_inheriting_stage_ones(db):
    """k only acts through k * M, so carrying stage one's k into stage two
    would make the comparison a test of k rather than of the layer. The
    search must therefore try the whole k grid again with each layer."""
    loaded(db, season_games(2025) + season_games(2026))
    games = fit_elo.load_games(db)
    measured = fit_elo.measure(games, {2025, 2026})
    results = fit_elo.search(games, [2025, 2026], measured,
                             k_grid=(10, 20, 30), home_grid=(0, 50),
                             retention_grid=(0.8,), exponent_grid=(1.0,))
    for model in ('mov', 'mov_adjusted'):
        ks = {row['k'] for row in results if row['margin_model'] == model}
        assert ks == {10, 20, 30}, model


def test_every_margin_model_reaches_the_search(db):
    loaded(db, season_games(2025) + season_games(2026))
    games = fit_elo.load_games(db)
    measured = fit_elo.measure(games, {2025, 2026})
    results = fit_elo.search(games, [2025, 2026], measured,
                             k_grid=(20,), home_grid=(50,),
                             retention_grid=(0.8,), exponent_grid=(1.0,))
    assert {row['margin_model'] for row in results} == set(
        fit_elo.MARGIN_MODELS)


def test_the_search_is_ordered_best_first(db):
    loaded(db, season_games(2025) + season_games(2026))
    games = fit_elo.load_games(db)
    measured = fit_elo.measure(games, {2025, 2026})
    results = fit_elo.search(games, [2025, 2026], measured,
                             k_grid=(10, 20), home_grid=(0, 50),
                             retention_grid=(0.8,), exponent_grid=(1.0,))
    briers = [row['brier'] for row in results]
    assert briers == sorted(briers)


def test_footballs_parameters_are_among_the_baselines(db):
    """The plan forbids inheriting them without validation, so the report has
    to say what inheriting them would have scored."""
    loaded(db, season_games(2025) + season_games(2026))
    games = fit_elo.load_games(db)
    measured = fit_elo.measure(games, {2025})
    rows, rate = fit_elo.baselines(games, [2026], measured)
    names = [row['name'] for row in rows]
    assert any('football' in name for name in names)
    assert any('coin flip' in name for name in names)
    assert 0 <= rate <= 1


def test_a_single_season_cannot_be_held_out_from_itself(db):
    loaded(db, season_games(2026))
    with pytest.raises(ValueError, match='at least two seasons'):
        fit_elo.run(db)


def test_an_empty_database_says_to_ingest_rather_than_fitting_nothing(db):
    with pytest.raises(ValueError, match='run the ingestion first'):
        fit_elo.run(db)


def test_a_run_holds_out_the_latest_season_by_default(db):
    loaded(db, season_games(2025) + season_games(2026))
    report = fit_elo.run(db)
    assert report['holdout_seasons'] == [2026]
    assert report['fit_seasons'] == [2025]
    assert '# Basketball Elo fit' in '\n'.join(fit_elo.render(report))


# --- storing what the model produced ---------------------------------------

ELO = dict(initial_rating=1500.0, scale=400.0, k=20,
           home_advantage=60, offseason_retention=0.8,
           margin_model='mov', margin_exponent=1.0,
           margin_min=0.25, margin_max=4.0,
           reference_margin=11.0, points_per_elo=0.04)


def validation(elo=None, **overrides):
    """A held-out claim pinned to the parameters it was measured on.

    The hash is computed from the parameters rather than written down, so a
    test that changes a parameter cannot accidentally be asserting against a
    stale claim -- which is the very thing the rule exists to catch.
    """
    from src.ncaambb.build_ratings import elo_digest
    block = dict(fit_seasons=[2024, 2025], holdout_seasons=[2026],
                 holdout=dict(brier=0.19, games=5752),
                 measured_on_elo_sha256=elo_digest(elo or ELO))
    block.update(overrides)
    return block


FITTED = dict(elo=ELO, validation=validation())


def write_config(tmp_path, payload):
    path = tmp_path / 'ncaambb_model.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    return path


def test_a_config_missing_a_fitted_parameter_is_refused(tmp_path):
    """A silent default is how a parameter nobody fitted reaches a published
    rating."""
    from src.ncaambb.build_ratings import load_config
    broken = dict(elo={key: value for key, value in FITTED['elo'].items()
                       if key != 'k'})
    with pytest.raises(ValueError, match='missing fitted parameters'):
        load_config(write_config(tmp_path, broken))


def test_a_config_with_no_elo_section_is_refused(tmp_path):
    from src.ncaambb.build_ratings import load_config
    with pytest.raises(ValueError, match='no "elo" section'):
        load_config(write_config(tmp_path, dict(hybrid={})))


def test_a_placeholder_reference_margin_is_refused(tmp_path):
    """The normalisation is only meaningful if the margin was measured. A 1.0
    left in here would scale every update by about eleven."""
    from src.ncaambb.build_ratings import load_config
    payload = dict(elo=dict(FITTED['elo'], reference_margin=0))
    with pytest.raises(ValueError, match='measured mean winning margin'):
        load_config(write_config(tmp_path, payload))


def test_result_only_does_not_need_a_measured_margin(tmp_path):
    from src.ncaambb.build_ratings import load_config
    payload = dict(elo=dict(FITTED['elo'], margin_model='result_only',
                            reference_margin=0))
    assert load_config(write_config(tmp_path, payload))['elo']['k'] == 20


def test_the_config_path_is_resolved_inside_the_call(tmp_path, monkeypatch):
    """A def-time default would bind config/ncaambb_model.json and ignore the
    argument. This trap has bitten this repository four times."""
    from src.ncaambb import build_ratings
    monkeypatch.setattr(build_ratings, 'CONFIG_PATH',
                        write_config(tmp_path, FITTED))
    assert build_ratings.load_config()['elo']['k'] == 20


def test_the_committed_config_is_a_complete_fitted_model():
    """The file every replay reads. A config that fails its own loader would
    only be noticed when a ratings run died, long after the commit."""
    from src.ncaambb.build_ratings import CONFIG_PATH, load_config
    elo = load_config(CONFIG_PATH)['elo']
    # Measured on basketball, so neither is a round number anyone typed.
    assert 8 < elo['reference_margin'] < 16
    assert 0.02 < elo['points_per_elo'] < 0.2
    from src.ncaambb.elo import MARGIN_MODELS
    assert elo['margin_model'] in MARGIN_MODELS


def test_the_committed_config_did_not_inherit_football_whole():
    """The plan forbids inheriting football's Elo settings without basketball
    validation. The fit landed on football's k and retention, so the proof that
    nothing was copied is that something else differs."""
    from src.ncaambb.build_ratings import CONFIG_PATH, load_config
    from src.ncaambb.fit_elo import FOOTBALL
    elo = load_config(CONFIG_PATH)['elo']
    assert any(elo[key] != value for key, value in FOOTBALL.items())


def test_building_stores_the_model_its_ratings_and_its_games(db):
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2025) + season_games(2026))
    summary = build(db, FITTED, report=dict(
        fit_seasons=[2025], holdout_seasons=[2026],
        holdout=dict(brier=0.19, games=6)))
    assert summary['seasons'] == [2025, 2026]
    assert summary['games'] == 12 and summary['counted'] == 12
    model = db.execute('SELECT * FROM rating_model').fetchone()
    assert model['name'] == 'ncaambb_elo_v1'
    assert json.loads(model['config'])['k'] == 20
    assert model['holdout_brier'] == 0.19
    assert json.loads(model['held_out']) == [2026]
    assert db.execute('SELECT COUNT(*) AS n FROM rating_game').fetchone()['n'] == 12


def test_a_rating_is_stored_per_season_not_only_at_the_end(db):
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2025) + season_games(2026))
    build(db, FITTED)
    rows = db.execute('SELECT team_key, season FROM team_rating'
                      ' ORDER BY season, team_key').fetchall()
    seasons = sorted({row['season'] for row in rows})
    assert seasons == [2025, 2026]
    assert len(rows) == 8, 'four teams in each of two seasons'


def test_rebuilding_changes_no_row_count(db):
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2025) + season_games(2026))
    first = build(db, FITTED)
    second = build(db, FITTED)
    assert first['model_id'] == second['model_id']
    for table in ('rating_model', 'team_rating', 'rating_game'):
        count = db.execute(f'SELECT COUNT(*) AS n FROM {table}').fetchone()['n']
        assert count == {'rating_model': 1, 'team_rating': 8,
                         'rating_game': 12}[table], table


def test_the_stored_chain_is_a_chain(db):
    """Each counted game's postgame rating is that team's pregame rating in its
    next counted game. That is what makes a stored rating auditable instead of
    a number to take on trust."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    build(db, FITTED)
    rows = db.execute(
        'SELECT g.start_date_utc, g.home_team_key, g.away_team_key,'
        '       r.home_pregame, r.away_pregame, r.home_postgame,'
        '       r.away_postgame, r.game_key'
        '  FROM rating_game r JOIN game g USING (game_key)'
        ' WHERE r.counted = 1'
        ' ORDER BY g.start_date_utc, r.game_key').fetchall()
    assert len(rows) == 6, 'the fixture season has six decided games'
    last, links = {}, 0
    for row in rows:
        for side in ('home', 'away'):
            team = row[f'{side}_team_key']
            if team in last:
                assert row[f'{side}_pregame'] == pytest.approx(last[team])
                links += 1
            last[team] = row[f'{side}_postgame']
    # An earlier version of this test read team keys off rating_game, which
    # does not carry them, and checked nothing at all while passing.
    assert links >= 6, f'only {links} links checked'


def test_building_with_nothing_ingested_says_so(db):
    from src.ncaambb.build_ratings import build
    with pytest.raises(ValueError, match='run the ingestion first'):
        build(db, FITTED)


def test_an_uncounted_game_adds_no_rating_and_no_game_to_the_count(db):
    """A scheduled or level game is in the chain with counted = 0. Letting it
    into the end-of-season rating would set that rating from a game nobody
    played, and letting it into games_counted would overstate the sample."""
    from src.ncaambb.build_ratings import build
    games = season_games(2026) + [
        game('unplayed', season=2026, home='A', away='B', home_points=None,
             away_points=None, day='2026-03-20'),
        game('level', season=2026, home='A', away='C', home_points=70,
             away_points=70, day='2026-03-21')]
    loaded(db, games)
    summary = build(db, FITTED)
    assert summary['counted'] == 6 and summary['declined'] == 2
    rows = db.execute('SELECT team_key, games_counted FROM team_rating'
                      ' WHERE season = 2026 ORDER BY team_key').fetchall()
    assert [row['games_counted'] for row in rows] == [3, 3, 3, 3]


def test_the_season_rating_is_the_postgame_of_the_last_counted_game(db):
    """Storing the PREGAME rating of that game would report a team's standing
    as it was before its final result."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    build(db, FITTED)
    last = db.execute(
        'SELECT r.home_team_key, r.home_postgame, r.home_pregame FROM ('
        '  SELECT g.home_team_key, rg.home_postgame, rg.home_pregame,'
        '         g.start_date_utc FROM rating_game rg JOIN game g'
        '    USING (game_key) WHERE rg.counted = 1'
        ') AS r ORDER BY r.start_date_utc DESC LIMIT 1').fetchone()
    stored = db.execute('SELECT rating FROM team_rating WHERE team_key = ?'
                        ' AND season = 2026',
                        (last['home_team_key'],)).fetchone()['rating']
    assert stored == pytest.approx(last['home_postgame'])
    assert stored != pytest.approx(last['home_pregame'])


def test_refitting_the_model_overwrites_what_was_stored(db):
    """A model name is a slot, not an archive. If a changed configuration were
    kept out, the stored ratings and the stored config would disagree and the
    config would be the one that looked authoritative."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    build(db, FITTED)
    before = db.execute('SELECT rating FROM team_rating WHERE team_key = ?'
                        ' AND season = 2026', ('A',)).fetchone()['rating']
    changed = dict(FITTED['elo'], k=45)
    build(db, dict(elo=changed, validation=validation(changed)))
    row = db.execute('SELECT config FROM rating_model').fetchone()
    assert json.loads(row['config'])['k'] == 45
    after = db.execute('SELECT rating FROM team_rating WHERE team_key = ?'
                       ' AND season = 2026', ('A',)).fetchone()['rating']
    assert after != pytest.approx(before), 'a bigger k must move the rating'


def test_a_rebuild_after_more_games_updates_the_game_count(db):
    """A refresh mid-season rebuilds over rows that already exist. If the
    count were not updated, a team's sample size would freeze at whatever the
    first run of the season saw."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    build(db, FITTED)
    before = db.execute('SELECT games_counted FROM team_rating'
                        ' WHERE team_key = ? AND season = 2026',
                        ('A',)).fetchone()['games_counted']
    loaded(db, [game('later', season=2026, home='A', away='B',
                     home_points=80, away_points=60, day='2026-02-01')])
    build(db, FITTED)
    after = db.execute('SELECT games_counted FROM team_rating'
                       ' WHERE team_key = ? AND season = 2026',
                       ('A',)).fetchone()['games_counted']
    assert after == before + 1


# --- the replay carries the validation with it -----------------------------
#
# A scheduled refresh does not fit anything: it replays the committed
# parameters on a fresh runner, with no fit report anywhere on disk. Until
# this, that stored the model with no held-out score at all, and the export
# gate -- correctly -- refused every such export. The refresh could never have
# published. So the held-out claim is committed beside the parameters, and
# pinned to them, because a claim that outlives what it measured is worse than
# no claim.

def test_a_replay_with_no_report_takes_the_held_out_score_from_the_config(db):
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    build(db, FITTED)
    row = db.execute('SELECT fitted_on, held_out, holdout_brier, holdout_games'
                     ' FROM rating_model ORDER BY model_id DESC'
                     ' LIMIT 1').fetchone()
    assert json.loads(row['held_out']) == [2026]
    assert json.loads(row['fitted_on']) == [2024, 2025]
    assert row['holdout_brier'] == 0.19
    assert row['holdout_games'] == 5752


def test_a_fit_report_still_wins_over_the_committed_claim(db):
    """The report is the measurement that just happened; the config's block is
    the record of the last one."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    build(db, FITTED, report=dict(fit_seasons=[2015], holdout_seasons=[2025],
                                  holdout=dict(brier=0.11, games=4000)))
    row = db.execute('SELECT held_out, holdout_brier FROM rating_model'
                     ' ORDER BY model_id DESC LIMIT 1').fetchone()
    assert json.loads(row['held_out']) == [2025]
    assert row['holdout_brier'] == 0.11


def test_a_replay_of_a_config_with_no_validation_is_refused(db):
    """This is the state the refresh was actually in: it would have replayed
    happily and produced an export the gate then refused, every run."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    with pytest.raises(ValueError, match='no "validation" section'):
        build(db, dict(elo=ELO))


def test_a_validation_measured_on_other_parameters_is_refused(db):
    """Editing k without re-fitting must fail here rather than publish the old
    Brier over the new model."""
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    stale = dict(elo=dict(ELO, k=45), validation=validation(ELO))
    with pytest.raises(ValueError, match='never earned'):
        build(db, stale)


@pytest.mark.parametrize('block, reason', [
    (dict(holdout_seasons=[]), 'no held-out season'),
    (dict(fit_seasons=[2024, 2026]), 'holdout leaked'),
    (dict(holdout=dict(brier=0.25, games=5752)), 'coin flip'),
    (dict(holdout=dict(brier=None, games=5752)), 'coin flip'),
    (dict(holdout=dict(brier=True, games=5752)), 'coin flip'),
    (dict(holdout=dict(brier=0.19, games=0)), 'held-out games'),
    (dict(holdout=dict(brier=0.19, games=None)), 'held-out games'),
])
def test_an_unusable_validation_block_is_refused(db, block, reason):
    from src.ncaambb.build_ratings import build
    loaded(db, season_games(2026))
    with pytest.raises(ValueError, match=reason):
        build(db, dict(elo=ELO, validation=validation(**block)))


def test_the_committed_configuration_passes_its_own_rule():
    """The config in the repository is the one every refresh replays. A rule
    nothing in the repository satisfies is a rule that gets deleted."""
    from src.ncaambb.build_ratings import (check_validation, load_config,
                                           report_from_config)
    config = load_config()
    checked = check_validation(config)
    assert checked['holdout_seasons']
    report = report_from_config(config)
    # And what it yields must satisfy the publish gate, which is the thing
    # that was refusing every refresh.
    from src.ncaambb import publish_gate
    manifest = dict(model=dict(
        name='ncaambb_elo_v1', config_sha256='a' * 64,
        fitted_on=report['fit_seasons'], held_out=report['holdout_seasons'],
        holdout_brier=report['holdout']['brier'],
        holdout_games=report['holdout']['games']))
    assert publish_gate.check_model(manifest) == []
