"""
EXP-03 (xSRDiff / SR+ performance layer): the invariants from the guide's §8,
plus the ones that protect the existing Elo engine from the new layer.

These run entirely on synthetic fixtures -- no database and no per-game Success
Rate needed -- so they pin the arithmetic and the fallback behavior whether or
not CFBD data has been fetched yet.
"""
import pytest

from build_elo import GameContext, run_elo
from srdiff import (MOV, RAW_SRDIFF, RESULT_ONLY, TIE, XSRDIFF, MovLayer, ResultOnlyLayer,
                    XsrModel, actual_sr_diff, build_layer, expected_sr_diff,
                    load_performance_config, performance_multiplier, sr_plus)

ELO_CFG = {"initial_rating": 1500.0, "scale": 400.0, "k": 20.0, "home_field": 50.0,
           "offseason_retention": 0.75, "mov_c": 2.2, "mov_d": 0.001}
PERF = {"modifier": XSRDIFF, "fallback": MOV, "beta": 1.0, "m_min": 0.5, "m_max": 1.5,
        "model_path": None}

# A curve with a deliberately simple slope: +0.001 SRDiff per Elo point, so a
# 100-point venue-adjusted edge is expected to produce +0.10 SRDiff.
MODEL = XsrModel({"kind": "linear", "version": "test_v1",
                  "global_fit": {"a": 0.0, "b": 0.001},
                  "folds": {"2015": {"a": 0.0, "b": 0.001, "trained_through": 2014}}})


def ctx(**kw):
    base = dict(season=2015, is_tie=False, point_diff=14, winner_advantage=100.0,
                winner_elo_diff_adjusted=100.0, winner_sr=0.50, loser_sr=0.40)
    base.update(kw)
    return GameContext(**base)


# ---------------------------------------------------------------- arithmetic

def test_xsrdiff_increases_monotonically_with_the_elo_gap():
    prev = None
    for elo_diff in range(-600, 601, 50):
        x = expected_sr_diff(elo_diff, MODEL, 2015)
        if prev is not None:
            assert x > prev, f"not monotonic at {elo_diff}"
        prev = x


def test_swapping_teams_negates_every_differential():
    a_sr, b_sr, elo_diff = 0.52, 0.41, 120.0
    d_a = actual_sr_diff(a_sr, b_sr)
    d_b = actual_sr_diff(b_sr, a_sr)
    x_a = expected_sr_diff(elo_diff, MODEL, 2015)
    x_b = expected_sr_diff(-elo_diff, MODEL, 2015)
    assert d_a == pytest.approx(-d_b)
    assert x_a == pytest.approx(-x_b)
    assert sr_plus(d_a, x_a) == pytest.approx(-sr_plus(d_b, x_b))


def test_multiplier_stays_inside_its_bounds_and_positive():
    for value in (-10.0, -0.5, -0.05, 0.0, 0.05, 0.5, 10.0):
        m = performance_multiplier(value, beta=4.0, m_min=0.5, m_max=1.5)
        assert 0.5 <= m <= 1.5
        assert m > 0


def test_a_missing_success_rate_is_never_zero():
    assert actual_sr_diff(None, 0.4) is None
    assert actual_sr_diff(0.4, None) is None
    assert sr_plus(None, 0.1) is None
    assert sr_plus(0.1, None) is None
    # ...and a None SR+ leaves the multiplier neutral rather than 1 + beta*0 by accident.
    assert performance_multiplier(None, beta=4.0, m_min=0.5, m_max=1.5) == 1.0


# ------------------------------------------------------------- layer routing

def test_missing_success_rate_takes_the_explicit_fallback_path():
    layer = build_layer(PERF, ELO_CFG, MODEL)
    m_full, name_full = layer.multiplier(ctx())
    m_missing, name_missing = layer.multiplier(ctx(winner_sr=None))
    assert name_full == XSRDIFF
    assert name_missing == MOV                      # recorded, not silently absorbed
    expected_mov = MovLayer(ELO_CFG["mov_c"], ELO_CFG["mov_d"]).multiplier(ctx(winner_sr=None))[0]
    assert m_missing == pytest.approx(expected_mov)
    assert m_full != pytest.approx(m_missing)


def test_a_season_with_no_fold_falls_back_rather_than_borrowing_the_global_fit():
    layer = build_layer(PERF, ELO_CFG, MODEL)
    _, name = layer.multiplier(ctx(season=2003))    # MODEL only has a 2015 fold
    assert name == MOV


def test_no_model_at_all_falls_back():
    layer = build_layer(PERF, ELO_CFG, None)
    _, name = layer.multiplier(ctx())
    assert name == MOV


def test_a_tie_is_recorded_as_its_own_path_not_as_a_success_rate_game():
    """A tie has no winner, so M = 1.0 comes from the tie rule rather than from the
    layer's formula. Recording it separately keeps "how many games actually used
    Success Rate?" an answerable question. result_only is the exception: M = 1 is
    its rule for every game, tie or not, so it truthfully reports itself."""
    for modifier in (MOV, RAW_SRDIFF, XSRDIFF):
        layer = build_layer({**PERF, "modifier": modifier}, ELO_CFG, MODEL)
        m, name = layer.multiplier(ctx(is_tie=True, point_diff=0, winner_sr=None, loser_sr=None))
        assert (m, name) == (1.0, TIE), modifier
    result_only = build_layer({**PERF, "modifier": RESULT_ONLY}, ELO_CFG, MODEL)
    assert result_only.multiplier(ctx(is_tie=True, point_diff=0)) == (1.0, RESULT_ONLY)


def test_result_only_is_exactly_one():
    assert ResultOnlyLayer().multiplier(ctx()) == (1.0, RESULT_ONLY)


def test_raw_srdiff_ignores_opponent_strength_and_xsrdiff_does_not():
    raw = build_layer({**PERF, "modifier": RAW_SRDIFF}, ELO_CFG, MODEL)
    xsr = build_layer(PERF, ELO_CFG, MODEL)
    weak, strong = ctx(winner_elo_diff_adjusted=-300.0), ctx(winner_elo_diff_adjusted=300.0)
    assert raw.multiplier(weak)[0] == pytest.approx(raw.multiplier(strong)[0])
    assert xsr.multiplier(weak)[0] > xsr.multiplier(strong)[0]


# ------------------------------------------------------------ configuration

def test_bad_performance_config_fails_loudly():
    with pytest.raises(ValueError, match="not one of"):
        load_performance_config({"modifier": "nope"})
    with pytest.raises(ValueError, match="cannot be a fallback"):
        load_performance_config({"modifier": XSRDIFF, "fallback": XSRDIFF})
    with pytest.raises(ValueError, match="m_min"):
        load_performance_config({"modifier": MOV, "m_min": 0.0, "m_max": 1.0})
    with pytest.raises(ValueError, match="m_min"):
        load_performance_config({"modifier": MOV, "m_min": 1.5, "m_max": 1.0})


def test_the_shipped_config_is_valid_and_names_a_real_model_path(repo_root):
    import json
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text())
    perf = load_performance_config(cfg["performance"])
    assert perf["model_path"], "an SR layer needs somewhere to load its fitted curve from"


# ------------------------------------------- the Elo invariants the guide lists

def _game(gid, season, hs, aws, neutral=0):
    return {"game_id": gid, "season_year": season, "game_date": f"{season}-09-01",
            "home_team_id": 1, "away_team_id": 2, "home_score": hs, "away_score": aws,
            "neutral_site": neutral, "went_ot": 0, "home_name": "Home", "away_name": "Away"}


def _run(games, modifier, success_rates=None, model=MODEL):
    layer = build_layer({**PERF, "modifier": modifier}, ELO_CFG, model)
    return run_elo(games, ELO_CFG, layer, success_rates or {})


@pytest.mark.parametrize("modifier", [MOV, RESULT_ONLY, RAW_SRDIFF, XSRDIFF])
def test_winner_gains_loser_loses_and_the_update_is_zero_sum(modifier):
    games = [_game(1, 2015, 35, 14), _game(2, 2015, 7, 31)]
    sr = {(1, 1): 0.30, (1, 2): 0.70, (2, 1): 0.70, (2, 2): 0.30}   # efficiency against the result
    rows, _, _ = _run(games, modifier, sr)
    by_game = {}
    for r in rows:
        by_game.setdefault(r[0], []).append(r)
    for gid, pair in by_game.items():
        changes = {r[1]: r[6] for r in pair}
        assert sum(changes.values()) == pytest.approx(0.0, abs=1e-9), "not zero-sum"
        winner = 1 if by_game[gid][0][0] == 1 else 2
        assert changes[winner] > 0, "winner must gain"
        assert changes[3 - winner] < 0, "loser must lose"


def test_a_bad_performance_day_softens_a_win_but_never_reverses_it():
    games = [_game(1, 2015, 35, 14)]
    strong = {(1, 1): 0.60, (1, 2): 0.30}    # winner far more efficient than expected
    weak = {(1, 1): 0.30, (1, 2): 0.60}      # winner far less efficient than expected
    m_strong = _run(games, XSRDIFF, strong)[0][0][5]
    m_weak = _run(games, XSRDIFF, weak)[0][0][5]
    assert m_strong > m_weak
    assert m_weak > 0
    assert _run(games, XSRDIFF, weak)[0][0][6] > 0, "the winner still gains Elo"


def test_the_two_stored_rows_share_one_multiplier_and_mirror_every_differential():
    games = [_game(1, 2015, 35, 14)]
    rows, _, _ = _run(games, XSRDIFF, {(1, 1): 0.55, (1, 2): 0.35})
    home, away = rows[0], rows[1]
    assert home[5] == away[5], "M scales a zero-sum update and must be shared"
    assert home[8] == pytest.approx(-away[8])      # elo_diff_adjusted
    assert home[11] == pytest.approx(-away[11])    # sr_diff
    assert home[12] == pytest.approx(-away[12])    # xsr_diff
    assert home[13] == pytest.approx(-away[13])    # sr_plus
    assert home[9] == 0.55 and home[10] == 0.35    # own SR, opponent SR


def test_historical_games_use_pregame_elo_not_the_rating_they_end_up_with():
    games = [_game(1, 2015, 35, 14), _game(2, 2015, 21, 20)]
    rows, _, _ = _run(games, XSRDIFF, {(1, 1): 0.55, (1, 2): 0.35, (2, 1): 0.52, (2, 2): 0.48})
    first_home, second_home = rows[0], rows[2]
    assert first_home[2] == ELO_CFG["initial_rating"]
    # The second game's pregame Elo is the first game's postgame Elo, never a later value.
    assert second_home[2] == pytest.approx(first_home[7])
    # ...and its elo_diff_adjusted is built from those same pregame ratings.
    assert second_home[8] == pytest.approx(second_home[2] + ELO_CFG["home_field"] - second_home[3])


def test_the_engine_is_unchanged_when_no_success_rate_exists():
    """The whole safety argument for shipping this: with no per-game SR, every
    game takes the fallback and ratings match the margin-of-victory engine
    exactly."""
    games = [_game(1, 2015, 35, 14), _game(2, 2016, 7, 31), _game(3, 2016, 17, 17)]
    baseline, _, _ = run_elo(games, ELO_CFG)                 # default layer == MOV
    candidate, _, _ = _run(games, XSRDIFF, {})               # xSRDiff with no data
    assert [r[:8] for r in candidate] == [r[:8] for r in baseline]


def test_walk_forward_folds_never_let_a_later_season_shape_an_earlier_game():
    from fit_xsrdiff import walk_forward_folds
    rows = [{"season": s, "elo_diff_adjusted": float(i % 200 - 100), "sr_diff": 0.001 * (i % 200 - 100)}
            for s in (2011, 2012, 2013) for i in range(500)]
    folds = walk_forward_folds(rows, min_rows=400)
    assert 2011 not in folds, "the first season has no prior data to train on"
    assert folds[2012]["trained_through"] == 2011
    assert folds[2013]["trained_through"] == 2012
    assert folds[2013]["n"] > folds[2012]["n"]


def test_the_fit_recovers_a_known_line():
    from fit_xsrdiff import fit_linear
    rows = [{"elo_diff_adjusted": float(x), "sr_diff": 0.02 + 0.0015 * x} for x in range(-400, 401, 10)]
    fit = fit_linear(rows)
    assert fit["a"] == pytest.approx(0.02, abs=1e-9)
    assert fit["b"] == pytest.approx(0.0015, abs=1e-12)
    assert fit["r2"] == pytest.approx(1.0)
    assert fit_linear([{"elo_diff_adjusted": 5.0, "sr_diff": 0.1}] * 10) is None   # no spread in x
