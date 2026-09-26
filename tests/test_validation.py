"""
MODEL-07 (model validation harness).

The guide names three fixtures -- walk-forward folds, known Brier/log-loss
values, and edge-case diagnostics -- and they are the first three groups here.
The rest pin the two properties the harness exists to guarantee: that a
configuration reproduces its own report, and that predictive and achievement
results are never merged into one number.
"""
import json
import math
import sqlite3

import pytest

from run_validation import build_report, config_fingerprint, evaluate_predictive
from validation import (Fold, assert_chronological, brier_score, calibration_error, log_loss,
                        reliability_bins, score_predictions, season_folds, sharpness)
from validation.achievement import (conference_behavior, cupcake_resistance, edge_cases,
                                    elite_win_reward, schedule_strength_sensitivity)


# -------------------------------------------------- known metric values

def test_brier_and_log_loss_match_hand_computed_values():
    # A coin flip on a balanced pair: Brier 0.25, log loss ln 2.
    assert brier_score([(0.5, 1), (0.5, 0)]) == pytest.approx(0.25)
    assert log_loss([(0.5, 1), (0.5, 0)]) == pytest.approx(math.log(2))
    # Perfect confident forecasts score zero on both.
    assert brier_score([(1.0, 1), (0.0, 0)]) == pytest.approx(0.0)
    assert log_loss([(1.0, 1), (0.0, 0)]) == pytest.approx(0.0, abs=1e-9)
    # A single 0.8 call that came in: (0.8-1)^2 = 0.04, -ln(0.8).
    assert brier_score([(0.8, 1)]) == pytest.approx(0.04)
    assert log_loss([(0.8, 1)]) == pytest.approx(-math.log(0.8))
    # A tie counts as half an outcome, not a dropped game.
    assert brier_score([(0.5, 0.5)]) == pytest.approx(0.0)


def test_log_loss_stays_finite_when_a_model_is_confidently_wrong():
    """Without clamping this is infinite, and one such game would destroy the
    whole report rather than showing up as a bad score."""
    v = log_loss([(1.0, 0)])
    assert math.isfinite(v)
    assert v > 25


def test_calibration_and_sharpness_capture_different_failures():
    # Always 0.5 on a balanced set: perfectly calibrated, completely unsharp.
    flat = [(0.5, 1), (0.5, 0)] * 50
    assert calibration_error(flat) == pytest.approx(0.0)
    assert sharpness(flat) == pytest.approx(0.0)
    # Confident and right: sharp and calibrated.
    sure = [(0.9, 1)] * 90 + [(0.9, 0)] * 10
    assert calibration_error(sure) == pytest.approx(0.0, abs=1e-9)
    assert sharpness(sure) == pytest.approx(0.4)
    # Confident and wrong: still sharp, now badly calibrated.
    wrong = [(0.9, 1)] * 10 + [(0.9, 0)] * 90
    assert calibration_error(wrong) == pytest.approx(0.8, abs=1e-9)
    assert sharpness(wrong) == pytest.approx(0.4)


def test_reliability_bins_group_by_predicted_probability():
    bins = reliability_bins([(0.05, 0), (0.15, 0), (0.95, 1)], bins=10)
    assert [b["bin"] for b in bins] == [0, 1, 9]
    assert bins[0]["n"] == 1 and bins[-1]["observed"] == 1.0
    # A probability of exactly 1.0 belongs in the last bin, not one past the end.
    assert reliability_bins([(1.0, 1)], bins=10)[0]["bin"] == 9


def test_empty_input_is_none_rather_than_zero():
    """A missing score is not a perfect score."""
    for fn in (brier_score, log_loss, calibration_error, sharpness):
        assert fn([]) is None
    assert score_predictions([])["n"] == 0


# -------------------------------------------------- walk-forward folds

def test_folds_only_ever_train_on_earlier_seasons():
    folds = season_folds(range(2000, 2011), min_train_seasons=5)
    assert [f.test_season for f in folds] == list(range(2005, 2011))
    for f in folds:
        assert max(f.train) < f.test_season
        assert f.trained_through == f.test_season - 1
    # The first five seasons cannot be tested: there is nothing legal to train on.
    assert all(f.test_season >= 2005 for f in folds)


def test_a_rolling_window_still_never_reaches_forward():
    folds = season_folds(range(2000, 2011), min_train_seasons=3, window=3)
    for f in folds:
        assert len(f.train) == 3
        assert max(f.train) < f.test_season
    assert folds[0].train == (2000, 2001, 2002) and folds[0].test_season == 2003


def test_gaps_in_the_season_list_do_not_invent_folds():
    folds = season_folds([2000, 2001, 2002, 2003, 2004, 2010], min_train_seasons=5)
    assert [f.test_season for f in folds] == [2010]
    assert folds[0].train == (2000, 2001, 2002, 2003, 2004)


def test_leakage_is_caught_rather_than_scored():
    assert_chronological([(2010, 2009), (2011, None)])          # fine
    with pytest.raises(ValueError, match="future data leaked"):
        assert_chronological([(2010, 2010)])                     # its own season
    with pytest.raises(ValueError, match="future data leaked"):
        assert_chronological([(2010, 2012)])                     # a later season


def test_evaluate_predictive_pools_observations_rather_than_averaging_folds():
    """A short season must not weigh as much as a full one."""
    obs = {"m": {y: [(0.6, 1)] * 100 for y in range(2000, 2006)}}
    obs["m"][2005] = [(0.6, 0)] * 1000          # one big, badly-predicted season
    rep = evaluate_predictive(obs, min_train=5)["m"]
    assert rep["seasons_scored"] == 1 and rep["folds"][0]["test_season"] == 2005
    assert rep["aggregate"]["n"] == 1000
    assert rep["aggregate"]["brier"] == pytest.approx(0.36)


# -------------------------------------------------- achievement diagnostics

def _ach_db(tmp_path, games, hybrid, elo, membership=()):
    conn = sqlite3.connect(str(tmp_path / "a.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, game_phase TEXT,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER, game_coe REAL,
            hybrid_expectation REAL, result_type TEXT);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, elo_expectation REAL,
            opponent_pregame_elo REAL);
        CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER,
            conference_real TEXT);
    """)
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?,?,?)", games)
    conn.executemany("INSERT INTO hybrid_game_ratings VALUES (?,?,?,?,?)", hybrid)
    conn.executemany("INSERT INTO elo_game_history VALUES (?,?,?,?)", elo)
    conn.executemany("INSERT INTO team_membership_by_season VALUES (?,?,?)", membership)
    conn.commit()
    return conn


def test_elite_win_reward_sees_opponent_quality(tmp_path):
    """A system that pays more for beating strong teams must show a ratio above 1."""
    games, hybrid, elo = [], [], []
    for i in range(200):
        strong = i % 2 == 0
        gid = i + 1
        games.append((gid, 2020, "regular", 1, 2, 30, 10))
        hybrid.append((gid, 1, 4.0 if strong else 2.0, 0.5, "WIN"))
        elo.append((gid, 1, 0.5, 1800.0 if strong else 1200.0))
    conn = _ach_db(tmp_path, games, hybrid, elo)
    r = elite_win_reward(conn)
    assert r["elite_win_mean"] == pytest.approx(4.0)
    assert r["weak_win_mean"] == pytest.approx(2.0)
    assert r["ratio"] == pytest.approx(2.0)
    assert cupcake_resistance(conn)["weak_wins_per_elite_win"] == pytest.approx(2.0)
    conn.close()


def test_a_system_blind_to_opponent_quality_shows_a_ratio_of_one(tmp_path):
    """The diagnostic has to be able to fail, or it is not measuring anything."""
    games, hybrid, elo = [], [], []
    for i in range(200):
        gid = i + 1
        games.append((gid, 2020, "regular", 1, 2, 30, 10))
        hybrid.append((gid, 1, 3.0, 0.5, "WIN"))                 # flat award
        elo.append((gid, 1, 0.5, 1800.0 if i % 2 == 0 else 1200.0))
    conn = _ach_db(tmp_path, games, hybrid, elo)
    assert elite_win_reward(conn)["ratio"] == pytest.approx(1.0)
    conn.close()


def test_schedule_strength_sensitivity_detects_a_system_that_rewards_it(tmp_path):
    """Team-seasons with identical win totals but different schedules: a system
    that pays for difficulty must correlate positively."""
    games, hybrid, elo = [], [], []
    gid = 0
    for team in range(1, 61):
        sos = 1200 + team * 10
        for _ in range(8):                                        # same record for everyone
            gid += 1
            games.append((gid, 2020, "regular", team, 99, 30, 10))
            hybrid.append((gid, team, sos / 1000.0, 0.5, "WIN"))  # award tracks difficulty
            elo.append((gid, team, 0.5, sos))
    conn = _ach_db(tmp_path, games, hybrid, elo)
    r = schedule_strength_sensitivity(conn)
    assert r["mean_correlation"] > 0.9
    conn.close()


def test_conference_behavior_reports_internal_games_separately(tmp_path):
    conn = _ach_db(tmp_path,
                   games=[(1, 2020, "regular", 1, 2, 21, 14), (2, 2020, "regular", 1, 3, 28, 7)],
                   hybrid=[(1, 1, 3.0, 0.5, "WIN"), (2, 1, 3.0, 0.5, "WIN")],
                   elo=[(1, 1, 0.5, 1500.0), (2, 1, 0.5, 1500.0)],
                   membership=[(1, 2020, "Alpha"), (2, 2020, "Alpha"), (3, 2020, "Beta")])
    r = conference_behavior(conn)
    assert r["internal_regular_season_games"] == 1, "the Alpha-vs-Alpha game is internal"
    conn.close()


def test_edge_cases_handle_the_pathological_shapes(tmp_path):
    """The edge-case fixture: a one-game season and a winless season must both
    produce defensible numbers rather than a crash or a silent zero."""
    games, hybrid, elo = [], [], []
    gid = 0
    for _ in range(8):                                    # team 1: winless against strong teams
        gid += 1
        games.append((gid, 2020, "regular", 1, 99, 7, 30))
        hybrid.append((gid, 1, 0.0, 0.2, "LOSS"))
        elo.append((gid, 1, 0.2, 1900.0))
    for _ in range(8):                                    # team 2: undefeated against weak ones
        gid += 1
        games.append((gid, 2020, "regular", 2, 98, 40, 3))
        hybrid.append((gid, 2, 2.1, 0.95, "WIN"))
        elo.append((gid, 2, 0.95, 1100.0))
    gid += 1                                              # team 3: a single game
    games.append((gid, 2020, "regular", 3, 97, 20, 17))
    hybrid.append((gid, 3, 3.0, 0.5, "WIN"))
    elo.append((gid, 3, 0.5, 1500.0))
    conn = _ach_db(tmp_path, games, hybrid, elo)
    r = edge_cases(conn)
    assert r["winless_vs_hardest_schedule"]["team_id"] == 1
    assert r["winless_vs_hardest_schedule"]["coe"] == 0.0, "losses are worth zero, not negative"
    assert r["undefeated_vs_weakest_schedule"]["team_id"] == 2
    assert r["single_game_season"]["team_id"] == 3
    assert r["negative_season_totals"] == 0
    # The undefeated-against-nobody team must not outscore a real schedule by accident.
    assert r["undefeated_vs_weakest_schedule"]["coe"] == pytest.approx(16.8)
    conn.close()


# -------------------------------------------------- reproducibility and separation

def test_the_same_configuration_always_fingerprints_the_same():
    cfg = {"elo": {"k": 20, "_comment": "ignored"}, "coe": {"win_base": 2.0}}
    a, _ = config_fingerprint(cfg)
    b, _ = config_fingerprint({"coe": {"win_base": 2.0}, "elo": {"_comment": "ignored", "k": 20}})
    assert a == b, "key order and comments must not invent a new experiment"
    c, _ = config_fingerprint({"elo": {"k": 21}, "coe": {"win_base": 2.0}})
    assert c != a, "a real parameter change must produce a new report"


def test_the_fingerprint_embeds_the_configuration_it_names():
    cfg = {"elo": {"k": 20, "_comment": "x"}}
    h, clean = config_fingerprint(cfg)
    assert clean == {"elo": {"k": 20}}
    assert len(h) == 12


def test_a_report_keeps_predictive_and_achievement_apart(db_conn, repo_root):
    """The acceptance criterion: two sections, never a single combined score."""
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text(encoding="utf-8"))
    report = build_report(db_conn, cfg)
    assert set(report) >= {"config_hash", "config", "predictive", "achievement"}
    assert "elo" in report["predictive"]
    assert set(report["achievement"]) >= {"schedule_strength", "elite_win_reward",
                                          "cupcake_resistance", "conference_behavior",
                                          "historical_overlap", "edge_cases"}
    # No key anywhere merges the two into one number.
    assert "score" not in report
    assert not (set(report["predictive"]) & set(report["achievement"]))


def test_a_report_is_reproducible_from_the_same_inputs(db_conn, repo_root):
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text(encoding="utf-8"))
    a = build_report(db_conn, cfg)
    b = build_report(db_conn, cfg)
    assert a["config_hash"] == b["config_hash"]
    assert a["predictive"] == b["predictive"]
    assert a["achievement"] == b["achievement"]


def test_every_real_predictive_fold_is_out_of_sample(db_conn, repo_root):
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text(encoding="utf-8"))
    report = build_report(db_conn, cfg)
    for model, r in report["predictive"].items():
        assert r["folds"], model
        for f in r["folds"]:
            assert f["trained_through"] < f["test_season"], f"{model}: {f}"
            assert 0 <= f["brier"] <= 1


def test_every_achievement_diagnostic_explains_its_own_value(db_conn, repo_root):
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text(encoding="utf-8"))
    report = build_report(db_conn, cfg)
    for name, d in report["achievement"].items():
        assert "reading" in d, f"{name} has no reading, so its number cannot be interpreted"
        assert d["reading"].strip()
