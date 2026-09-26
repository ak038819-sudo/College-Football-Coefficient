"""
Tests for the shared validation harness (MODEL-07).

Two halves, deliberately:

1. The metrics, against values computed by hand in the test itself. A metric
   that is only compared against its own implementation proves nothing, so
   every expected number here is written out from the definition.
2. The harness against the REAL database, where the properties worth checking
   are of the actual ratings: that the engine's own pregame expectations beat
   a coin flip, that the per-season rows reconcile with the overall row, and
   that the report is deterministic. These skip cleanly when db/league.db has
   not been built, the same as the rest of the suite.
"""
from __future__ import annotations

import json
import math
import subprocess
import sys

import pytest

import run_validation
import validation


# --------------------------------------------------------------------------
# Metrics, against hand-computed values
# --------------------------------------------------------------------------

def test_brier_is_mean_squared_error():
    # (0.8 - 1)^2 = 0.04 ; (0.3 - 0)^2 = 0.09 ; mean = 0.065
    assert validation.brier_score([(0.8, 1.0), (0.3, 0.0)]) == pytest.approx(0.065)


def test_brier_of_a_coin_flip_on_anything_is_a_quarter():
    """The baseline every other number in the suite is read against."""
    assert validation.brier_score([(0.5, 1.0), (0.5, 0.0), (0.5, 1.0)]) == pytest.approx(0.25)


def test_log_loss_of_a_coin_flip_is_ln_two():
    assert validation.log_loss([(0.5, 1.0), (0.5, 0.0)]) == pytest.approx(math.log(2))


def test_log_loss_punishes_a_confident_miss_far_harder_than_brier():
    """
    The reason log loss is in the report at all: a model that is confidently
    wrong once looks nearly fine on Brier and clearly bad here.
    """
    mild = [(0.6, 0.0)]
    confident = [(0.99, 0.0)]
    brier_ratio = validation.brier_score(confident) / validation.brier_score(mild)
    loss_ratio = validation.log_loss(confident) / validation.log_loss(mild)
    assert brier_ratio < 3          # 0.9801 / 0.36
    assert loss_ratio > 4           # 4.605 / 0.916


def test_log_loss_is_finite_when_a_prediction_was_certain_and_wrong():
    """
    An unclamped log loss would be infinite here and would take the whole
    comparison with it, which is exactly when a report is most needed.
    """
    value = validation.log_loss([(1.0, 0.0), (0.6, 1.0)])
    assert math.isfinite(value)
    assert value > 10  # still enormous -- clamped, not forgiven


def test_a_tie_scores_as_half_not_as_a_win():
    assert validation.brier_score([(0.5, 0.5)]) == pytest.approx(0.0)
    assert validation.accuracy([(0.9, 0.5)]) == pytest.approx(0.5)
    assert validation.accuracy([(0.5, 1.0)]) == pytest.approx(0.5)


def test_accuracy_counts_the_favourite_winning():
    # right, wrong, right
    assert validation.accuracy([(0.7, 1.0), (0.7, 0.0), (0.2, 0.0)]) == pytest.approx(2 / 3)


def test_brier_skill_is_zero_for_the_base_rate_itself():
    """Predicting the observed rate for everything is the definition of no skill."""
    pairs = [(0.75, 1.0), (0.75, 1.0), (0.75, 1.0), (0.75, 0.0)]
    assert validation.base_rate(pairs) == pytest.approx(0.75)
    assert validation.brier_skill_score(pairs) == pytest.approx(0.0)


def test_brier_skill_is_negative_for_a_model_worse_than_the_base_rate():
    pairs = [(0.1, 1.0), (0.1, 1.0), (0.1, 1.0), (0.9, 0.0)]
    assert validation.brier_skill_score(pairs) < 0


def test_brier_skill_is_none_when_every_outcome_is_identical():
    """No spread to explain, so a skill score would be a division by zero."""
    assert validation.brier_skill_score([(0.8, 1.0), (0.3, 1.0)]) is None


def test_calibration_bins_report_the_gap_and_its_sign():
    # Four games predicted at 0.9, three of which were won: predicted 0.9,
    # observed 0.75, so the model was OVERconfident and the gap is negative.
    bins = validation.calibration_bins([(0.9, 1.0), (0.9, 1.0), (0.9, 1.0), (0.9, 0.0)])
    assert len(bins) == 1
    assert bins[0]["n"] == 4
    assert bins[0]["mean_predicted"] == pytest.approx(0.9)
    assert bins[0]["observed_rate"] == pytest.approx(0.75)
    assert bins[0]["gap"] == pytest.approx(-0.15)


def test_a_prediction_of_exactly_one_lands_in_the_top_bin():
    """Not in a bin of its own past the end of the range."""
    bins = validation.calibration_bins([(1.0, 1.0)], bins=10)
    assert len(bins) == 1
    assert bins[0]["bin_lower"] == pytest.approx(0.9)
    assert bins[0]["bin_upper"] == pytest.approx(1.0)


def test_empty_bins_are_omitted_rather_than_reported_as_zeros():
    bins = validation.calibration_bins([(0.05, 0.0), (0.95, 1.0)])
    assert [b["n"] for b in bins] == [1, 1]  # not ten rows, eight of them empty


def test_calibration_error_weights_bins_by_how_many_games_they_hold():
    # bin 0.0-0.1: one game, predicted 0.05, observed 0.0 -> gap 0.05
    # bin 0.9-1.0: three games, predicted 0.95, observed 1.0 -> gap 0.05
    pairs = [(0.05, 0.0), (0.95, 1.0), (0.95, 1.0), (0.95, 1.0)]
    assert validation.calibration_error(pairs) == pytest.approx(0.05)


def test_a_perfectly_calibrated_but_useless_model_has_zero_calibration_error():
    """
    Predicting the base rate for everything is perfectly calibrated and has no
    skill whatsoever -- which is why the report shows both numbers, and why
    neither one alone decides anything.
    """
    pairs = [(0.5, 1.0), (0.5, 0.0)]
    assert validation.calibration_error(pairs) == pytest.approx(0.0)
    assert validation.brier_skill_score(pairs) == pytest.approx(0.0)


def test_score_of_nothing_returns_every_key_rather_than_raising():
    result = validation.score([])
    assert result["n"] == 0
    assert set(result) == set(validation.METRIC_KEYS)
    assert all(result[key] is None for key in validation.METRIC_KEYS if key != "n")


# --------------------------------------------------------------------------
# Walk-forward report shape
# --------------------------------------------------------------------------

def test_walk_forward_splits_by_season_and_keeps_the_overall_row():
    entries = [(2001, 0.8, 1.0), (2001, 0.4, 0.0), (2002, 0.9, 0.0)]
    report = validation.walk_forward_report("m", entries)

    assert report["seasons"] == [2001, 2002]
    assert report["overall"]["n"] == 3
    assert [row["season_year"] for row in report["by_season"]] == [2001, 2002]
    assert sum(row["n"] for row in report["by_season"]) == report["overall"]["n"]
    # 2002's single confident miss must not be diluted by 2001's good season.
    assert report["by_season"][1]["brier"] == pytest.approx(0.81)


def test_from_season_drops_earlier_seasons_and_records_how_many():
    entries = [(1980, 0.5, 1.0), (2001, 0.8, 1.0), (2002, 0.9, 1.0)]
    report = validation.walk_forward_report("m", entries, from_season=2001)

    assert report["from_season"] == 2001
    assert report["n_dropped_before_from_season"] == 1
    assert report["seasons"] == [2001, 2002]
    assert report["overall"]["n"] == 2


def test_from_season_that_drops_everything_still_returns_a_readable_report():
    report = validation.walk_forward_report("m", [(1980, 0.5, 1.0)], from_season=2030)
    assert report["overall"]["n"] == 0
    assert report["by_season"] == []
    assert report["calibration"] == []


# --------------------------------------------------------------------------
# Rank diagnostics
# --------------------------------------------------------------------------

def test_spearman_is_one_for_an_identical_ordering_and_minus_one_for_a_reversed_one():
    assert validation.spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert validation.spearman([1, 2, 3, 4], [40, 30, 20, 10]) == pytest.approx(-1.0)


def test_spearman_ranks_rather_than_values():
    """A monotone but wildly non-linear relationship is still a perfect ranking."""
    assert validation.spearman([1, 2, 3, 4], [1, 4, 9, 1000]) == pytest.approx(1.0)


def test_spearman_averages_tied_ranks():
    # Ties on the right: ranks become 1, 2.5, 2.5, 4 rather than an arbitrary order.
    assert validation.spearman([1, 2, 3, 4], [1, 2, 2, 3]) == pytest.approx(0.9486832980505138)


def test_spearman_is_none_without_spread():
    assert validation.spearman([1, 1, 1], [1, 2, 3]) is None
    assert validation.spearman([1.0], [2.0]) is None


def test_spearman_rejects_mismatched_lengths():
    with pytest.raises(ValueError):
        validation.spearman([1, 2, 3], [1, 2])


def test_rank_agreement_measures_disagreement_in_ranks():
    left = {"a": 10.0, "b": 8.0, "c": 6.0}
    right = {"a": 10.0, "b": 6.0, "c": 8.0}  # b and c swapped
    result = validation.rank_agreement(left, right)

    assert result["n"] == 3
    assert result["max_abs_rank_change"] == 1
    assert result["mean_abs_rank_change"] == pytest.approx(2 / 3)
    assert result["spearman"] == pytest.approx(0.5)


def test_rank_agreement_uses_only_the_keys_both_sides_have():
    result = validation.rank_agreement({"a": 1.0, "b": 2.0, "gone": 3.0}, {"a": 1.0, "b": 2.0})
    assert result["n"] == 2


def test_rank_agreement_of_nothing_shared_is_reported_not_raised():
    result = validation.rank_agreement({"a": 1.0}, {"b": 2.0})
    assert result == {"n": 0, "spearman": None, "mean_abs_rank_change": None,
                      "max_abs_rank_change": None}


def test_rank_stability_only_compares_adjacent_seasons():
    """A gap year would mix a real change with a missing one."""
    values = {2001: {"a": 1.0, "b": 2.0},
              2002: {"a": 2.0, "b": 1.0},
              2005: {"a": 1.0, "b": 2.0}}
    rows = validation.rank_stability(values)
    assert [(row["from_season"], row["to_season"]) for row in rows] == [(2001, 2002)]


def test_association_sign_says_whether_a_weak_schedule_pays():
    achievement = {"a": 10.0, "b": 5.0, "c": 1.0}
    tough_schedule = {"a": 1600.0, "b": 1500.0, "c": 1400.0}
    weak_schedule = {"a": 1400.0, "b": 1500.0, "c": 1600.0}

    assert validation.association(achievement, tough_schedule, "sos")["spearman"] == pytest.approx(1.0)
    assert validation.association(achievement, weak_schedule, "sos")["spearman"] == pytest.approx(-1.0)


# --------------------------------------------------------------------------
# Against the real database
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def elo_scored(db_conn):
    rows = run_validation.elo_rows(db_conn)
    if not rows:
        pytest.skip("no scored games in elo_game_history -- run build_elo.py first")
    return run_validation.entries(rows)


def _pairs(entries):
    """Drop the season, leaving what the metric functions take."""
    return [(p, s) for _, p, s in entries]


def test_the_real_elo_expectations_beat_a_coin_flip(elo_scored):
    """
    The engine's own pregame numbers, scored against what happened. If this
    ever fails, the ratings are not predictive at all and nothing downstream
    of them means anything.
    """
    assert validation.brier_score(_pairs(elo_scored)) < 0.25


def test_the_real_elo_expectations_beat_the_home_win_rate(elo_scored):
    """
    The harder baseline: home-field advantage alone. Elo has to add something
    beyond knowing that home teams win about 60% of the time.
    """
    assert validation.brier_skill_score(_pairs(elo_scored)) > 0.05


def test_one_row_per_game_not_two(db_conn, elo_scored):
    """
    Scoring both sides of a game would double every n in the report while
    leaving the metrics unchanged -- the kind of bug that hides.
    """
    games = db_conn.execute(
        "SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchone()[0]
    assert len(elo_scored) == games


def test_per_season_rows_reconcile_with_the_overall_row(elo_scored):
    report = validation.walk_forward_report("elo", elo_scored)
    assert sum(row["n"] for row in report["by_season"]) == report["overall"]["n"]
    # A weighted mean of the per-season Briers must be the overall Brier.
    weighted = sum(row["n"] * row["brier"] for row in report["by_season"]) / report["overall"]["n"]
    assert weighted == pytest.approx(report["overall"]["brier"])


def test_every_real_expectation_is_a_probability(elo_scored):
    assert all(0.0 <= p <= 1.0 for _, p, _ in elo_scored)


def test_hybrid_is_compared_against_elo_on_the_same_games(db_conn):
    """
    The hybrid layer covers fewer games than Elo does, so the report has to
    offer a like-for-like row. Without it, the two Briers describe different
    populations and the comparison is meaningless.
    """
    elo = run_validation.elo_rows(db_conn)
    hybrid = run_validation.hybrid_rows(db_conn)
    if not hybrid:
        pytest.skip("no hybrid ratings built")
    hybrid_games = {row[0] for row in hybrid}
    restricted = run_validation.restricted_to(elo, hybrid_games)
    assert len(restricted) == len(hybrid)
    assert {row[0] for row in restricted} == hybrid_games


def test_the_report_is_reproducible_and_machine_readable(db_path, repo_root, tmp_path):
    """
    The point of writing a report at all is that a later run can be compared
    against it, which requires that a rerun on unchanged data produces an
    identical file.
    """
    json_out = tmp_path / "validation_report.json"
    csv_out = tmp_path / "validation_by_season.csv"
    command = [sys.executable, "src/run_validation.py", "--db", str(db_path),
               "--json-out", str(json_out), "--csv-out", str(csv_out)]

    first = subprocess.run(command, cwd=repo_root, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    written = json_out.read_text()
    report = json.loads(written)

    assert report["models"]["elo"]["overall"]["brier"] < 0.25
    assert report["generated_for"]["prediction_fingerprint"]
    assert csv_out.read_text().splitlines()[0].startswith("model,season_year,")

    second = subprocess.run(command, cwd=repo_root, capture_output=True, text=True)
    assert second.returncode == 0, second.stderr
    assert json_out.read_text() == written
