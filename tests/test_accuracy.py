"""The track-record export, held to the report it summarises.

src/accuracy.py is a reshaping of data/processed/validation_report.json, so
the thing worth testing is that the reshaping preserves the numbers and never
invents, rounds or reorders one. A page that misreports the model's own
accuracy is worse than no page.
"""
from __future__ import annotations

import json

import pytest

import accuracy
from accuracy import HEADLINE, METRICS, MODELS, build_accuracy


@pytest.fixture(scope="module")
def report(repo_root):
    path = repo_root / "data" / "processed" / "validation_report.json"
    if not path.exists():
        pytest.skip("no validation report -- run src/run_validation.py first")
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def built(repo_root, report):
    return build_accuracy(repo_root / "data" / "processed" / "validation_report.json")


def test_a_missing_report_is_absent_not_empty(tmp_path):
    """None, so the manifest omits the key and the page says so.

    An empty dict would publish a Track Record page with no numbers on it,
    which reads as "the model scored nothing" rather than "this build has no
    report".
    """
    assert build_accuracy(tmp_path / "nothing.json") is None


def test_the_headline_model_is_the_one_the_site_publishes():
    """predict_upcoming and the projection both run the Elo engine's expectation.

    If the published predictions ever came from a different model, this
    constant would have to move with them -- the page says "this is what the
    site publishes" in so many words.
    """
    assert HEADLINE == "elo"
    assert [m[0] for m in MODELS][0] == HEADLINE


def test_every_number_is_the_report_s_own(built, report):
    """No rounding, no recomputation: each metric is carried across untouched."""
    for row in built["models"]:
        overall = report["models"][row["key"]]["overall"]
        for metric in METRICS:
            assert row[metric] == overall[metric], (row["key"], metric)


def test_the_model_rows_are_the_configured_models_in_order(built):
    available = [key for key, _label, _blurb in MODELS if key in {"elo", "hybrid",
                 "elo_on_hybrid_games", "baseline_home_rate", "baseline_coin_flip"}]
    assert [r["key"] for r in built["models"]] == available
    assert sum(r["headline"] for r in built["models"]) == 1
    assert {r["key"] for r in built["models"] if r["baseline"]} == {
        "baseline_home_rate", "baseline_coin_flip"}


def test_the_baselines_are_actually_beaten(built):
    """A sanity floor on the data itself, not on the reshaping.

    If the published model ever scored worse than a coin flip, this page would
    be the first place anyone saw it -- and this test would be the first thing
    to fail, which is the point of having it.
    """
    rows = {r["key"]: r for r in built["models"]}
    assert rows["elo"]["brier"] < rows["baseline_home_rate"]["brier"]
    assert rows["elo"]["brier"] < rows["baseline_coin_flip"]["brier"]
    assert rows["elo"]["accuracy"] > rows["baseline_home_rate"]["accuracy"]
    assert rows["elo"]["brier_skill"] > 0


def test_the_season_series_is_the_headline_model_in_order(built, report):
    seasons = [r["season"] for r in built["by_season"]]
    assert seasons == sorted(seasons), "the page reverses this itself; the export is ascending"
    assert seasons == sorted(r["season_year"] for r in report["models"][HEADLINE]["by_season"])
    source = {r["season_year"]: r for r in report["models"][HEADLINE]["by_season"]}
    for row in built["by_season"]:
        for key in ("accuracy", "brier", "brier_skill", "base_rate", "n"):
            assert row[key] == source[row["season"]][key]


def test_the_calibration_bins_cover_the_probability_range(built):
    """Ten bins from 0 to 1, each with games in it, each labelled by its edges."""
    bins = built["calibration"]
    assert bins, "the headline model has no calibration curve"
    assert bins[0]["lower"] == pytest.approx(0.0)
    assert bins[-1]["upper"] == pytest.approx(1.0)
    for lower, upper in zip(bins, bins[1:]):
        assert lower["upper"] == pytest.approx(upper["lower"]), "a gap between bins"
    for b in bins:
        assert b["n"] > 0
        assert b["lower"] <= b["predicted"] <= b["upper"], b


def test_the_model_is_roughly_calibrated(built):
    """Stated probabilities within ten points of what happened, in every bin.

    This is the claim the page makes in its own heading ("when it says 70%,
    does it happen 70% of the time?"). It should be a test, not a hope.
    """
    for b in built["calibration"]:
        assert abs(b["observed"] - b["predicted"]) < 0.10, b


def test_the_fingerprint_identifies_the_run(built, report):
    assert built["fingerprint"] == report["generated_for"]["prediction_fingerprint"]
    assert built["bins"] == report["generated_for"]["bins"]


def test_a_report_without_the_headline_model_publishes_nothing(tmp_path):
    """Rather than silently promoting whichever model happens to be first."""
    path = tmp_path / "report.json"
    path.write_text(json.dumps({"models": {"hybrid": {"overall": {}, "seasons": []}},
                                "generated_for": {}}))
    assert build_accuracy(path) is None


def test_the_default_report_path_is_resolved_when_called(monkeypatch, tmp_path):
    """DEFAULT_REPORT is read through the module, not bound at import.

    A default argument binds once at def time, so `path=DEFAULT_REPORT` in the
    signature could not be monkeypatched and a test that patched the module
    attribute would pass without proving anything. This checks the real thing:
    repointing the module constant changes where the function looks.
    """
    monkeypatch.setattr(accuracy, "DEFAULT_REPORT", tmp_path / "absent.json")
    assert build_accuracy() is None
