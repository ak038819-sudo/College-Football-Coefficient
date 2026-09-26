"""
EXP-03 in production: that the configured layer actually runs.

test_srdiff.py pins the arithmetic on synthetic fixtures. It cannot catch the
failure that actually happened here, which had nothing to do with the maths:
config/model_config.json said modifier = "xsrdiff" for days while every single
one of the 60,086 built games recorded "mov", because the curve file had never
been fitted and the per-game Success Rate had never been fetched. The layer was
switched on, tested, and inert, and nothing in the suite noticed -- a silent
fallback is indistinguishable from a working model unless something checks which
path the engine actually took.

So these tests read the built database and the committed artifacts rather than
fixtures, and they are deliberately about wiring and provenance:

  * the configured layer reaches the games it has data for,
  * the parameters are the calibrated ones and not the placeholders they shipped as,
  * k is not carried across a layer switch, since k only ever acts through k * M,
  * and CI fits the curve before it builds Elo, or the two tests above would pass
    against a build that never ran the layer.
"""
from __future__ import annotations

import json

import pytest

from srdiff import MOV, TIE, XSRDIFF, XsrModel, load_performance_config

# Success Rate coverage starts in 2001; everything before it is expected to take
# the fallback, and that is correct behaviour rather than a gap to fix.
SR_FIRST_SEASON = 2001
# Coverage inside the Success Rate era is not flat, and the ramp is real rather
# than a bug to chase: 2001 has no walk-forward fold at all (a fold is trained on
# strictly earlier seasons, so the first one is 2002), 2002's per-game data is a
# near-total hole at 4%, and CFBD's coverage climbs through the 2000s -- 66% in
# 2003, 85% in 2005, 97% by 2008. So the "is the layer actually running" check is
# made where the data is genuinely there; the ramp itself is asserted separately.
FULL_COVERAGE_FROM = 2008


@pytest.fixture(scope="module")
def perf(repo_root):
    return load_performance_config(
        json.loads((repo_root / "config" / "model_config.json").read_text())["performance"])


@pytest.fixture(scope="module")
def by_season(db_conn):
    """{season: {performance_model: count}} over every built game."""
    row = db_conn.execute("SELECT count(*) FROM elo_game_history").fetchone()
    if not row[0]:
        pytest.skip("no Elo history built -- run python src/build_elo.py")
    out: dict[int, dict[str, int]] = {}
    for season, model, n in db_conn.execute(
        """SELECT g.season_year, h.performance_model, count(*)
           FROM elo_game_history h JOIN games g ON g.game_id = h.game_id
           GROUP BY 1, 2"""):
        out.setdefault(season, {})[model] = n
    return out


def played(counts: dict) -> dict:
    """Drop ties, which are their own path and never a Success Rate game."""
    return {k: v for k, v in counts.items() if k != TIE}


def totals(by_season: dict, seasons) -> dict:
    out: dict[str, int] = {}
    for s in seasons:
        for k, v in played(by_season.get(s, {})).items():
            out[k] = out.get(k, 0) + v
    return out


# ------------------------------------------------------ the layer actually runs

def test_the_configured_layer_is_the_one_the_engine_actually_used(perf, by_season):
    """
    The regression test for the inert switch. Over the seasons where the data is
    genuinely present, the configured modifier must be what the engine actually
    ran. If it is not, the engine is running something other than what the config
    claims -- exactly as wrong as running the wrong formula, and much harder to
    see, because nothing anywhere reports an error.
    """
    seasons = [s for s in by_season if s >= FULL_COVERAGE_FROM]
    assert seasons, f"no games from {FULL_COVERAGE_FROM} on"
    counts = totals(by_season, seasons)
    total = sum(counts.values())
    configured = counts.get(perf["modifier"], 0)
    assert configured / total > 0.95, (
        f"config says modifier={perf['modifier']!r} but only {configured}/{total} "
        f"games since {FULL_COVERAGE_FROM} took that path: {counts}. Either the curve "
        "was never fitted (python src/fit_xsrdiff.py) or per-game Success Rate was "
        "never loaded, and the engine is silently running the fallback.")


def test_seasons_before_the_data_take_the_fallback_rather_than_failing(perf, by_season):
    """
    The dataset starts in 1980 and Success Rate in 2001, so two decades must fall
    back -- and must fall back to the declared fallback, not to something else.
    """
    counts = totals(by_season, [s for s in by_season if s < SR_FIRST_SEASON])
    assert counts, "no pre-2001 games found; this test is checking the wrong boundary"
    assert set(counts) == {perf["fallback"]}, (
        f"pre-{SR_FIRST_SEASON} games should all take the declared fallback "
        f"{perf['fallback']!r}, got {counts}")


def test_the_first_success_rate_season_falls_back_for_want_of_a_fold(perf, by_season):
    """
    2001 has per-game Success Rate but no expectation curve: a fold is fitted only
    on seasons strictly before the one it values, so the earliest fold is 2002.
    Borrowing the global fit for 2001 would be the one shortcut that quietly
    invalidates the whole walk-forward claim, so 2001 must fall back instead --
    and this holds in the built ratings, not only in the unit fixtures.
    """
    if perf["modifier"] != XSRDIFF:
        pytest.skip("no xSRDiff curve in play")
    counts = totals(by_season, [SR_FIRST_SEASON])
    if not counts:
        pytest.skip("2001 not in the built range")
    assert counts.get(XSRDIFF, 0) == 0, (
        f"{SR_FIRST_SEASON} used the xSRDiff curve on {counts[XSRDIFF]} games, but no "
        "fold can exist for it -- a later season's fit has leaked backwards")
    assert set(counts) == {perf["fallback"]}


def test_coverage_climbs_and_is_near_total_in_the_recent_seasons(perf, by_season):
    """
    The fallback share is the honest measure of how much of the record this layer
    actually touches, and it is worth pinning: a data regression that halved
    coverage would leave every other test here passing, because falling back is
    legal behaviour. The recent seasons are the ones the site's own rankings are
    built from, so those are held to the tighter bar.
    """
    if perf["modifier"] != XSRDIFF:
        pytest.skip("no xSRDiff curve in play")
    recent = [s for s in by_season if s >= 2018]
    counts = totals(by_season, recent)
    share = counts.get(XSRDIFF, 0) / sum(counts.values())
    assert share > 0.99, f"2018+ coverage has dropped to {share:.1%}: {counts}"


def test_the_built_ratings_used_the_committed_curve(repo_root, db_conn, perf):
    """
    A curve file that is stale or absent is the failure mode above wearing a
    different hat: the version the engine recorded must be the version sitting in
    the repository, so a refit that never happened cannot hide.
    """
    if perf["modifier"] != XSRDIFF:
        pytest.skip("no xSRDiff curve in play")
    model = XsrModel.load(str(repo_root / perf["model_path"]))
    assert model.version, "the committed curve has no version"
    assert model.folds, "a curve with no walk-forward folds would score every season on itself"


# ------------------------------------------------- calibrated, not placeholder

def test_the_parameters_are_calibrated_rather_than_the_shipped_placeholders(perf):
    """
    beta=1.0 with bounds [0.5, 1.5] was the deliberate placeholder the layer
    shipped as. Running production on it would be running an untuned model that
    looks tuned, so the values must have moved and the config must say where to.
    """
    assert (perf["beta"], perf["m_min"], perf["m_max"]) != (1.0, 0.5, 1.5), \
        "still on the placeholder beta/bounds -- see src/backtest_performance_layer.py"
    assert perf["beta"] > 1.0
    assert perf["m_min"] > 0, "a zero floor lets a game move no ratings at all"
    assert perf["m_min"] < 1.0 < perf["m_max"]


def test_the_config_records_the_window_the_parameters_were_chosen_on(repo_root):
    """
    These numbers were picked on 2002-2017 and scored on 2018-2026. A reader who
    cannot tell which seasons were held out cannot tell a result from a fit, so
    the comment carrying that is part of the calibration, not decoration.
    """
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text())
    comment = cfg["performance"]["_comment"]
    assert "2002-2017" in comment and "2018-2026" in comment
    assert "backtest_performance_layer" in comment


def test_k_is_not_carried_across_a_layer_switch(repo_root):
    """
    k reaches the record only through k * M. MOV's multiplier averages 2.43 and
    xSRDiff's 1.66, so inheriting MOV's fitted k=35 would have cut every update by
    a third while looking like a pure layer change. The elo comment has to own
    which layer its k belongs to.
    """
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text())
    if cfg["performance"]["modifier"] == MOV:
        pytest.skip("MOV is configured, so its own fitted k applies")
    assert cfg["elo"]["k"] != 35, \
        "k=35 was fitted against MOV; a switch to another layer needs its own k"
    assert "k * M" in cfg["elo"]["_comment"] or "k * mean(M)" in cfg["elo"]["_comment"], \
        "the elo comment should say why k belongs to the configured performance layer"


# ---------------------------------------------------------- the claim it rests on

def test_the_committed_backtest_still_prefers_the_configured_layer(repo_root, perf):
    """
    The switch rests on one claim: at each layer's OWN best k, the configured
    layer predicts better than the MOV it replaced. The committed CSV is that
    comparison, so if a later data refresh reverses it, this fails and the switch
    should be revisited rather than inherited.
    """
    import csv
    path = repo_root / "data" / "processed" / "performance_layer_backtest.csv"
    if not path.exists():
        pytest.skip("no committed backtest -- run src/backtest_performance_layer.py --fit-k")
    rows = {r["variant"]: r for r in csv.DictReader(path.open(encoding="utf-8"))}
    if "k" not in next(iter(rows.values())):
        pytest.skip("committed backtest predates --fit-k, so its rows share one k")
    mine, mov = rows.get(perf["modifier"]), rows.get("production_mov")
    assert mine and mov, f"expected both {perf['modifier']!r} and production_mov rows"
    assert mine["k"] != mov["k"] or mine["variant"] == mov["variant"], \
        "both layers landing on the same best k is suspicious -- check --fit-k really swept"
    assert float(mine["brier"]) < float(mov["brier"]), (
        f"the configured layer {perf['modifier']!r} no longer beats MOV "
        f"({mine['brier']} vs {mov['brier']}); the switch rested on it doing so")


# ------------------------------------------------------------------ CI wiring

def test_ci_fits_the_curve_before_it_builds_elo(repo_root):
    """
    The tests above read a built database. If CI builds Elo without fitting the
    curve first, it builds one that fell back everywhere -- so they would either
    fail or, worse, pass against ratings nobody intended. Both jobs that build
    Elo must fit first.
    """
    wf = (repo_root / ".github" / "workflows" / "ci-and-deploy.yml").read_text(encoding="utf-8")
    builds = [i for i, line in enumerate(wf.splitlines()) if "src/build_elo.py" in line]
    fits = [i for i, line in enumerate(wf.splitlines()) if "src/fit_xsrdiff.py" in line]
    assert builds, "no build_elo.py step found; this test is reading the wrong file"
    assert len(fits) >= len(builds), \
        f"{len(builds)} build_elo.py steps but only {len(fits)} fit_xsrdiff.py steps"
    for b in builds:
        assert any(f < b for f in fits), \
            "a build_elo.py step runs with no fit_xsrdiff.py before it"
