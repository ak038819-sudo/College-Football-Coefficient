"""
The fitted bracket temperature (SIM).

Two different things need pinning, and they fail for different reasons:

  * the fit itself -- that the search really does what the docstring claims,
    in particular that it never lets a season's own result into the predictor
    for that season, which is the one mistake that would make the whole
    exercise worthless while still producing a confident-looking number;
  * the wiring -- that the fitted value actually reaches both simulators, the
    Python one and the button on the page, and that they cannot drift apart.

The numeric checks are stated as relationships and bounds rather than the
exact fitted constants: the point is that the value is fitted and sane, not
that it is frozen forever at 4.5.
"""
from __future__ import annotations

import json
import math
import sqlite3

import pytest

import calibrate_sim_temperature as cal
import simulate_bracket


# --------------------------------------------------------------------------
# The probability model
# --------------------------------------------------------------------------

def test_an_even_matchup_is_a_coin_flip():
    (p, _), = cal.probabilities([(0.0, 0.0, 1.0)], 4.5, 0.0)
    assert p == pytest.approx(0.5)


def test_the_favourite_is_favoured_and_more_so_at_a_lower_temperature():
    gap = [(5.0, 0.0, 1.0)]
    sharp, = cal.probabilities(gap, 3.0, 0.0)
    flat, = cal.probabilities(gap, 9.0, 0.0)
    assert 0.5 < flat[0] < sharp[0] < 1.0


def test_home_field_only_applies_where_there_was_a_home_team():
    at_home, = cal.probabilities([(0.0, 1.0, 1.0)], 4.5, 1.25)
    neutral, = cal.probabilities([(0.0, 0.0, 1.0)], 4.5, 1.25)
    assert at_home[0] > 0.5
    assert neutral[0] == pytest.approx(0.5)


def test_the_python_and_the_fit_agree_on_the_formula():
    """
    simulate_bracket.win_probability and the one being fitted must be the same
    function, or the fitted number means nothing where it is used.
    """
    for gap in (-12.0, -3.0, 0.0, 1.5, 7.0):
        (fitted, _), = cal.probabilities([(gap, 0.0, 1.0)], 4.5, 0.0)
        assert simulate_bracket.win_probability(gap, 0.0, 4.5) == pytest.approx(fitted)


# --------------------------------------------------------------------------
# The fit cannot see the season it is predicting
# --------------------------------------------------------------------------

def test_the_predictor_is_the_window_that_closed_before_the_season():
    """
    The load-bearing property. end_year = Y spans [Y-4 .. Y], so using it to
    predict season Y's games would be scoring the model on its own input. Two
    seasons are given deliberately different ratings for the same team, and the
    gap that comes out must be the EARLIER one.
    """
    coe = {2019: {"A": 10.0, "B": 4.0}, 2020: {"A": 0.0, "B": 99.0}}
    games = [(2020, "A", "B", 1.0, 1.0)]
    assert cal.gaps_for(games, coe) == [(6.0, 1.0, 1.0)]


def test_a_team_with_no_prior_window_is_dropped_rather_than_called_average():
    coe = {2019: {"A": 10.0}}
    assert cal.gaps_for([(2020, "A", "B", 1.0, 1.0)], coe) == []
    assert cal.gaps_for([(2020, "B", "A", 1.0, 0.0)], coe) == []


def test_a_season_before_any_window_exists_is_dropped():
    assert cal.gaps_for([(1980, "A", "B", 1.0, 1.0)], {1984: {"A": 1.0, "B": 2.0}}) == []


def test_the_playoff_calibre_filter_needs_both_teams_and_uses_the_prior_window():
    coe = {2019: {"A": 10.0, "B": 9.0, "C": 1.0}}
    games = [(2020, "A", "B", 1.0, 1.0), (2020, "A", "C", 1.0, 1.0)]
    eligible = cal.top_n_by_season(coe, 2)          # A and B, by the 2019 window
    assert len(cal.gaps_for(games, coe, eligible)) == 1
    assert len(cal.gaps_for(games, coe)) == 2


def test_top_n_ranks_by_rating_not_by_name():
    coe = {2019: {"aardvark": 1.0, "zebra": 9.0, "middle": 5.0}}
    assert cal.top_n_by_season(coe, 2)[2019] == {"zebra", "middle"}


# --------------------------------------------------------------------------
# The search
# --------------------------------------------------------------------------

def test_the_search_ranks_by_brier_best_first():
    gaps = [(6.0, 0.0, 1.0), (-6.0, 0.0, 0.0), (2.0, 0.0, 1.0)]
    rows = cal.search(gaps, [1.0, 4.0, 20.0], [0.0])
    assert rows == sorted(rows, key=lambda r: r["brier"])
    # Every combination searched is reported, so a near-tie stays visible.
    assert len(rows) == 3


def test_the_search_recovers_a_temperature_it_was_given():
    """
    Against outcomes generated from a known temperature, the fit should land
    near it. Without this, the search could be ranking noise and nobody would
    know from the real data alone.
    """
    truth = 4.0
    gaps = []
    for i in range(-80, 81):
        gap = i / 8.0
        p = 1.0 / (1.0 + math.exp(-gap / truth))
        # 200 games at each gap, split exactly in the true proportion, so the
        # sample carries the model rather than a random draw from it.
        wins = round(200 * p)
        gaps += [(gap, 0.0, 1.0)] * wins + [(gap, 0.0, 0.0)] * (200 - wins)
    best = cal.search(gaps, [1.0, 2.0, 3.0, 3.5, 4.0, 4.5, 5.0, 6.0, 8.0], [0.0])[0]
    assert best["temperature"] == pytest.approx(truth, abs=0.5)


def test_refining_brackets_the_coarse_winner_and_never_goes_negative():
    temperatures, home_fields = cal.refine(0.5, 0.0)
    assert all(t > 0 for t in temperatures)
    assert all(h >= 0 for h in home_fields)
    assert 0.5 in temperatures and 0.0 in home_fields


# --------------------------------------------------------------------------
# The wiring: one temperature, reaching both simulators
# --------------------------------------------------------------------------

def test_the_config_carries_a_fitted_temperature(repo_root):
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text())
    sim = cfg["simulation"]
    # Sane rather than exact: a temperature outside this range would mean
    # something has gone wrong in the fit, not that it has been re-tuned.
    assert 1.0 < sim["temperature"] < 12.0
    assert sim["temperature"] != 6.0, "6.0 was the unfitted placeholder"
    assert 0.0 < sim["home_field"] < 5.0
    assert "calibrate_sim_temperature" in sim["_comment"], \
        "the config should say where the number came from"


def test_the_python_simulator_uses_the_configured_temperature(repo_root):
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text())
    assert simulate_bracket.DEFAULT_TEMPERATURE == cfg["simulation"]["temperature"]


def test_the_page_takes_its_temperature_from_the_payload_not_a_constant(repo_root):
    """
    The button on the bracket page ran its own copy of the model with its own
    hardcoded 6.0, so it disagreed with the Title Odds table beside it. It must
    now read the shipped value, and must not carry a fallback that would
    quietly restore the old number.
    """
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert "temperature = temperature || 6.0" not in shell
    assert "const temperature = (pf.sim_meta || {}).temperature;" in shell
    # The venue argument is checked by test_only_the_round_of_24_gets_a_venue_term;
    # what matters here is that the temperature still comes from the payload.
    assert "simulateGame(finalField[0], finalField[1], teamCoe, temperature, 0)" in shell


def test_no_build_layer_hardcodes_its_own_temperature(repo_root):
    """
    build_dashboard.py used to pass --temperature 6.0 down to the exporter, so
    the fitted value in the config was overridden on the way to the page and
    the site kept publishing odds from the old number. Nothing above the config
    may carry a default of its own.
    """
    build = (repo_root / "build_dashboard.py").read_text(encoding="utf-8")
    assert '"--temperature", type=float, default=None' in build
    assert "default=6.0" not in build


def test_a_neutral_site_game_is_exactly_symmetric():
    """
    With no venue term, swapping the two teams must give exactly the
    complementary probability, with no residual edge to whichever side is named
    first. This is the property every round from the Round of 16 on relies on,
    and it is also what makes the home-field term below visible rather than
    confounded with a naming bias.
    """
    for gap in (0.0, 1.5, 7.0, -4.0):
        forward = simulate_bracket.win_probability(gap, 0.0, 4.5)
        reverse = simulate_bracket.win_probability(0.0, gap, 4.5)
        assert forward + reverse == pytest.approx(1.0)
    assert simulate_bracket.win_probability(0.0, 0.0, 4.5) == pytest.approx(0.5)


def test_home_field_helps_the_host_and_is_antisymmetric():
    """
    The host's edge must go to whichever side is actually at home, so passing it
    negated has to give exactly the mirror image. A term that helped team A
    regardless of who hosted would be a naming bias wearing a home-field label.
    """
    home = simulate_bracket.win_probability(0.0, 0.0, 4.5, 1.25)
    away = simulate_bracket.win_probability(0.0, 0.0, 4.5, -1.25)
    assert home > 0.5 > away
    assert home + away == pytest.approx(1.0)


def test_home_field_is_worth_less_than_the_gap_it_is_measured_in():
    """
    1.25 CoE points is a real but modest edge: it must not turn a clear underdog
    into a favourite. A term large enough to do that would be a sign the fit had
    absorbed something else, so the bound is worth pinning.
    """
    underdog_at_home = simulate_bracket.win_probability(-5.0, 0.0, 4.5, 1.25)
    assert underdog_at_home < 0.5


def test_the_python_simulator_uses_the_configured_home_field(repo_root):
    cfg = json.loads((repo_root / "config" / "model_config.json").read_text())
    assert simulate_bracket.DEFAULT_HOME_FIELD == cfg["simulation"]["home_field"]


def test_host_must_be_one_of_the_two_teams():
    """
    simulate_game takes the host by name, so a caller that passes a team from
    another game would otherwise silently play a neutral-site game while looking
    like it applied home field.
    """
    import random as _random
    with pytest.raises(ValueError):
        simulate_bracket.simulate_game("A", "B", {"A": 1.0, "B": 0.0}, 4.5,
                                       _random.Random(0), 1.25, "C")


def test_the_host_named_is_the_team_helped():
    """
    The decision that turns a host's NAME into a signed edge is its own step, and
    testing win_probability's arithmetic does not reach it: a simulate_game that
    added the edge to whichever team was listed first would pass every algebraic
    check while handing home field to the wrong team half the time.

    So this drives simulate_game itself. Two equal teams, a fixed coin at exactly
    0.5: the host's probability is above 0.5 and the visitor's below, so the
    winner must be whoever is at home, whichever argument position they occupy.
    """
    class FixedCoin:
        def random(self):
            return 0.5

    coe = {"A": 5.0, "B": 5.0}
    for host in ("A", "B"):
        for a, b in (("A", "B"), ("B", "A")):
            won = simulate_bracket.simulate_game(a, b, coe, 4.5, FixedCoin(), 1.25, host)
            assert won == host, f"{a} vs {b} at {host}'s place was won by {won}"
    # And with no host at all the same coin sits exactly on the boundary, so the
    # second-named team takes it -- the neutral case is genuinely neutral.
    assert simulate_bracket.simulate_game("A", "B", coe, 4.5, FixedCoin(), 1.25, None) == "B"


def test_no_round_after_the_round_of_24_feels_home_field():
    """
    Behavioural, not textual. An earlier version of this test asserted that the
    source contained exactly one call passing a host, which a one-word edit
    defeated while leaving the leak in place.

    Instead: a bracket whose sixteen Round-of-16 slots are all byes has no
    Round-of-24 game at all, so home field has nothing legitimate to act on.
    Every later round is a neutral site, so the whole simulation must give
    bit-identical results no matter what home field is set to. A leak into any
    later round would change who wins, since the term is deliberately made large
    here.
    """
    import random

    teams = [f"T{i:02d}" for i in range(16)]
    coe = {t: 10.0 - i * 0.4 for i, t in enumerate(teams)}
    seed_order = [{"fixed_team": t, "pair": None} for t in teams]

    for seed in range(50):
        quiet = simulate_bracket.simulate_one_bracket(
            teams, [], seed_order, coe, 4.5, random.Random(seed), 0.0)
        loud = simulate_bracket.simulate_one_bracket(
            teams, [], seed_order, coe, 4.5, random.Random(seed), 8.0)
        assert quiet == loud, (
            f"seed {seed}: home field changed a bracket with no home games, so it is "
            "reaching a neutral-site round")


def test_the_round_of_24_boost_goes_to_the_higher_coe_team():
    """
    Who hosts is not a detail the simulation may get backwards. The bracket
    graphic prints "away @ home" from choose_home_away, and the simulation has to
    agree with it, or the published bracket shows one venue while the odds assume
    the other.

    Driven through simulate_one_bracket rather than asserted from the source: one
    Round-of-24 pair, the rest byes, and the higher-CoE team's rate of reaching
    the Round of 16 must RISE when home field is turned up. Handing the boost to
    the visitor instead would push it down.
    """
    import random

    strong, weak = "Strong", "Weak"
    byes = [f"Bye{i:02d}" for i in range(15)]
    coe = {strong: 10.0, weak: 7.0}
    for i, b in enumerate(byes):
        coe[b] = 9.0 - i * 0.3
    seed_order = ([{"fixed_team": None, "pair": (weak, strong)}]
                  + [{"fixed_team": b, "pair": None} for b in byes])

    def advance_rate(home_field):
        got = 0
        for seed in range(400):
            result = simulate_bracket.simulate_one_bracket(
                byes, [], seed_order, coe, 4.5, random.Random(seed), home_field)
            if simulate_bracket.ROUND_RANK[result[strong]] >= simulate_bracket.ROUND_RANK["r16"]:
                got += 1
        return got / 400

    quiet, loud = advance_rate(0.0), advance_rate(6.0)
    assert loud > quiet + 0.05, (
        f"the higher-CoE team advanced {quiet:.1%} with no home field and {loud:.1%} with a "
        "large one; the boost is not reaching the host")


def test_the_page_keeps_home_field_to_the_round_of_24(repo_root):
    """
    The page runs its own copy of the model and cannot be driven from here, so it
    is read instead: the Round of 24 passes the shipped value and each later
    round passes an explicit 0, rather than inheriting whatever the last call
    used.
    """
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert "simulateGame(game.home, game.away, teamCoe, temperature, homeField)" in shell
    for later in ("qfField.push(simulateGame(r16Field[i], r16Field[i + 1], teamCoe, temperature, 0))",
                  "sfField.push(simulateGame(qfField[i], qfField[i + 1], teamCoe, temperature, 0))",
                  "finalField.push(simulateGame(sfField[i], sfField[i + 1], teamCoe, temperature, 0))",
                  "simulateGame(finalField[0], finalField[1], teamCoe, temperature, 0)"):
        assert later in shell, f"a later round does not state its neutral venue: {later}"


def test_the_page_takes_its_home_field_from_the_payload_not_a_constant(repo_root):
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert "const homeField = (pf.sim_meta || {}).home_field;" in shell
    assert "1.25" not in shell.split("function winProbability")[1].split("function runBracketSimulation")[0]


# --------------------------------------------------------------------------
# Against the real record
# --------------------------------------------------------------------------

def test_the_fitted_temperature_beats_the_old_guess_on_real_games(db_path, repo_root):
    """
    The claim the change rests on: on the games a bracket actually resembles,
    the fitted value predicts better than 6.0 did. If this ever stops being
    true, the default should not stay where it is.
    """
    coe = cal.load_coe_by_end_year()
    conn = sqlite3.connect(str(db_path))
    games = cal.load_games(conn)
    conn.close()
    gaps = cal.gaps_for(games, coe, cal.top_n_by_season(coe, cal.DEFAULT_TOP_N))
    if len(gaps) < 500:
        pytest.skip("not enough rated games -- run the pipeline and build_coefficients.py")

    fitted = json.loads((repo_root / "config" / "model_config.json").read_text())["simulation"]
    now = cal.score(gaps, fitted["temperature"], fitted["home_field"])
    before = cal.score(gaps, cal.INCUMBENT_TEMPERATURE, 0.0)
    assert now["brier"] < before["brier"]
    # The real gain is calibration: 6.0 was underconfident, not just off.
    assert now["calibration_error"] < before["calibration_error"] / 2
