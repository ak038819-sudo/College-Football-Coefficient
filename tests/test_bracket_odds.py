"""
The 24-team simulator's draw averaging (SIM).

NOTE: this file covers src/coefficients/simulate_bracket.py, the RETAINED
24-team simulator. It is no longer what the site runs -- the live model is
the real 12-team CFP bracket in simulate_cfp_bracket.py, whose odds need no
draw averaging because a 12-team bracket is fixed by the seeds. The two
tests at the bottom are the ones that check the LIVE export and page, and
they assert that the live payload makes no draw claim at all.

The rest is kept because the 24-team model is kept: it still runs, and the
bug it had is worth keeping fixed. That simulator used to draw the Round of
24 once and run all 10,000 brackets on it, so the published "Title Odds"
were conditional on an arbitrary draw seed while being presented as a
team's chances. The properties worth pinning are that the draw really does
vary between runs, that fixing it is still available and still means what
it used to, and that a single unlucky shuffle cannot take a run down.

test_bracket_draw.py covers whether a draw is VALID -- no same-conference
matchup, home field to the higher CoE, one seed one draw. This file covers
what the simulation does with draws, which is a different question; the one
overlap kept here is the same-conference check on the new rng-driven path,
because that path is new code rather than the seeded one already covered.

The end-to-end checks run against the real db/league.db and skip cleanly
without it, like the rest of the suite.
"""
from __future__ import annotations

import json
import random
import statistics

import pytest

import simulate_bracket as sb
from simulate_bracket import (DRAW_ATTEMPTS, build_field, draw_round_of_24, draw_with,
                              run_simulation)

SEASON = 2026
FAST_SIMS = 1500


@pytest.fixture(scope="module")
def field(db_path):
    byes, pot1, pot2, team_coe, conf_of = build_field(str(db_path), SEASON)
    if not pot1 or not pot2:
        pytest.skip(f"no playoff field for {SEASON}")
    return byes, pot1, pot2, team_coe, conf_of


def pairings(pairs):
    """A draw as an order-independent set, so a reshuffle of the same matchups
    is recognised as the same draw rather than a different one."""
    return frozenset(frozenset(p) for p in pairs)


# --------------------------------------------------------------------------
# The draw itself
# --------------------------------------------------------------------------

def test_different_seeds_give_different_draws(field):
    byes, pot1, pot2, _, conf_of = field
    drawn = {pairings(draw_round_of_24(byes, pot1, pot2, conf_of, s)) for s in range(8)}
    assert len(drawn) > 1, "if every seed drew the same bracket there would be nothing to average"


def test_drawing_from_an_rng_keeps_pulling_new_brackets(field):
    """
    The property the whole change rests on: consecutive draws off one rng must
    differ, or redrawing per run would be redrawing the same bracket.
    """
    byes, pot1, pot2, _, conf_of = field
    rng = random.Random(0)
    drawn = {pairings(draw_with(byes, pot1, pot2, conf_of, rng)) for _ in range(12)}
    assert len(drawn) > 1


def test_a_seeded_draw_is_just_a_draw_from_that_seeds_rng(field):
    byes, pot1, pot2, _, conf_of = field
    assert draw_round_of_24(byes, pot1, pot2, conf_of, 3) == \
        draw_with(byes, pot1, pot2, conf_of, random.Random(3))


def test_every_draw_pairs_each_team_once(field):
    byes, pot1, pot2, _, conf_of = field
    rng = random.Random(1)
    for _ in range(10):
        pairs = draw_with(byes, pot1, pot2, conf_of, rng)
        teams = [t for pair in pairs for t in pair]
        assert len(teams) == len(set(teams))
        assert set(teams) == set(pot1) | set(pot2)


def test_no_draw_pits_two_teams_from_the_same_conference(field):
    byes, pot1, pot2, _, conf_of = field
    rng = random.Random(2)
    for _ in range(10):
        for a, b in draw_with(byes, pot1, pot2, conf_of, rng):
            assert conf_of[a] != conf_of[b], (a, b, conf_of[a])


def test_one_unlucky_shuffle_does_not_end_a_run(field, monkeypatch):
    """
    A shuffle can fail the same-conference constraint by chance. That was
    survivable when a draw happened once per run of the program; it is not
    when it happens 10,000 times. The first failure must be retried.
    """
    byes, pot1, pot2, _, conf_of = field
    real = sb.backtrack_pairings
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        return None if calls["n"] <= 3 else real(*args, **kwargs)

    monkeypatch.setattr(sb, "backtrack_pairings", flaky)
    assert draw_with(byes, pot1, pot2, conf_of, random.Random(0))
    assert calls["n"] == 4


def test_a_field_that_cannot_be_paired_at_all_still_fails(field, monkeypatch):
    """Retrying must not turn an impossible field into an infinite loop or a
    silently empty draw."""
    byes, pot1, pot2, _, conf_of = field
    monkeypatch.setattr(sb, "backtrack_pairings", lambda *a, **k: None)
    with pytest.raises(SystemExit):
        draw_with(byes, pot1, pot2, conf_of, random.Random(0))
    assert DRAW_ATTEMPTS > 1


# --------------------------------------------------------------------------
# What the odds mean
# --------------------------------------------------------------------------

def test_fixing_the_draw_reproduces_the_same_odds(db_path):
    """--fixed-draw is still deterministic: the old question, still answerable."""
    a = run_simulation(str(db_path), SEASON, 1, FAST_SIMS, 4.5, sim_seed=0, fixed_draw=True)[0]
    b = run_simulation(str(db_path), SEASON, 1, FAST_SIMS, 4.5, sim_seed=0, fixed_draw=True)[0]
    assert {t: dict(c) for t, c in a.items()} == {t: dict(c) for t, c in b.items()}


def test_the_draw_seed_no_longer_decides_a_teams_chances(db_path):
    """
    The bug, stated as a test: once the odds average over the draw, the draw
    seed must stop being a source of variation.

    Measured against the simulation's OWN noise, which is the only honest
    yardstick here. Changing the draw seed on a redrawn run only changes which
    brackets get sampled, so its effect should be no larger than changing the
    game seed -- and if the simulator went back to one draw per run, the draw
    seed would decide the answer again and the spread would leave that noise
    far behind.

    The earlier form of this test compared one pair of draw seeds, fixed draw
    against redrawn, and asserted the redrawn pair was closer together. That
    holds only while the field is draw-sensitive enough for the effect to clear
    the noise of a single pair, and it failed on main on 2026-10-03 with 1.67
    against 1.27 -- two numbers that are both noise. A spread across six seeds,
    expressed as a ratio to the noise floor, is the same claim without the
    dependence on which week's field happens to be lopsided.
    """
    def champion_pct(draw_seed, fixed, sim_seed=0):
        counts, n, _, _ = run_simulation(str(db_path), SEASON, draw_seed, FAST_SIMS, 4.5,
                                         sim_seed=sim_seed, fixed_draw=fixed)
        return {t: 100 * c["champion"] / n for t, c in counts.items()}

    def dispersion(runs, contenders):
        """Mean over contenders of a team's standard deviation across runs."""
        return statistics.fmean(
            statistics.pstdev([run.get(t, 0) for run in runs]) for t in contenders)

    base = champion_pct(1, False)
    contenders = [t for t in base if base[t] > 5.0]
    assert contenders, "no contender above 5% -- the field or the model has changed shape"

    seeds = [1, 2, 3, 4, 5, 6]
    free = [champion_pct(s, False) for s in seeds]            # the DRAW seed varies
    noise = [champion_pct(1, False, sim_seed=s) for s in seeds]  # the GAME seed varies
    fixed = [champion_pct(s, True) for s in seeds]            # one draw each, as it was

    free_sd = dispersion(free, contenders)
    noise_sd = dispersion(noise, contenders)
    fixed_sd = dispersion(fixed, contenders)

    # It still samples DIFFERENT brackets per seed -- a draw rng that ignored
    # the seed would pass the ratio below while quietly drawing one sequence.
    assert free_sd > 0, "changing the draw seed changed nothing at all"

    # The draw seed is not a source of variation beyond the simulation's own.
    # Measured over five disjoint blocks of six seeds on the 2026 field, this
    # ratio ran 0.68 to 0.87 while a fixed draw ran 3.3 to 4.5.
    assert free_sd <= 1.5 * noise_sd, (
        f"the draw seed still moves the odds: {free_sd:.2f} against a noise floor "
        f"of {noise_sd:.2f}")

    # And where this week's field IS draw-sensitive, fixing the draw is visibly
    # worse. Guarded, because a lopsided field can have little to say here and
    # that is a fact about the season, not a regression.
    if fixed_sd > 2 * noise_sd:
        assert free_sd < fixed_sd


def test_every_teams_odds_still_add_up(db_path):
    counts, n, _, _ = run_simulation(str(db_path), SEASON, 1, FAST_SIMS, 4.5)
    total = sum(c["champion"] for c in counts.values())
    assert total == n, "exactly one champion per simulated bracket"
    for team, c in counts.items():
        # Reaching a later round implies having reached every earlier one.
        assert c["champion"] <= c["final"] <= c["sf"] <= c["qf"] <= c["r16"], team


def test_the_live_export_ships_a_twelve_team_bracket_with_no_draw(db_path):
    """
    The live payload must not carry a draw claim in either direction.

    A `redraws` flag is now meaningless: there is no draw in a 12-team
    bracket. Leaving it at True would have the page tell the reader its odds
    average over brackets that were never drawn, and setting it False would
    have the page say the opposite about a draw that does not exist. It has to
    be absent.

    This builds a payload from the exporter rather than reading the committed
    ui/dashboard_data.json. That file is regenerated by the deploy, so a test
    that asserts on its contents fails for everyone between a source change
    and the next rebuild -- which is how a test coupled to generated data has
    blocked publishing in this project three times now.
    """
    import export_dashboard_data

    payload = export_dashboard_data.build_playoff_data(
        str(db_path), SEASON, draw_seed=1, sims=50, temperature=4.5, home_field=1.25)

    assert "redraws" not in payload["sim_meta"]
    assert payload["format"] == {"field_size": 12, "auto_bids": 5,
                                 "bye_seeds": 4, "seeding": "straight"}
    assert len(payload["field"]) == 12
    assert len(payload["byes"]) == 4
    assert len(payload["first_round"]) == 4
    assert len(payload["quarterfinals"]) == 4
    assert len(payload["simulation"]) == 12
    # The 24-team model's keys must be gone, not merely unused: a page reading
    # a stale key would render the old bracket from new data.
    for gone in ("round_of_24", "qualifiers", "conference_ranking",
                 "independent_replacements"):
        assert gone not in payload, gone


def test_the_page_no_longer_tells_the_reader_to_discount_a_draw(repo_root):
    """
    The old page carried a paragraph explaining that its odds were not the odds
    of the bracket beside them. That paragraph is now false, and a page that
    keeps apologising for a limitation it no longer has is as wrong as one that
    hides a limitation it does.
    """
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert "meta.redraws" not in shell
    assert "before the draw is known" not in shell
    # And it says the opposite, which is the thing that is now true.
    assert "fully determined by the seeds" in shell
