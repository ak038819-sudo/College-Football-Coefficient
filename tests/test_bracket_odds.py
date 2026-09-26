"""
Title odds average over the draw, not over one draw (SIM).

The simulator used to draw the Round of 24 once and run all 10,000 brackets
on it, so the published "Title Odds" were conditional on an arbitrary draw
seed while being presented as a team's chances. The properties worth pinning
are that the draw really does vary between runs, that fixing it is still
available and still means what it used to, and that a single unlucky shuffle
cannot take a run down.

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
    The bug, stated as a test. With the draw fixed, changing only the seed moves
    the odds a lot; averaged over draws it must not, because the draw is no
    longer part of the answer.
    """
    def champion_pct(seed, fixed):
        counts, n, _, _ = run_simulation(str(db_path), SEASON, seed, FAST_SIMS, 4.5,
                                         sim_seed=0, fixed_draw=fixed)
        top = max(counts.items(), key=lambda kv: kv[1]["champion"])[0]
        return top, {t: 100 * c["champion"] / n for t, c in counts.items()}

    _, fixed_a = champion_pct(1, True)
    _, fixed_b = champion_pct(5, True)
    _, free_a = champion_pct(1, False)
    _, free_b = champion_pct(5, False)

    contenders = [t for t in free_a if free_a[t] > 5.0]
    assert contenders, "no contender above 5% -- the field or the model has changed shape"
    fixed_spread = max(abs(fixed_a.get(t, 0) - fixed_b.get(t, 0)) for t in contenders)
    free_spread = max(abs(free_a[t] - free_b[t]) for t in contenders)
    assert free_spread < fixed_spread


def test_every_teams_odds_still_add_up(db_path):
    counts, n, _, _ = run_simulation(str(db_path), SEASON, 1, FAST_SIMS, 4.5)
    total = sum(c["champion"] for c in counts.values())
    assert total == n, "exactly one champion per simulated bracket"
    for team, c in counts.items():
        # Reaching a later round implies having reached every earlier one.
        assert c["champion"] <= c["final"] <= c["sf"] <= c["qf"] <= c["r16"], team


def test_the_export_says_the_odds_are_draw_averaged(repo_root):
    """
    The page explains itself from this flag. Shipping draw-averaged odds while
    the payload still claims one bracket would be the same error in a new place.
    """
    data = json.loads((repo_root / "ui" / "dashboard_data.json").read_text())
    for year, payload in data.get("playoff_by_year", {}).items():
        assert payload["sim_meta"].get("redraws") is True, year
    # The committed file above can only go stale; the exporter that writes it is
    # what has to keep saying so on the next rebuild.
    exporter = (repo_root / "src" / "export_dashboard_data.py").read_text(encoding="utf-8")
    assert '"redraws": True' in exporter


def test_the_page_tells_the_reader_the_bracket_shown_is_not_the_odds(repo_root):
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert "meta.redraws" in shell
    assert "before the draw is known" in shell
