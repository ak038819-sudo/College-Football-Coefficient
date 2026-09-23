"""
The Round-of-24 draw must never produce a same-conference matchup, for
any draw seed. This is checked across many seeds per year rather than
just one, since a pairing bug could easily slip through on a single
lucky seed.
"""
import pytest
import random

from conftest import PLAYOFF_YEARS
from select_playoff_field_v2 import (
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from draw_playoff_bracket_v2 import backtrack_pairings, choose_home_away, build_conf_map

DRAW_SEEDS_TO_TRY = list(range(1, 21))  # 20 different seeds per year


def _build_pots(db_conn, year):
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conf_ranked = load_conference_coe_rank(db_conn, year)
    team_coe = load_team_coe_5yr(year)
    qualifiers = select_qualifiers(db_conn, year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)
    independents = get_independent_teams_with_coe(db_conn, year, team_coe)
    qualifiers, _ = apply_independent_threshold(qualifiers, independents)

    pot1 = [q["team_name"] for q in qualifiers if q["pot"] == 1]
    pot2 = [q["team_name"] for q in qualifiers if q["pot"] == 2]
    conf_of = build_conf_map(qualifiers)
    return pot1, pot2, conf_of, team_coe


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_no_same_conference_matchup_across_many_seeds(db_conn, year):
    pot1, pot2, conf_of, team_coe = _build_pots(db_conn, year)

    for seed in DRAW_SEEDS_TO_TRY:
        rng = random.Random(seed)
        p1, p2 = pot1[:], pot2[:]
        rng.shuffle(p1)
        rng.shuffle(p2)
        pairs = backtrack_pairings(p1, p2, conf_of, p2[:])
        assert pairs is not None, f"{year} seed={seed}: no valid pairing found at all"
        for a, b in pairs:
            assert conf_of[a] != conf_of[b], (
                f"{year} seed={seed}: same-conference matchup {a} vs {b} ({conf_of[a]})"
            )


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_home_field_always_goes_to_higher_coe(db_conn, year):
    pot1, pot2, conf_of, team_coe = _build_pots(db_conn, year)
    rng = random.Random(1)
    p1, p2 = pot1[:], pot2[:]
    rng.shuffle(p1)
    rng.shuffle(p2)
    pairs = backtrack_pairings(p1, p2, conf_of, p2[:])
    assert pairs is not None

    for a, b in pairs:
        home, away = choose_home_away(a, b, team_coe)
        home_coe = team_coe.get(home, 0.0)
        away_coe = team_coe.get(away, 0.0)
        assert home_coe >= away_coe, (
            f"{year}: {away} (CoE {away_coe}) got home field over {home} (CoE {home_coe})"
        )


def test_same_seed_reproduces_identical_draw(db_conn):
    pot1, pot2, conf_of, team_coe = _build_pots(db_conn, PLAYOFF_YEARS[-1])

    def draw(seed):
        rng = random.Random(seed)
        p1, p2 = pot1[:], pot2[:]
        rng.shuffle(p1)
        rng.shuffle(p2)
        return backtrack_pairings(p1, p2, conf_of, p2[:])

    assert draw(42) == draw(42)
