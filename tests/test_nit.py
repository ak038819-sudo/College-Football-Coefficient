"""
Structural invariants for the NIT field, mirroring test_playoff_field.py's
approach for the main field. These caught two real bugs during
development: a shortfall when the independent threshold displaced a
team the NIT selection didn't know to look for, and same-conference
matchups from a naive fixed 1v16 seeding.
"""
import pytest

from conftest import PLAYOFF_YEARS
from select_playoff_field_v2 import (
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from select_nit_field import select_nit_qualifiers, seed_nit_bracket


def _build_main_and_nit(db_conn, year):
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conf_ranked = load_conference_coe_rank(db_conn, year)
    team_coe = load_team_coe_5yr(year)

    main_q = select_qualifiers(db_conn, year, conf_ranked, bid_table)
    main_q = assign_pots(main_q)
    assign_homefield(main_q, team_coe)
    main_indep = get_independent_teams_with_coe(db_conn, year, team_coe)
    main_q, _ = apply_independent_threshold(main_q, main_indep)
    already = {q["team_name"] for q in main_q}

    nit_q = select_nit_qualifiers(db_conn, year, conf_ranked, bid_table, already)
    assign_homefield(nit_q, team_coe)
    leftover_indep = [(n, c) for n, c in main_indep if n not in already]
    nit_q, _ = apply_independent_threshold(nit_q, leftover_indep)

    return already, nit_q


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_nit_field_is_16(db_conn, year):
    _, nit_q = _build_main_and_nit(db_conn, year)
    assert len(nit_q) == 16, f"{year}: expected 16 NIT qualifiers, got {len(nit_q)}"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_nit_never_overlaps_main_field(db_conn, year):
    """
    Regression test: an independent displacing a conference's own
    at-large qualifier (e.g. Notre Dame bumping Colorado out of a Big
    12 slot in 2024) must not cause that displaced team to be silently
    skipped by a static rank-threshold -- it should surface as a real
    NIT candidate. This test catches both a missing team AND an
    accidental double-booking.
    """
    already, nit_q = _build_main_and_nit(db_conn, year)
    nit_names = {q["team_name"] for q in nit_q}
    overlap = already & nit_names
    assert not overlap, f"{year}: teams in both main field and NIT: {overlap}"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_nit_bracket_no_same_conference_matchup(db_conn, year):
    _, nit_q = _build_main_and_nit(db_conn, year)
    _, pairs = seed_nit_bracket(nit_q)
    for a, b in pairs:
        assert a["conference"] != b["conference"], (
            f"{year}: same-conference NIT matchup {a['team_name']} vs {b['team_name']} ({a['conference']})"
        )


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_nit_higher_seed_hosts(db_conn, year):
    _, nit_q = _build_main_and_nit(db_conn, year)
    _, pairs = seed_nit_bracket(nit_q)
    for home, away in pairs:
        assert home["seed"] < away["seed"], (
            f"{year}: NIT game has lower seed ({away['seed']}) hosting higher seed ({home['seed']})"
        )
