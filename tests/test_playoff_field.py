"""
Structural invariants of the 24-team playoff field that must hold for
EVERY season, not just the ones we happened to eyeball. These are
regression tests for two real bugs found in this project: unequal pot
sizes (was 7 vs 9, which breaks a 1-to-1 draw pairing) and a seeding
rule that could produce 7 byes in one ruleset, contradicting the fixed
"top-8 get byes" requirement.
"""
import pytest

from conftest import PLAYOFF_YEARS
from select_playoff_field_v2 import (
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield,
    get_independent_teams_with_coe, apply_independent_threshold,
)


def _build_field(db_conn, year):
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    conf_ranked = load_conference_coe_rank(db_conn, year)
    team_coe = load_team_coe_5yr(year)
    qualifiers = select_qualifiers(db_conn, year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)
    independents = get_independent_teams_with_coe(db_conn, year, team_coe)
    qualifiers, replacements = apply_independent_threshold(qualifiers, independents)
    return qualifiers, replacements


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_exactly_8_byes(db_conn, year):
    qualifiers, _ = _build_field(db_conn, year)
    byes = [q for q in qualifiers if q["pot"] == "bye"]
    assert len(byes) == 8, f"{year}: expected exactly 8 byes (rule 8), got {len(byes)}"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_pot1_and_pot2_are_equal_size(db_conn, year):
    qualifiers, _ = _build_field(db_conn, year)
    pot1 = [q for q in qualifiers if q["pot"] == 1]
    pot2 = [q for q in qualifiers if q["pot"] == 2]
    assert len(pot1) == len(pot2), (
        f"{year}: Pot1 ({len(pot1)}) and Pot2 ({len(pot2)}) are unequal -- "
        "a 1-to-1 draw pairing is impossible"
    )
    assert len(pot1) == 8, f"{year}: expected 8 teams per pot, got {len(pot1)}"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_total_field_is_24(db_conn, year):
    qualifiers, _ = _build_field(db_conn, year)
    assert len(qualifiers) == 24, f"{year}: expected 24 total qualifiers, got {len(qualifiers)}"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_independent_threshold_never_displaces_a_champion_on_real_data(db_conn, year):
    """Sanity check on real data: every replacement's target must have been an at-large qualifier."""
    conf_ranked = load_conference_coe_rank(db_conn, year)
    bid_table = YEAR1_BIDS if year == 2014 else YEAR2_BIDS
    team_coe = load_team_coe_5yr(year)
    qualifiers_before = select_qualifiers(db_conn, year, conf_ranked, bid_table)
    qualifiers_before = assign_pots(qualifiers_before)
    assign_homefield(qualifiers_before, team_coe)
    champions_before = {q["team_name"] for q in qualifiers_before if q["bid_type"] == "champion"}

    independents = get_independent_teams_with_coe(db_conn, year, team_coe)
    _, replacements = apply_independent_threshold(list(qualifiers_before), independents)

    for r in replacements:
        assert r["replaced_team"] not in champions_before, (
            f"{year}: independent threshold displaced {r['replaced_team']}, "
            f"which was a conference champion -- violates rule 7"
        )


def test_independent_threshold_refuses_to_displace_champion_even_when_far_stronger():
    """
    Direct unit test: even if an independent's CoE is astronomically
    higher than a conference champion's, apply_independent_threshold must
    still refuse to touch the champion -- it may only ever remove an
    'at_large' qualifier. This is checked with synthetic data specifically
    engineered to tempt the function into breaking rule 7, since real data
    may never naturally produce a case extreme enough to expose the bug.
    """
    qualifiers = [
        {"team_name": "WeakChamp", "conference": "TinyConf", "conf_coe_rank": 10,
         "conf_standing_rank": 1, "bid_type": "champion", "pot": "bye", "team_coe_5yr": 1.0},
        {"team_name": "MidAtLarge", "conference": "BigConf", "conf_coe_rank": 1,
         "conf_standing_rank": 4, "bid_type": "at_large", "pot": 2, "team_coe_5yr": 3.0},
    ]
    # Independent CoE (100.0) dwarfs BOTH the champion (1.0) and the at-large team (3.0)
    independents = [("SuperIndependent", 100.0)]

    updated, replacements = apply_independent_threshold(qualifiers, independents)

    champ_still_present = any(q["team_name"] == "WeakChamp" for q in updated)
    assert champ_still_present, "A conference champion was displaced -- violates rule 7"
    assert any(r["replaced_team"] == "MidAtLarge" for r in replacements), (
        "Expected the independent to displace the at-large qualifier instead"
    )
