#!/usr/bin/env python3
"""
Selects the 16-team NIT field: teams that missed the main 24-team
playoff, using the same conference-CoE-rank-driven bid structure as
the main field (see select_playoff_field_v2.py), but built from each
conference's NEXT-best team(s) beyond their playoff cutoff.

Project decision (rule 11: "similar seeding and qualification rules
to the Playoff", exact numbers not specified in the original rules):
  - Field size: 16 (clean power of 2, no byes/pots needed)
  - Bid allocation: ranks 1-6 get 2 bids each (their two next-best
    teams past the playoff cutoff) = 12; ranks 7-10 get 1 bid each
    (their runner-up, since only the champion went to the playoff) = 4
  - Every NIT qualifier is inherently an "at_large" type -- no
    conference sends its CHAMPION to the NIT (every conference sends
    its champion to the main playoff at minimum), so the independent
    threshold can freely compare against any NIT slot without a
    champion-protection carve-out.
  - Same cascading bid-carry as the main field if a conference is too
    small to fill its NIT allocation (reusing the same principle as
    select_qualifiers's tier_pot_template carry, just without a pot
    destiny attached since NIT seeding is a plain 1-16 CoE seed, not
    a pot draw).
  - Seeding: teams ranked 1-16 by 5yr rolling team CoE, standard
    bracket pairing (1v16, 2v15, ... 8v9) -- deterministic, no random
    draw (unlike the main playoff's Round of 24).
  - Independents not already pulled into the main field via its own
    threshold check are eligible here too, using the same mechanic.

Does NOT (yet) implement the "NIT winner gains an additional bid for
their conference" carryover into the following year's main playoff --
that's a deliberate, separate follow-up (needs threading a prior-year
NIT champion into the main bid table).

Usage:
    python src/coefficients/select_nit_field.py --year 2025
"""
from __future__ import annotations

import argparse
import sqlite3

from select_playoff_field_v2 import (
    YEAR1_BIDS, YEAR2_BIDS, load_conference_coe_rank, load_team_coe_5yr,
    select_qualifiers, assign_pots, assign_homefield, bid_count_for_rank,
    get_independent_teams_with_coe, apply_independent_threshold,
)
from draw_playoff_bracket_v2 import backtrack_pairings


def nit_bids_for_rank(coe_rank: int) -> int:
    return 2 if coe_rank <= 6 else 1


def select_nit_qualifiers(
    conn: sqlite3.Connection,
    season_year: int,
    conf_ranked: list,
    main_bid_table: dict,
    already_qualified: set,
) -> list:
    """
    Returns a list of dicts in the same shape as select_qualifiers's
    output (team_name, conference, conf_coe_rank, conf_standing_rank,
    bid_type -- always "at_large" here), drawn from each conference's
    standings, taking whoever's next in line AFTER excluding teams
    actually in the main field.

    Filters by "not in already_qualified" rather than a static
    "conf_rank > main_bids" cutoff -- the two are NOT equivalent once
    the independent-CoE-threshold has run: if an independent displaced
    a conference's own at-large qualifier (e.g. Notre Dame bumping
    Colorado out of a Big 12 slot in 2024), that displaced team is a
    real, strong team that deserves NIT consideration, not a team to
    silently skip because its standing rank was within the nominal
    "already used by the main field" range. Verified: this exact case
    was previously causing a real 15-of-16 shortfall.
    """
    nit_qualifiers = []
    carry = 0

    for coe_rank, (conf, coeff) in enumerate(conf_ranked, start=1):
        n_bids = nit_bids_for_rank(coe_rank) + carry
        if n_bids == 0:
            continue

        rows = conn.execute(
            """
            SELECT t.team_name, s.conf_rank
            FROM conference_standings_by_year s
            JOIN teams t ON t.team_id = s.team_id
            WHERE s.season_year = ? AND s.conference = ?
            ORDER BY s.conf_rank
            """,
            (season_year, conf),
        ).fetchall()

        available = [(name, rank) for name, rank in rows if name not in already_qualified]
        taken = available[:n_bids]
        carry = n_bids - len(taken)

        for team_name, conf_rank in taken:
            nit_qualifiers.append(
                {
                    "team_name": team_name,
                    "conference": conf,
                    "conf_coe_rank": coe_rank,
                    "conf_standing_rank": conf_rank,
                    "bid_type": "at_large",
                    "pot": None,  # NIT has no pot/bye structure -- placeholder only so
                                  # apply_independent_threshold (shared with the main field,
                                  # which DOES use pot) doesn't KeyError on this field.
                }
            )

    return nit_qualifiers


def seed_nit_bracket(qualifiers: list):
    """
    Ranks 1-16 by 5yr rolling team CoE, splits into top-8/bottom-8 (like
    the main playoff's Pot1/Pot2), and pairs them using the SAME
    no-same-conference backtracking algorithm already proven for the
    main playoff's Round of 24 -- a naive fixed 1v16/2v15/... pairing
    was tested and found to produce same-conference matchups in several
    real seasons (2016, 2017, 2018, 2021, 2022), which is inconsistent
    with the whole project's design philosophy of avoiding them. The
    preferred pairing order (seed i vs seed 17-i) is tried first, with
    backtracking only kicking in when that specific pairing would be
    same-conference.
    """
    ranked = sorted(qualifiers, key=lambda q: -q["team_coe_5yr"])
    for i, q in enumerate(ranked):
        q["seed"] = i + 1
    by_seed = {q["seed"]: q for q in ranked}

    top8 = [by_seed[i] for i in range(1, 9)]
    bottom8 = [by_seed[i] for i in range(16, 8, -1)]  # 16,15,...,9 -- preferred partner order matches top8 index-for-index

    top8_names = [q["team_name"] for q in top8]
    bottom8_names = [q["team_name"] for q in bottom8]
    conf_of = {q["team_name"]: q["conference"] for q in ranked}

    name_pairs = backtrack_pairings(top8_names, bottom8_names, conf_of, bottom8_names[:])
    if name_pairs is None:
        raise SystemExit("No valid no-same-conference NIT pairing exists for this field.")

    by_name = {q["team_name"]: q for q in ranked}
    pairs = [(by_name[a], by_name[b]) for a, b in name_pairs]
    # Higher seed (lower seed number) hosts, matching the main playoff's homefield rule
    pairs = [(a, b) if a["seed"] < b["seed"] else (b, a) for a, b in pairs]
    return ranked, pairs


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    bid_table = YEAR1_BIDS if args.year == 2014 else YEAR2_BIDS

    conn = sqlite3.connect(args.db)
    conf_ranked = load_conference_coe_rank(conn, args.year)
    team_coe = load_team_coe_5yr(args.year)

    # Build the main field first, purely to know who's already claimed
    main_qualifiers = select_qualifiers(conn, args.year, conf_ranked, bid_table)
    main_qualifiers = assign_pots(main_qualifiers)
    assign_homefield(main_qualifiers, team_coe)
    main_independents = get_independent_teams_with_coe(conn, args.year, team_coe)
    main_qualifiers, main_replacements = apply_independent_threshold(main_qualifiers, main_independents)
    already_qualified = {q["team_name"] for q in main_qualifiers}

    nit_qualifiers = select_nit_qualifiers(conn, args.year, conf_ranked, bid_table, already_qualified)
    assign_homefield(nit_qualifiers, team_coe)

    # Independents not already claimed by the main field are eligible for the NIT too
    leftover_independents = [(n, c) for n, c in main_independents if n not in already_qualified]
    nit_qualifiers, nit_replacements = apply_independent_threshold(nit_qualifiers, leftover_independents)

    conn.close()

    if len(nit_qualifiers) != 16:
        print(f"WARNING: expected 16 NIT qualifiers, got {len(nit_qualifiers)}")

    if nit_replacements:
        print("NIT independent threshold triggered:")
        for r in nit_replacements:
            print(f"  {r['independent']} ({r['independent_coe']:.3f}) replaces "
                  f"{r['replaced_team']} [{r['replaced_conference']}] ({r['replaced_coe']:.3f})")

    ranked, pairs = seed_nit_bracket(nit_qualifiers)

    print(f"\n=== {args.year} NIT Field (seeded by 5yr CoE) ===")
    for q in ranked:
        print(f"  {q['seed']:>2}. {q['team_name']:<20} {q['conference']:<18} CoE={q['team_coe_5yr']:.3f}")

    print(f"\n=== {args.year} NIT Round of 16 ===")
    same_conf_flags = 0
    for i, (a, b) in enumerate(pairs, start=1):
        flag = ""
        if a["conference"] == b["conference"]:
            flag = "  <-- SAME CONFERENCE"
            same_conf_flags += 1
        print(f"  Game {i}: ({a['seed']}) {a['team_name']:<18} vs ({b['seed']}) {b['team_name']:<18} [home: {a['team_name']}]{flag}")

    if same_conf_flags:
        print(f"\nNOTE: {same_conf_flags} same-conference matchup(s) in this seeding -- "
              "deterministic seeding doesn't avoid this the way the main playoff's random draw does.")


if __name__ == "__main__":
    main()
