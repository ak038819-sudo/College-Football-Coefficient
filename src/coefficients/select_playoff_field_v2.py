#!/usr/bin/env python3
"""
Builds the 24-team playoff field for a season, using the iterative
rating model as the source of truth (build_coefficients.py's output),
NOT the old discrete win/loss-point system.

Pipeline per season:
  1. Rank the 10 real conferences (excludes "FBS Independents" -- the
     new ruleset requires every team belong to a conference, so
     independents aren't bid-eligible in this model) by their rolling
     5yr conference coefficient (data/processed/conference_coeff_5yr.csv).
  2. Look up each conference's bid count from the Year 1 (2014) or
     Year 2+ (2015-2025) bid table.
  3. Within each conference, take the top-N teams by conf_rank
     (conference_standings_by_year, derived from our own loaded games --
     see derive_conference_standings_local.py) as that conference's
     qualifiers.
  4. Assign seeding pots (Bye / Pot 1 / Pot 2) per the seeding tables.

Conference 6 gets exactly 1 bid (champion only) in BOTH Year 1 and
Year 2+ -- this was changed from the original spec's Year 1 table
(which gave it 2 bids) per project decision, which also resolves what
was previously a contradiction between Year 1's and Year 2+'s seeding
rules for that slot. Year 1 and Year 2+ bid tables and seeding are now
identical; the ruleset distinction is kept in the code for clarity and
in case the two diverge again later (e.g. once the NIT-winner bonus
bid is implemented).

Conference rank 5's 3rd-place qualifier goes to Pot 1 (not Pot 2, as
the original spec's seeding table said) -- also a project decision, to
balance Pot 1 and Pot 2 at 8 teams each. Unequal pots (7 vs 9 under the
original table) can't support a clean 1-to-1 Pot1-vs-Pot2 draw pairing.

Team-level seeding source: 5-year rolling team coefficient
(data/processed/team_coeff_5yr.csv), per project decision.

Usage:
    python src/coefficients/select_playoff_field_v2.py --year 2025
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

DATA_DIR = Path("data/processed")

# (min_coe_rank, max_coe_rank) -> bid count, Year 1 (first simulated season: 2014)
# NOTE: conference 6 was changed from 2 bids to 1 bid per project decision --
# this makes YEAR1_BIDS identical to YEAR2_BIDS, and also resolves the
# earlier seeding ambiguity (conf 6's champion is now always the sole
# qualifier and always gets the bye, in both rulesets -- no more
# contradiction with rule 8's fixed 8-bye count).
YEAR1_BIDS = {
    (1, 4): 4,
    (5, 5): 3,
    (6, 10): 1,
}

# Year 2+ (2015-2025): same as Year 1 now
YEAR2_BIDS = {
    (1, 4): 4,
    (5, 5): 3,
    (6, 10): 1,
}


def bid_count_for_rank(coe_rank: int, table: dict) -> int:
    for (lo, hi), bids in table.items():
        if lo <= coe_rank <= hi:
            return bids
    return 0


def load_conference_coe_rank(season_year: int) -> list[tuple[str, float]]:
    """
    Returns [(conference_name, coeff_5yr), ...] sorted strongest-first,
    EXCLUDING 'FBS Independents' (not bid-eligible -- see module docstring).
    """
    rows = []
    with open(DATA_DIR / "conference_coeff_5yr.csv", newline="") as f:
        for r in csv.DictReader(f):
            if int(r["end_year"]) != season_year:
                continue
            if r["conference_name"] == "FBS Independents":
                continue
            rows.append((r["conference_name"], float(r["coeff_5yr"])))
    rows.sort(key=lambda x: -x[1])
    return rows


def load_team_coe_5yr(season_year: int) -> dict[str, float]:
    """team_name -> rolling 5yr CoE as of season_year, for seeding/homefield."""
    out = {}
    with open(DATA_DIR / "team_coeff_5yr.csv", newline="") as f:
        for r in csv.DictReader(f):
            if int(r["end_year"]) == season_year:
                out[r["team_name"]] = float(r["coeff_5yr"])
    return out


def tier_pot_template(coe_rank: int) -> list:
    """
    The pot destiny for each of a tier's OWN slots, in conf_standing_rank
    order (1st place first). Length always equals bid_count_for_rank for
    that tier under the current bid tables (both Year1 and Year2+, which
    are now identical -- see module docstring).
    """
    if coe_rank in (1, 2):
        return ["bye", "bye", 1, 2]
    if coe_rank in (3, 4):
        return ["bye", 1, 1, 2]
    if coe_rank == 5:
        return ["bye", 1, 1]
    if coe_rank == 6:
        return ["bye"]
    if 7 <= coe_rank <= 10:
        return [2]
    return []


def select_qualifiers(conn: sqlite3.Connection, season_year: int, conf_ranked: list, bid_table: dict):
    """
    Returns list of dicts: {team_name, conference, conf_coe_rank,
    conf_standing_rank, bid_type, pot} -- pot is assigned HERE, directly
    during selection (not as a separate pass -- see assign_pots, now a
    no-op kept for callers that still invoke it).

    Project decision: if a conference ranks highly enough to earn
    multiple bids but doesn't actually have enough member teams to fill
    them (e.g. the Pac-12 collapsing to 2 teams by 2024 while still
    ranking #5 by conference CoE, which would normally mean 3 bids: bye,
    Pot1, Pot1), the SPECIFIC missing slot's pot destiny -- not just a
    bare count -- carries forward to the next-ranked conference,
    cascading further if that conference also can't absorb it. This
    preserves the exact pot-1/pot-2/bye balance regardless of which real
    conference ends up filling a given slot. This is a real, previously
    undiscovered edge case (caught by the test suite, not anticipated by
    the original rules).
    """
    qualifiers = []
    carried_destinies: list = []

    for coe_rank, (conf, coeff) in enumerate(conf_ranked, start=1):
        own_template = tier_pot_template(coe_rank)
        full_destinies = own_template + carried_destinies
        n_bids = len(full_destinies)
        if n_bids == 0:
            continue

        rows = conn.execute(
            """
            SELECT t.team_name, s.conf_rank
            FROM conference_standings_by_year s
            JOIN teams t ON t.team_id = s.team_id
            WHERE s.season_year = ? AND s.conference = ?
            ORDER BY s.conf_rank
            LIMIT ?
            """,
            (season_year, conf, n_bids),
        ).fetchall()

        for (team_name, conf_rank), pot in zip(rows, full_destinies):
            qualifiers.append(
                {
                    "team_name": team_name,
                    "conference": conf,
                    "conf_coe_rank": coe_rank,
                    "conf_standing_rank": conf_rank,
                    "bid_type": "champion" if conf_rank == 1 else "at_large",
                    "pot": pot,
                }
            )

        carried_destinies = full_destinies[len(rows):]

    return qualifiers


def assign_pots(qualifiers: list) -> list:
    """
    No-op: pot is now assigned directly in select_qualifiers (see its
    docstring for why -- the old two-step approach couldn't correctly
    carry a specific pot destiny forward when a conference came up
    short on real teams). Kept so existing callers don't need updating.
    """
    return qualifiers

    return qualifiers


def assign_homefield(qualifiers: list, team_coe: dict) -> None:
    """Adds 'team_coe_5yr' to each qualifier for homefield-advantage reference."""
    for q in qualifiers:
        q["team_coe_5yr"] = team_coe.get(q["team_name"], 0.0)


def get_independent_teams_with_coe(conn: sqlite3.Connection, season_year: int, team_coe: dict) -> list:
    """
    FBS Independents for this season, sorted strongest-first by 5yr
    rolling team CoE. Independents don't belong to any bid-eligible
    conference (see module docstring), so they're never in the normal
    conference-standings qualifier pool -- they only enter the field via
    apply_independent_threshold().
    """
    rows = conn.execute(
        """
        SELECT t.team_name FROM team_membership_by_season m
        JOIN teams t ON t.team_id = m.team_id
        WHERE m.season_year = ? AND m.conference_real = 'FBS Independents'
        """,
        (season_year,),
    ).fetchall()
    return sorted(
        [(r[0], team_coe.get(r[0], 0.0)) for r in rows], key=lambda x: -x[1]
    )


def apply_independent_threshold(qualifiers: list, independents: list) -> tuple[list, list]:
    """
    Project decision: Independents are evaluated on their own team CoE,
    not a conference bid table (they have no conference standings). If
    an independent's 5yr CoE exceeds the field's weakest AT-LARGE
    qualifier (never a conference champion -- rule 7 protects those
    unconditionally), it replaces that qualifier, inheriting its exact
    pot assignment. Checked independent-by-independent, strongest first,
    always against the current weakest remaining at-large qualifier
    (which can change after each replacement) -- so more than one
    independent can qualify in the same season if the field is weak
    enough at the bottom.

    Returns (updated_qualifiers, replacement_log) where each log entry
    records exactly what displaced what, for transparency.
    """
    replacements = []
    for indep_name, indep_coe in independents:
        if any(q["team_name"] == indep_name for q in qualifiers):
            continue  # already in field somehow; skip
        at_large = [q for q in qualifiers if q["bid_type"] == "at_large"]
        if not at_large:
            break
        weakest = min(at_large, key=lambda q: q["team_coe_5yr"])
        if indep_coe > weakest["team_coe_5yr"]:
            qualifiers.remove(weakest)
            qualifiers.append(
                {
                    "team_name": indep_name,
                    "conference": "FBS Independents",
                    "conf_coe_rank": None,
                    "conf_standing_rank": None,
                    "bid_type": "independent",
                    "pot": weakest["pot"],
                    "team_coe_5yr": indep_coe,
                }
            )
            replacements.append(
                {
                    "independent": indep_name,
                    "independent_coe": round(indep_coe, 3),
                    "replaced_team": weakest["team_name"],
                    "replaced_conference": weakest["conference"],
                    "replaced_coe": round(weakest["team_coe_5yr"], 3),
                }
            )
    return qualifiers, replacements


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    ruleset = "year1" if args.year == 2014 else "year2"
    bid_table = YEAR1_BIDS if ruleset == "year1" else YEAR2_BIDS

    conn = sqlite3.connect(args.db)
    conf_ranked = load_conference_coe_rank(args.year)
    team_coe = load_team_coe_5yr(args.year)

    print(f"=== {args.year} ({ruleset}) conference CoE ranking ===")
    for i, (conf, coeff) in enumerate(conf_ranked, start=1):
        bids = bid_count_for_rank(i, bid_table)
        print(f"  {i:>2}. {conf:<20} {coeff:>10.3f}  -> {bids} bid(s)")

    qualifiers = select_qualifiers(conn, args.year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)

    independents = get_independent_teams_with_coe(conn, args.year, team_coe)
    qualifiers, replacements = apply_independent_threshold(qualifiers, independents)

    if independents:
        print(f"\nIndependents this season: " + ", ".join(f"{n} ({c:.3f})" for n, c in independents))
    if replacements:
        print("Independent threshold triggered:")
        for r in replacements:
            print(
                f"  {r['independent']} ({r['independent_coe']:.3f}) replaces "
                f"{r['replaced_team']} [{r['replaced_conference']}] ({r['replaced_coe']:.3f})"
            )
    elif independents:
        print("No independent exceeded the weakest at-large qualifier -- field unchanged.")

    n_bye = sum(1 for q in qualifiers if q["pot"] == "bye")
    n_pot1 = sum(1 for q in qualifiers if q["pot"] == 1)
    n_pot2 = sum(1 for q in qualifiers if q["pot"] == 2)
    print(f"\nTotal qualifiers: {len(qualifiers)} (byes={n_bye}, pot1={n_pot1}, pot2={n_pot2})")

    def sort_key(q):
        # Independents have no conf_coe_rank/conf_standing_rank -- sort them
        # after everything else, by their own team CoE descending.
        if q["conf_coe_rank"] is None:
            return (999, -q["team_coe_5yr"])
        return (q["conf_coe_rank"], q["conf_standing_rank"])

    print(f"\n{'Team':<20} {'Conf':<18} {'CoE Rk':>6} {'Std Rk':>6} {'Bid':<12} {'Pot':<6} {'Team CoE 5yr':>12}")
    for q in sorted(qualifiers, key=sort_key):
        coe_rk = q["conf_coe_rank"] if q["conf_coe_rank"] is not None else "-"
        std_rk = q["conf_standing_rank"] if q["conf_standing_rank"] is not None else "-"
        print(
            f"{q['team_name']:<20} {q['conference']:<18} {str(coe_rk):>6} "
            f"{str(std_rk):>6} {q['bid_type']:<12} {str(q['pot']):<6} {q['team_coe_5yr']:>12.3f}"
        )

    conn.close()


if __name__ == "__main__":
    main()
