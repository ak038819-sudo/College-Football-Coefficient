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


def select_qualifiers(conn: sqlite3.Connection, season_year: int, conf_ranked: list, bid_table: dict):
    """
    Returns list of dicts: {team_name, conference, conf_coe_rank, conf_standing_rank, bid_type}
    """
    qualifiers = []
    for coe_rank, (conf, coeff) in enumerate(conf_ranked, start=1):
        n_bids = bid_count_for_rank(coe_rank, bid_table)
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

        for team_name, conf_rank in rows:
            qualifiers.append(
                {
                    "team_name": team_name,
                    "conference": conf,
                    "conf_coe_rank": coe_rank,
                    "conf_standing_rank": conf_rank,
                    "bid_type": "champion" if conf_rank == 1 else "at_large",
                }
            )
    return qualifiers


def assign_pots(qualifiers: list) -> list:
    """
    Assigns pot: 'bye', 1, or 2, per the seeding tables. Year 1 and
    Year 2+ use the SAME structure here (see module docstring re: the
    unresolved conference-6 ambiguity) -- this keeps byes at exactly 8,
    satisfying rule 8.
    """
    by_conf_rank: dict[int, list] = {}
    for q in qualifiers:
        by_conf_rank.setdefault(q["conf_coe_rank"], []).append(q)
    for rank, teams in by_conf_rank.items():
        teams.sort(key=lambda q: q["conf_standing_rank"])

    for coe_rank, teams in by_conf_rank.items():
        if coe_rank in (1, 2):
            # 1st & 2nd get byes, 3rd -> Pot1, 4th -> Pot2
            for q in teams:
                if q["conf_standing_rank"] in (1, 2):
                    q["pot"] = "bye"
                elif q["conf_standing_rank"] == 3:
                    q["pot"] = 1
                else:
                    q["pot"] = 2
        elif coe_rank in (3, 4):
            # champ bye, runner-up + 3rd -> Pot1, 4th -> Pot2
            for q in teams:
                if q["conf_standing_rank"] == 1:
                    q["pot"] = "bye"
                elif q["conf_standing_rank"] in (2, 3):
                    q["pot"] = 1
                else:
                    q["pot"] = 2
        elif coe_rank == 5:
            for q in teams:
                if q["conf_standing_rank"] == 1:
                    q["pot"] = "bye"
                elif q["conf_standing_rank"] == 2:
                    q["pot"] = 1
                else:
                    q["pot"] = 2
        elif coe_rank == 6:
            # Sole qualifier (champion only, both rulesets) always gets the bye
            for q in teams:
                q["pot"] = "bye"
        else:  # 7-10
            for q in teams:
                q["pot"] = 2

    return qualifiers


def assign_homefield(qualifiers: list, team_coe: dict) -> None:
    """Adds 'team_coe_5yr' to each qualifier for homefield-advantage reference."""
    for q in qualifiers:
        q["team_coe_5yr"] = team_coe.get(q["team_name"], 0.0)


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

    n_bye = sum(1 for q in qualifiers if q["pot"] == "bye")
    n_pot1 = sum(1 for q in qualifiers if q["pot"] == 1)
    n_pot2 = sum(1 for q in qualifiers if q["pot"] == 2)
    print(f"\nTotal qualifiers: {len(qualifiers)} (byes={n_bye}, pot1={n_pot1}, pot2={n_pot2})")

    print(f"\n{'Team':<20} {'Conf':<18} {'CoE Rk':>6} {'Std Rk':>6} {'Bid':<10} {'Pot':<6} {'Team CoE 5yr':>12}")
    for q in sorted(qualifiers, key=lambda x: (x["conf_coe_rank"], x["conf_standing_rank"])):
        print(
            f"{q['team_name']:<20} {q['conference']:<18} {q['conf_coe_rank']:>6} "
            f"{q['conf_standing_rank']:>6} {q['bid_type']:<10} {str(q['pot']):<6} {q['team_coe_5yr']:>12.3f}"
        )

    conn.close()


if __name__ == "__main__":
    main()
