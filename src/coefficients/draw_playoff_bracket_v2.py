#!/usr/bin/env python3
"""
Builds the Round-of-24 bracket for a season: a random pot draw (Pot 1 vs
Pot 2, 8 teams each) with a no-same-conference constraint, plus home-field
determined by team CoE. The 8 bye teams advance straight to the Round of
16 and aren't paired here -- this script only builds the Round of 24.

Reuses select_playoff_field_v2.py for the qualifier field itself, and
reuses the no-same-conference backtracking pairing algorithm and the
CoE-based home-field tiebreak from the archived discrete-system script
(archive/discrete_coe_system_2026-09/coefficients/build_round_of_24_year2.py)
-- that logic never depended on the discrete point system, only on a
conference map and a strength ranking, so it carries over unchanged.

The draw itself (which Pot1 team lands in which "slot") is randomized
with a seed for reproducibility -- rerunning with the same --draw-seed
reproduces the same bracket. The seed itself isn't meant to be secret
or fair in a cryptographic sense, just deterministic for testing.

Usage:
    python src/coefficients/draw_playoff_bracket_v2.py --year 2025 --draw-seed 1
"""
from __future__ import annotations

import argparse
import random
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from select_playoff_field_v2 import (
    YEAR1_BIDS,
    YEAR2_BIDS,
    load_conference_coe_rank,
    load_team_coe_5yr,
    select_qualifiers,
    assign_pots,
    assign_homefield,
)


def backtrack_pairings(
    pot1: List[str],
    pot2: List[str],
    conf_of: Dict[str, str],
    preferred: List[str],
) -> Optional[List[Tuple[str, str]]]:
    """
    Full matching between pot1 and pot2 teams such that conf(pot1[i]) !=
    conf(pot2[j]) for every pair. Deterministic: tries the "preferred"
    (draw-order) partner first, then remaining pot2 teams in list order.
    Returns None if no valid full matching exists (very rare -- would
    mean the field composition makes it impossible).
    """
    used = set()
    result: List[Tuple[str, str]] = []

    candidates: List[List[str]] = []
    for i, a in enumerate(pot1):
        pref_b = preferred[i]
        opts = []
        if conf_of[a] != conf_of[pref_b]:
            opts.append(pref_b)
        for b in pot2:
            if b == pref_b:
                continue
            if conf_of[a] != conf_of[b]:
                opts.append(b)
        candidates.append(opts)

    def dfs(i: int) -> bool:
        if i == len(pot1):
            return True
        a = pot1[i]
        for b in candidates[i]:
            if b in used:
                continue
            used.add(b)
            result.append((a, b))
            if dfs(i + 1):
                return True
            result.pop()
            used.remove(b)
        return False

    return result if dfs(0) else None


def choose_home_away(team_a: str, team_b: str, team_coe: Dict[str, float]) -> Tuple[str, str]:
    """Higher 5yr rolling team CoE hosts. Ties broken by team name (deterministic)."""
    ca = team_coe.get(team_a, 0.0)
    cb = team_coe.get(team_b, 0.0)
    if ca != cb:
        return (team_a, team_b) if ca > cb else (team_b, team_a)
    return (team_a, team_b) if team_a < team_b else (team_b, team_a)


def build_conf_map(qualifiers: list) -> Dict[str, str]:
    return {q["team_name"]: q["conference"] for q in qualifiers}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--draw-seed", type=int, default=1)
    args = p.parse_args()

    bid_table = YEAR1_BIDS if args.year == 2014 else YEAR2_BIDS

    conn = sqlite3.connect(args.db)
    conf_ranked = load_conference_coe_rank(args.year)
    team_coe = load_team_coe_5yr(args.year)
    qualifiers = select_qualifiers(conn, args.year, conf_ranked, bid_table)
    qualifiers = assign_pots(qualifiers)
    assign_homefield(qualifiers, team_coe)
    conn.close()

    byes = [q["team_name"] for q in qualifiers if q["pot"] == "bye"]
    pot1 = [q["team_name"] for q in qualifiers if q["pot"] == 1]
    pot2 = [q["team_name"] for q in qualifiers if q["pot"] == 2]

    if len(pot1) != len(pot2):
        raise SystemExit(
            f"Pot1 ({len(pot1)}) and Pot2 ({len(pot2)}) are not equal size -- "
            "cannot run a 1-to-1 draw. Check select_playoff_field_v2.py's seeding."
        )

    conf_of = build_conf_map(qualifiers)

    # Randomize draw order within each pot (this IS "the draw")
    rng = random.Random(args.draw_seed)
    pot1_drawn = pot1[:]
    pot2_drawn = pot2[:]
    rng.shuffle(pot1_drawn)
    rng.shuffle(pot2_drawn)

    # Preferred pairing mirrors ceremony intent: drawn order 1-to-1
    preferred = pot2_drawn[:]

    pairs = backtrack_pairings(pot1_drawn, pot2_drawn, conf_of, preferred)
    if pairs is None:
        raise SystemExit(
            "No valid Round of 24 pairing exists without a same-conference matchup "
            "for this field/draw-seed. Try a different --draw-seed."
        )

    print(f"=== {args.year} Round of 24 (draw-seed={args.draw_seed}) ===\n")
    print(f"Byes to Round of 16 ({len(byes)}):")
    for t in sorted(byes, key=lambda t: -team_coe.get(t, 0.0)):
        print(f"  {t:<20} (conf={conf_of[t]:<18} CoE 5yr={team_coe.get(t, 0.0):.3f})")

    print(f"\nRound of 24 games ({len(pairs)}):")
    for i, (a, b) in enumerate(pairs, start=1):
        home, away = choose_home_away(a, b, team_coe)
        print(
            f"  Game {i}: {away:<18} @ {home:<18} "
            f"({conf_of[away]} vs {conf_of[home]}) "
            f"[home CoE {team_coe.get(home,0.0):.3f} vs away CoE {team_coe.get(away,0.0):.3f}]"
        )


if __name__ == "__main__":
    main()
