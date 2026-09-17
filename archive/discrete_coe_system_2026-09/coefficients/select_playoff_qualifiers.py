#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from typing import Dict, List


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def bids_year1(rank: int) -> int:
    if 1 <= rank <= 4:
        return 4
    if rank == 5:
        return 3
    if rank == 6:
        return 2
    if 7 <= rank <= 10:
        return 1
    return 0


def bids_year2plus(rank: int) -> int:
    if 1 <= rank <= 4:
        return 4
    if rank == 5:
        return 3
    if 6 <= rank <= 10:
        return 1
    return 0


def apply_bid_overrides(conference: str, base_bids: int) -> int:
    # Policy patch until you realign to a fixed 10-conference world
    if conference == "FBS Independents":
        return min(base_bids, 2)
    if conference == "Mid-American":
        return max(base_bids, 1)
    return base_bids


def rebalance_to_total(ranked: List[Dict], target_total: int) -> None:
    """
    Ensure total bids == target_total after overrides.

    Strategy (safe + minimal):
      - If total too high: remove bids from the bottom-ranked conferences that have > 1,
        never dropping Mid-American below 1.
      - If total too low: add bids from the top-ranked conferences up to a cap (4, IND cap 2).
    """
    total = sum(r["bids"] for r in ranked)

    # Reduce if over
    while total > target_total:
        changed = False
        for r in reversed(ranked):
            if r["conference"] == "Mid-American":
                continue
            if r["conference"] == "FBS Independents":
                floor = 0  # could be 0, but we won't usually touch it once capped
            else:
                floor = 1  # keep champion-access for conferences that already have bids

            if r["bids"] > floor and r["bids"] > 1:
                r["bids"] -= 1
                total -= 1
                changed = True
                break
        if not changed:
            break

    # Increase if under (rare with your current rules, but safe)
    while total < target_total:
        changed = False
        for r in ranked:
            cap = 2 if r["conference"] == "FBS Independents" else 4
            if r["bids"] < cap:
                r["bids"] += 1
                total += 1
                changed = True
                break
        if not changed:
            break


def get_conference_bid_map(
    conn: sqlite3.Connection,
    season_year: int,
    formula_version: str,
    ruleset: str,
) -> Dict[str, int]:
    rows = list(
        conn.execute(
            """
            SELECT
              conference,
              total_points_5yr,
              games_counted_5yr,
              points_per_game_5yr
            FROM conference_coefficient_rolling_5yr
            WHERE season_year=? AND formula_version=?
            ORDER BY total_points_5yr DESC, points_per_game_5yr DESC, conference ASC
            """,
            (season_year, formula_version),
        )
    )
    if not rows:
        raise SystemExit(
            f"No rolling rows found for year={season_year}, formula_version={formula_version}. "
            "Run compute_conference_coe_rolling_5yr.py first."
        )

    alloc_fn = bids_year1 if ruleset == "year1" else bids_year2plus

    ranked = []
    for i, r in enumerate(rows, start=1):
        base = alloc_fn(i)
        bids = apply_bid_overrides(r["conference"], base)
        ranked.append(
            {
                "rank": i,
                "conference": r["conference"],
                "bids": int(bids),
                "base_bids": int(base),
            }
        )

    rebalance_to_total(ranked, target_total=24)
    return {r["conference"]: r["bids"] for r in ranked}


def select_qualifiers_for_conference(
    conn: sqlite3.Connection,
    season_year: int,
    conference: str,
    bids: int,
) -> List[Dict]:
    """
    Champion always qualifies: conf_rank=1.
    Remaining bids: next highest conf_rank (2..bids).
    """
    if bids <= 0:
        return []

    standings = list(
        conn.execute(
            """
            SELECT team_id, conf_rank
            FROM conference_standings_by_year
            WHERE season_year=? AND conference=?
            ORDER BY conf_rank ASC
            """,
            (season_year, conference),
        )
    )
    if not standings:
        return []

    # Must have champion row
    champ = next((r for r in standings if int(r["conf_rank"]) == 1), None)
    if champ is None:
        return []

    qualifiers: List[Dict] = []
    qualifiers.append(
        {
            "team_id": int(champ["team_id"]),
            "conf_rank": 1,
            "bid_type": "champion",
        }
    )

    # Fill remaining bids by rank order
    needed_at_large = bids - 1
    if needed_at_large > 0:
        for r in standings:
            cr = int(r["conf_rank"])
            if cr <= 1:
                continue
            qualifiers.append(
                {
                    "team_id": int(r["team_id"]),
                    "conf_rank": cr,
                    "bid_type": "at_large",
                }
            )
            needed_at_large -= 1
            if needed_at_large == 0:
                break

    return qualifiers


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--formula-version", default="v0")
    p.add_argument("--ruleset", choices=["year1", "year2"], default="year2")
    p.add_argument("--dry-run", action="store_true", help="Compute and print, but do not write to DB.")
    args = p.parse_args()

    conn = connect(args.db)
    try:
        bid_map = get_conference_bid_map(conn, args.year, args.formula_version, args.ruleset)

        # Clear existing qualifiers for deterministic reruns
        if not args.dry_run:
            conn.execute(
                """
                DELETE FROM playoff_qualifiers_by_year
                WHERE season_year=? AND formula_version=? AND ruleset=?
                """,
                (args.year, args.formula_version, args.ruleset),
            )

        inserted = 0
        missing_standings = 0

        for conf, bids in bid_map.items():
            quals = select_qualifiers_for_conference(conn, args.year, conf, bids)
            if not quals and bids > 0:
                missing_standings += 1
                continue

            for q in quals:
                if not args.dry_run:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO playoff_qualifiers_by_year
                          (season_year, conference, team_id, conf_rank, bid_type, formula_version, ruleset)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (args.year, conf, q["team_id"], q["conf_rank"], q["bid_type"], args.formula_version, args.ruleset),
                    )
                inserted += 1

        if not args.dry_run:
            conn.commit()

    finally:
        conn.close()

    print(f"Selected playoff qualifiers for {args.year}: {inserted} teams written (formula_version={args.formula_version}, ruleset={args.ruleset})")
    if missing_standings:
        print(f"WARNING: {missing_standings} conferences had bids but no standings rows found for that season.")


if __name__ == "__main__":
    main()