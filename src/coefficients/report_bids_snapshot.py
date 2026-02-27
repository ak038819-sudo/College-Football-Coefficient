#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from typing import List, Tuple

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def bids_year1(rank: int) -> int:
    # Year 1 rules from spec
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
    # Year 2+ rules from spec (baseline; NIT bonus handled later)
    if 1 <= rank <= 4:
        return 4
    if rank == 5:
        return 3
    if 6 <= rank <= 10:
        return 1
    return 0

def apply_bid_overrides(conference: str, base_bids: int) -> int:
    """
    Policy overrides for current-era FBS structure (v0):
    - Cap Independents at 2 bids
    - Ensure MAC gets at least 1 bid
    """
    if conference == "FBS Independents":
        return min(base_bids, 2)
    if conference == "Mid-American":
        return max(base_bids, 1)
    return base_bids


def rebalance_to_total(rows_with_bids, target_total: int) -> None:
    """
    Mutates rows_with_bids in-place to ensure total bids == target_total.
    Strategy:
      - If total too high, remove 1 bid at a time from the lowest-ranked conference
        that still has > 1 bid.
      - If total too low, add 1 bid at a time to the highest-ranked conference
        that isn't capped by overrides and is below a reasonable ceiling.
    """
    total = sum(r["bids"] for r in rows_with_bids)

    # Reduce if over
    while total > target_total:
        changed = False
        for r in reversed(rows_with_bids):  # start at bottom rank
            if r["bids"] > 1 and r["conference"] != "Mid-American":
                r["bids"] -= 1
                total -= 1
                changed = True
                break
        if not changed:
            break

    # Increase if under (shouldn't happen with your current rule set, but safe)
    while total < target_total:
        changed = False
        for r in rows_with_bids:  # start at top rank
            # allow adding bids, but don't exceed 4 (keeps structure sane)
            if r["conference"] == "FBS Independents":
                cap = 2
            else:
                cap = 4
            if r["bids"] < cap:
                r["bids"] += 1
                total += 1
                changed = True
                break
        if not changed:
            break

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True, help="Season year for which bids are determined.")
    p.add_argument("--formula-version", default="v0")
    p.add_argument(
        "--mode",
        choices=["year1", "year2"],
        default="year2",
        help="Bid allocation ruleset: year1 (initial) vs year2 (year 2+).",
    )
    p.add_argument("--window", type=int, default=5)
    args = p.parse_args()

    conn = connect(args.db)
    try:
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
                (args.year, args.formula_version),
            )
        )

        if not rows:
            raise SystemExit(
                f"No rolling rows found for year={args.year}, formula_version={args.formula_version}. "
                f"Did you run compute_conference_coe_rolling_5yr.py for this year?"
            )

        alloc_fn = bids_year1 if args.mode == "year1" else bids_year2plus

        ranked = []
        for i, r in enumerate(rows, start=1):
            base = alloc_fn(i)
            adjusted = apply_bid_overrides(r["conference"], base)
            ranked.append(
                {
                    "rank": i,
                    "conference": r["conference"],
                    "pts": float(r["total_points_5yr"]),
                    "games": int(r["games_counted_5yr"]),
                    "ppg": float(r["points_per_game_5yr"]),
                    "bids": int(adjusted),
                    "base_bids": int(base),
                }
            )

        # Ensure total bids remain 24 after overrides
        rebalance_to_total(ranked, target_total=24)

        print("")
        print(f"Conference Bids Snapshot — {args.year}")
        print(
            f"Ranking metric: total_points_5yr (window={args.window} years), "
            f"formula_version={args.formula_version}, ruleset={args.mode}"
        )
        print("")

        header = f"{'Rank':>4}  {'Conference':<18}  {'Pts(5yr)':>9}  {'Games':>5}  {'PPG':>6}  {'Bids':>4}"
        print(header)
        print("-" * len(header))

        total_bids = 0
        for item in ranked:
            total_bids += item["bids"]
            marker = "*" if item["bids"] != item["base_bids"] else ""
            print(
                f"{item['rank']:>4}  {item['conference']:<18}  "
                f"{item['pts']:>9.1f}  {item['games']:>5d}  {item['ppg']:>6.3f}  "
                f"{item['bids']:>4d}{marker}"
            )

        print("")
        print(f"Total allocated bids (conference spots, including champions): {total_bids}")
        print("Note: '*' indicates a policy override adjustment (e.g., IND cap / MAC floor).")
        print("")
    finally:
        conn.close()


if __name__ == "__main__":
    main()