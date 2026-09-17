#!/usr/bin/env python3
"""
Runs compute_team_coe_v1.compute_season() for a range of years, IN ORDER.

This is not optional sequencing -- season Y's bounty depends on every
opponent's v1 rolling 5yr PPG as of season Y-1, so each year must be
fully computed and rolled up before the next year starts.

Usage:
  python src/coefficients/run_team_coe_v1_pipeline.py --db db/league.db --start 2010 --end 2025
"""
from __future__ import annotations

import argparse

from compute_team_coe_v1 import connect, compute_season


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    args = p.parse_args()

    if args.start > args.end:
        raise SystemExit(f"--start ({args.start}) must be <= --end ({args.end})")

    conn = connect(args.db)
    try:
        for year in range(args.start, args.end + 1):
            compute_season(conn, year)
            conn.commit()
            print(f"  v1 team CoE done: {year}")
    finally:
        conn.close()

    print(f"Team CoE v1 pipeline complete: {args.start}-{args.end}")


if __name__ == "__main__":
    main()
