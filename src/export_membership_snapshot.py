#!/usr/bin/env python3
"""
Exports team_membership_by_season for one year to a stable CSV file, so
that year's conference membership becomes reproducible from git alone.

Without this, only historical years (2014-2025, baked into the old
backup database) have committed membership -- the CURRENT season's
membership only exists because of a live CFBD API fetch in your own
environment (fetch_cfbd_team_memberships.py), which a fresh --force
rebuild (e.g. in CI, with no API key) has no way to reproduce.

Run this any time you fetch fresh membership for the current season,
right after fetch_cfbd_team_memberships.py, then commit the output CSV
alongside your games CSV. run_pipeline.py automatically loads any
data/raw/membership_*.csv it finds, via load_membership_snapshot.py.

Usage:
    python src/export_membership_snapshot.py --year 2026
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--out", default=None)
    args = p.parse_args()

    out_path = Path(args.out) if args.out else Path(f"data/raw/membership_{args.year}.csv")

    conn = sqlite3.connect(args.db)
    rows = conn.execute(
        """
        SELECT t.team_name, m.conference_real, m.is_fbs
        FROM team_membership_by_season m
        JOIN teams t ON t.team_id = m.team_id
        WHERE m.season_year = ?
        ORDER BY t.team_name
        """,
        (args.year,),
    ).fetchall()
    conn.close()

    if not rows:
        raise SystemExit(f"No membership rows found for {args.year} in {args.db} -- fetch it first.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["team_name", "conference_real", "is_fbs"])
        w.writerows(rows)

    print(f"Wrote {len(rows)} membership rows for {args.year} to {out_path}")


if __name__ == "__main__":
    main()
