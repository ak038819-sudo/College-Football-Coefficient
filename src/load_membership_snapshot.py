#!/usr/bin/env python3
"""
Loads a committed membership snapshot CSV (produced by
export_membership_snapshot.py) into team_membership_by_season, making
that season's conference membership reproducible from git alone -- no
live API access needed. This is what lets a fresh --force rebuild
(e.g. in CI, with no CFBD_API_KEY) still have the current season's
playoff field, as long as someone has committed a recent snapshot.

Resolves team_name via the teams table, falling back to team_aliases
(same pattern as load_games.py), and reports any name it can't resolve
rather than silently skipping it. Safe to re-run -- upserts on
(team_id, season_year).

Usage:
    python src/load_membership_snapshot.py data/raw/membership_2026.csv --year 2026
"""
from __future__ import annotations

import argparse
import csv
import sqlite3


def resolve_team_id(cur: sqlite3.Cursor, raw_name: str):
    cur.execute("SELECT team_id FROM teams WHERE team_name = ?", (raw_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT team_name FROM team_aliases WHERE alias = ?", (raw_name,))
    row = cur.fetchone()
    if row:
        cur.execute("SELECT team_id FROM teams WHERE team_name = ?", (row[0],))
        row2 = cur.fetchone()
        if row2:
            return row2[0]
    return None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    inserted = 0
    unresolved = []
    with open(args.csv_path, newline="") as f:
        for row in csv.DictReader(f):
            team_id = resolve_team_id(cur, row["team_name"])
            if team_id is None:
                unresolved.append(row["team_name"])
                continue
            cur.execute(
                """
                INSERT INTO team_membership_by_season (team_id, season_year, conference_real, is_fbs)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(team_id, season_year) DO UPDATE SET
                    conference_real = excluded.conference_real,
                    is_fbs = excluded.is_fbs
                """,
                (team_id, args.year, row["conference_real"], int(row["is_fbs"])),
            )
            inserted += 1

    conn.commit()
    conn.close()

    print(f"Loaded {inserted} membership rows for {args.year} from {args.csv_path}")
    if unresolved:
        print(f"  {len(unresolved)} unresolved team names: {unresolved}")


if __name__ == "__main__":
    main()
