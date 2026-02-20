#!/usr/bin/env python3
"""
Populate team_membership_by_season using CFBD teams endpoint.

Usage:
    python src/fetch_cfbd_team_memberships.py 2014 2025
"""

import os
import sys
import requests
import sqlite3

DB_PATH = "db/league.db"
CFBD_API = "https://api.collegefootballdata.com/teams"

API_KEY = os.getenv("CFBD_API_KEY")
if not API_KEY:
    raise RuntimeError("CFBD_API_KEY env var not set.")

HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def fetch_year(year: int):
    params = {"year": year}
    r = requests.get(CFBD_API, headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def main(start_year: int, end_year: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    inserted = 0

    for year in range(start_year, end_year + 1):
        print(f"Fetching teams for {year}...")
        data = fetch_year(year)

        for t in data:
            team_name = t.get("school")
            conference = t.get("conference")
            classification = (t.get("classification") or "").lower()

            if not team_name or not conference:
                continue

            # Only FBS
            if classification != "fbs":
                continue

            row = cur.execute(
                "SELECT team_id FROM teams WHERE team_name=?",
                (team_name,),
            ).fetchone()

            if not row:
                continue

            team_id = row[0]

            cur.execute(
                """
                INSERT OR IGNORE INTO team_membership_by_season
                (team_id, season_year, conference_real, is_fbs)
                VALUES (?, ?, ?, 1)
                """,
                (team_id, year, conference),
            )

            inserted += 1

    conn.commit()
    conn.close()

    print(f"Inserted memberships: {inserted}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: fetch_cfbd_team_memberships.py START_YEAR END_YEAR")
    raise SystemExit(main(int(sys.argv[1]), int(sys.argv[2])))