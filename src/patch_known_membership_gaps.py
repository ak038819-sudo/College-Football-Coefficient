#!/usr/bin/env python3
"""
Patches team_membership_by_season for teams that were completely missing
from the CFBD-sourced backup data despite being real, active FBS programs
with real game data in this project's dataset. Found by diffing the full
list of teams against which teams have zero membership rows across
2014-2025 (see conversation history / project notes for the discovery).

Each entry below is sourced and dated to when researched (Sept 2026);
re-verify if re-running this far in the future, since conference
membership changes.

These patches stop at 2025 and should stay that way. The gap they cover
was never about the backup: CFBD names six of these programs differently
from this project ("Miami", "San Jose State", "Florida Atlantic",
"Florida International", "UL Monroe", "App State") and
fetch_cfbd_team_memberships.py used to drop a school it could not name
without saying so. That is fixed at the source now -- the fetcher
resolves through the reviewed registry and fails on a school it cannot
name -- so a season fetched from 2026 on arrives complete and belongs in
its committed data/raw/membership_<year>.csv snapshot, not here.

Idaho is intentionally NOT given rows for 2018-2025: it dropped from
FBS (Sun Belt) to FCS (Big Sky) after the 2017 season and has remained
FCS since -- having no FBS membership row for those years is CORRECT,
not a gap.

Usage:
    python src/patch_known_membership_gaps.py --db db/league.db
"""
from __future__ import annotations

import argparse
import sqlite3

# (team_name, [(start_year, end_year_inclusive, conference_real), ...])
PATCHES = [
    ("Appalachian State", [(2014, 2025, "Sun Belt")]),
    ("FAU", [(2014, 2022, "Conference USA"), (2023, 2025, "American Athletic")]),
    ("FIU", [(2014, 2025, "Conference USA")]),
    ("Idaho", [(2014, 2017, "Sun Belt")]),  # FCS 2018+, intentionally no rows
    ("Massachusetts", [
        (2014, 2015, "Mid-American"),
        (2016, 2024, "FBS Independents"),
        (2025, 2025, "Mid-American"),
    ]),
    ("Miami (FL)", [(2014, 2025, "ACC")]),
    ("San Jose State", [(2014, 2025, "Mountain West")]),
    ("ULM", [(2014, 2025, "Sun Belt")]),
]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    total_inserted = 0
    for team_name, ranges in PATCHES:
        row = cur.execute("SELECT team_id FROM teams WHERE team_name=?", (team_name,)).fetchone()
        if not row:
            print(f"  SKIP: '{team_name}' not found in teams table")
            continue
        team_id = row[0]

        for start_year, end_year, conf in ranges:
            for year in range(start_year, end_year + 1):
                cur.execute(
                    "SELECT 1 FROM team_membership_by_season WHERE team_id=? AND season_year=?",
                    (team_id, year),
                )
                if cur.fetchone():
                    continue
                cur.execute(
                    """
                    INSERT INTO team_membership_by_season (team_id, season_year, conference_real, is_fbs)
                    VALUES (?, ?, ?, 1)
                    """,
                    (team_id, year, conf),
                )
                total_inserted += 1

        print(f"  Patched: {team_name}")

    conn.commit()
    conn.close()
    print(f"\nTotal membership rows inserted: {total_inserted}")


if __name__ == "__main__":
    main()
