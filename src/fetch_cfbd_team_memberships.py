#!/usr/bin/env python3
"""
Populate team_membership_by_season using CFBD teams endpoint.

CFBD's `school` string is not this project's team name. It sends "Miami"
for Miami (FL), "San José State", "Florida Atlantic", "Florida
International" and "UL Monroe", among others. This script used to look
the school up with a bare `WHERE team_name = ?` and `continue` on a miss,
so every one of those programs was dropped from every season it fetched
WITHOUT SAYING SO. That is how six real FBS programs ended up with no
2026 conference at all -- and why patch_known_membership_gaps.py exists
to paper over the same six for 2014-2025.

So the school is resolved through team_identity.resolve_database_name,
which consults the reviewed registry's aliases and the team_aliases
table, and a school that still will not resolve ends the run with its
name printed. A silent skip is never the right answer here: a missing
membership row removes a team from conference standings, conference CoE
and the conference-flow audit, and nothing downstream can tell a team
that has no conference from a team nobody looked up.

A school CFBD reports as FBS with no conference is a different case and
is NOT an error -- a program reclassifying from FCS genuinely has no FBS
conference yet (Sacramento State and North Dakota State in 2026). Those
are listed too, so the gap is visible rather than inferred from a
smaller row count.

Usage:
    python src/fetch_cfbd_team_memberships.py 2014 2025
"""

import os
import sys
import sqlite3

import cfbd_http
import team_identity

DB_PATH = "db/league.db"
CFBD_API = "https://api.collegefootballdata.com/teams"

def _headers() -> dict[str, str]:
    # Read at CALL time, not import time. Raising on import made load_year
    # unreachable from a test that never touches the network, which is the
    # part of this file most worth testing.
    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        raise RuntimeError("CFBD_API_KEY env var not set.")
    return {"Authorization": f"Bearer {api_key}"}


def fetch_year(year: int):
    # Nothing here degrades to a warning: a reset on any season ends the run
    # with a traceback and the memberships half written.
    return cfbd_http.get_json(CFBD_API, params={"year": year},
                              headers=_headers(), timeout=60,
                              describe=f"GET /teams {year}")


def load_year(cur, year: int, data) -> tuple[int, list[str], list[str]]:
    """Insert one season's memberships.

    Returns (inserted, unresolved, fbs_without_conference). `unresolved`
    holds FBS schools this project could not name; the caller treats it
    as a failure. `fbs_without_conference` holds FBS schools CFBD gave no
    conference for, which is reported but not an error.
    """
    inserted = 0
    unresolved: list[str] = []
    no_conference: list[str] = []

    for t in data:
        school = t.get("school")
        conference = t.get("conference")
        classification = (t.get("classification") or "").lower()

        if not school or classification != "fbs":
            continue

        # Resolve BEFORE the conference check, so an unnameable school is
        # reported even when CFBD also left its conference empty.
        team_name = team_identity.resolve_database_name(cur, school)

        if not conference or not str(conference).strip():
            if team_name:
                no_conference.append(f"{school} -> {team_name}")
            else:
                no_conference.append(school)
            continue

        if team_name is None:
            unresolved.append(school)
            continue

        cur.execute(
            """
            INSERT OR IGNORE INTO team_membership_by_season
            (team_id, season_year, conference_real, is_fbs)
            SELECT team_id, ?, ?, 1 FROM teams WHERE team_name = ?
            """,
            (year, conference, team_name),
        )

        inserted += 1

    return inserted, unresolved, no_conference


def main(start_year: int, end_year: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    inserted = 0
    unresolved: dict[int, list[str]] = {}
    no_conference: dict[int, list[str]] = {}

    for year in range(start_year, end_year + 1):
        print(f"Fetching teams for {year}...")
        count, missing, blank = load_year(cur, year, fetch_year(year))
        inserted += count
        if missing:
            unresolved[year] = missing
        if blank:
            no_conference[year] = blank

    conn.commit()
    conn.close()

    print(f"Inserted memberships: {inserted}")

    for year, schools in sorted(no_conference.items()):
        print(f"{year}: FBS with no conference from CFBD (no row written): "
              f"{', '.join(sorted(schools))}")

    if unresolved:
        for year, schools in sorted(unresolved.items()):
            print(f"::error::{year}: CFBD FBS schools this project could not "
                  f"name: {', '.join(sorted(schools))}", file=sys.stderr)
        print("::error::Add each school to data/reference/team_identities.json "
              "as an alias of its canonical team, then re-run. No membership "
              "row was written for them.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: fetch_cfbd_team_memberships.py START_YEAR END_YEAR")
    raise SystemExit(main(int(sys.argv[1]), int(sys.argv[2])))
