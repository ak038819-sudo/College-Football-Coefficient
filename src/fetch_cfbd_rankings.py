#!/usr/bin/env python3
"""Every AP and CFP poll release CFBD has, one CSV per season.

WHY THIS EXISTS SEPARATELY. `fetch_cfbd_games.py` already fetches a season's
rankings, as a side effect of fetching that season's games, and treats a
failure as a warning -- quite rightly, since a display-only poll must never
break the games fetch. That is the right behaviour for the CURRENT season and
the wrong one for a backfill: `poll_rankings` held 325 rows, all of 2026,
while CFBD serves the AP poll back to 1936. Walking ninety seasons through the
games fetcher would re-fetch every game to get them.

So this asks for the polls and nothing else, and unlike its sibling it FAILS
on an error instead of warning, because a backfill that silently skips 1961
leaves a hole nobody will ever notice.

RESUMABLE, because ninety seasons is more than one run wants to risk. A season
whose file is already committed is skipped unless --force, so a workflow can
be dispatched again after a timeout and carry on from where it stopped.

A SEASON THE ENDPOINT HAS NOTHING FOR WRITES NO FILE. The AP poll did not
exist before 1936, was not published in some wartime seasons, and the CFP
poll begins in 2014. An empty CSV would make `src/load_rankings.py` delete
that season's rows and write none, which is how real data gets replaced with
an absence.

NOTHING HERE FEEDS A MODEL. Polls are display data; see sql/rankings_tables.sql.

Usage:
    python src/fetch_cfbd_rankings.py --probe 1950
    python src/fetch_cfbd_rankings.py 1936 2025
    python src/fetch_cfbd_rankings.py 1936 2025 --force
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cfbd_http
from fetch_cfbd_games import rankings_rows

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw")

# The AP poll begins in 1936. This is a floor on what to ASK for, not a claim
# about what exists: a season the endpoint answers empty writes no file, and
# the run says so.
FIRST_SEASON = 1936

# Written by src/fetch_cfbd_games.py too, and load_rankings.py reads both, so
# the two must not drift. tests/test_rankings_backfill.py pins that.
CSV_COLUMNS = ["season_year", "season_type", "week", "poll", "rank", "school",
               "conference", "first_place_votes", "points"]


def season_rows(year: int, headers: dict) -> list[dict]:
    """Both season types, flattened by the same function the games fetch uses.

    Reusing `rankings_rows` is the point: a column that changes shape must
    change in one place, or a 1959 row and a 2026 row stop being the same
    kind of row.
    """
    rows = []
    for season_type in ("regular", "postseason"):
        payload = cfbd_http.get_json(
            f"{BASE}/rankings", params={"year": year, "seasonType": season_type},
            headers=headers, timeout=60, describe=f"GET /rankings {year} {season_type}")
        rows.extend(rankings_rows(payload, year))
    return rows


def path_for(year: int, out_dir: Path | None = None) -> Path:
    return (out_dir or OUT_DIR) / f"rankings_{year}.csv"


def write_season(year: int, headers: dict, out_dir: Path | None = None,
                 force: bool = False) -> int:
    """Returns the number of rows written; 0 means no file was written."""
    path = path_for(year, out_dir)
    if path.exists() and not force:
        print(f"{year}: already committed, skipping ({path})")
        return 0
    rows = season_rows(year, headers)
    if not rows:
        # Never truncate: an empty file would have load_rankings delete the
        # season's rows and write none.
        print(f"{year}: the endpoint has no poll release; no file written")
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    polls = sorted({row["poll"] for row in rows})
    releases = len({(row["season_type"], row["week"], row["poll"]) for row in rows})
    print(f"{year}: {len(rows)} rows, {releases} releases, polls {','.join(polls)} -> {path}")
    return len(rows)


def probe(year: int, headers: dict) -> None:
    """What the endpoint actually returns, written nowhere."""
    for season_type in ("regular", "postseason"):
        payload = cfbd_http.get_json(
            f"{BASE}/rankings", params={"year": year, "seasonType": season_type},
            headers=headers, timeout=60, describe=f"probe /rankings {year} {season_type}")
        weeks = payload if isinstance(payload, list) else []
        print(f"{year} {season_type}: {len(weeks)} week objects")
        if weeks:
            print("  first:", json.dumps(weeks[0])[:400])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("start", type=int)
    parser.add_argument("end", type=int, nargs="?")
    parser.add_argument("--probe", action="store_true",
                        help="print what the endpoint returns; write nothing")
    parser.add_argument("--force", action="store_true",
                        help="re-fetch a season whose file is already committed")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args(argv)

    # The range is checked BEFORE the key: an impossible range is wrong whether
    # or not a key is present, and saying "set CFBD_API_KEY" to someone who
    # asked for 1900 sends them to fix the wrong thing.
    if args.start < FIRST_SEASON:
        print(f"ERROR: the AP poll does not predate {FIRST_SEASON}.")
        return 2
    key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {key}"}
    end = args.end if args.end is not None else args.start
    out_dir = Path(args.out_dir) if args.out_dir else None

    written = seasons = 0
    for year in range(args.start, end + 1):
        if args.probe:
            probe(year, headers)
            continue
        rows = write_season(year, headers, out_dir, args.force)
        if rows:
            seasons += 1
            written += rows
    if not args.probe:
        print(f"rankings backfill: {written:,} rows over {seasons} seasons")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
