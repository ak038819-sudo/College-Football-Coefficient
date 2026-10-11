#!/usr/bin/env python3
"""
Recruiting classes per team-season and per recruit, from CFBD's /recruiting.

WHAT THIS IS FOR. The site knows what a coach DID -- wins above expectation,
Elo gained, every game of every tenure -- and nothing at all about what he was
handed. A coach who wins nine games with the 40th class and one who wins nine
with the 4th are the same row today. This is the input for the two items on
Austin's list that need it: coach recruiting records, and the recruiting
pipeline map (CFBD gives each recruit a hometown with coordinates, which is
what makes a map possible rather than a table of state names).

It is INPUT DATA ONLY. Nothing reads these files until a later change loads
them, so landing this changes no published number.

TWO ENDPOINTS, two files per season:

  /recruiting/teams   -> data/raw/recruiting_teams_<year>.csv
      One row per team: the class rank and point total.
  /recruiting/players -> data/raw/recruiting_players_<year>.csv
      One row per recruit: stars, rating, position, and the hometown.

UNKNOWN FIELDS FAIL THE RUN. This repository's recurring fault is a silent
drop -- a bare lookup that `continue`s past what it does not recognise (see
docs/identity-problems.md and team_identity.resolve_database_name). A metric
CFBD adds later is not something to discard quietly, so a key outside the
known set stops the run and names itself. `--probe` prints what the endpoints
actually return without writing anything, which is how the field lists below
are meant to be established rather than assumed.

Usage:
    python src/fetch_cfbd_recruiting.py --probe 2024
    python src/fetch_cfbd_recruiting.py 2000 2026
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

import cfbd_http

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw")
# CFBD's recruiting coverage starts in the 2000s; the probe reports where it
# really begins. This is a floor on what to ask for, not a claim about what
# exists -- a season the endpoint has nothing for writes no file.
FIRST_SEASON = 2000

# (CSV column, key in CFBD's response).
TEAM_FIELDS = [
    ("rank", "rank"),
    ("team", "team"),
    ("points", "points"),
]
TEAM_KNOWN = {"year"} | {key for _col, key in TEAM_FIELDS}
TEAM_CSV = ["season_year"] + [col for col, _key in TEAM_FIELDS]

PLAYER_FIELDS = [
    ("recruit_id", "id"),
    ("athlete_id", "athleteId"),
    ("recruit_type", "recruitType"),
    ("name", "name"),
    ("school", "school"),
    ("committed_to", "committedTo"),
    ("position", "position"),
    ("height", "height"),
    ("weight", "weight"),
    ("stars", "stars"),
    ("rating", "rating"),
    ("national_rank", "ranking"),
    ("city", "city"),
    ("state_province", "stateProvince"),
    ("country", "country"),
]
# The hometown arrives as a nested object. Its coordinates are the whole point
# of fetching recruits at all, so they are flattened into named columns rather
# than left as JSON that every reader would have to parse again.
HOMETOWN_FIELDS = [
    ("hometown_latitude", "latitude"),
    ("hometown_longitude", "longitude"),
    ("hometown_county_fips", "fipsCode"),
]
HOMETOWN_KEY = "hometownInfo"
PLAYER_KNOWN = {"year", HOMETOWN_KEY} | {key for _col, key in PLAYER_FIELDS}
HOMETOWN_KNOWN = {key for _col, key in HOMETOWN_FIELDS}
PLAYER_CSV = ["season_year"] + [col for col, _key in PLAYER_FIELDS] \
    + [col for col, _key in HOMETOWN_FIELDS]


def unknown_keys(payload, known):
    """Keys the endpoint returned that this file has no column for."""
    seen = set()
    for item in payload or []:
        if isinstance(item, dict):
            seen |= set(item)
    return sorted(seen - known)


def unknown_hometown_keys(payload):
    seen = set()
    for item in payload or []:
        nested = (item or {}).get(HOMETOWN_KEY) if isinstance(item, dict) else None
        if isinstance(nested, dict):
            seen |= set(nested)
    return sorted(seen - HOMETOWN_KNOWN)


def _check_season(item, year, what):
    season = item.get("year")
    if season is not None and int(season) != year:
        raise ValueError(
            f"{year}: CFBD labelled a {what} row season {season}. "
            "A recruiting class belongs to the season it signs INTO; "
            "do not silently renumber it.")


def team_rows(payload, year):
    """One row per team's recruiting class. A metric not reported is blank."""
    missing = unknown_keys(payload, TEAM_KNOWN)
    if missing:
        raise ValueError(
            f"{year}: /recruiting/teams returned field(s) with no column here: "
            f"{', '.join(missing)}. Add them to TEAM_FIELDS rather than dropping them.")
    rows = []
    for item in payload or []:
        if not isinstance(item, dict):
            raise ValueError(f"{year}: recruiting-team entry is not an object")
        if not item.get("team"):
            raise ValueError(f"{year}: recruiting-team entry has no team")
        _check_season(item, year, "recruiting-team")
        row = {"season_year": year}
        for col, key in TEAM_FIELDS:
            value = item.get(key)
            row[col] = "" if value is None else value
        rows.append(row)
    if len({r["team"] for r in rows}) != len(rows):
        raise ValueError(f"{year}: duplicate team in recruiting classes")
    return rows


def player_rows(payload, year):
    """One row per recruit, hometown flattened.

    A recruit with no `committedTo` is kept: an uncommitted or unsigned recruit
    is a real row, and dropping them would quietly change what a class average
    means for everyone else.
    """
    missing = unknown_keys(payload, PLAYER_KNOWN)
    if missing:
        raise ValueError(
            f"{year}: /recruiting/players returned field(s) with no column here: "
            f"{', '.join(missing)}. Add them to PLAYER_FIELDS rather than dropping them.")
    nested = unknown_hometown_keys(payload)
    if nested:
        raise ValueError(
            f"{year}: hometownInfo returned field(s) with no column here: "
            f"{', '.join(nested)}. Add them to HOMETOWN_FIELDS rather than dropping them.")
    rows = []
    for item in payload or []:
        if not isinstance(item, dict):
            raise ValueError(f"{year}: recruit entry is not an object")
        _check_season(item, year, "recruit")
        row = {"season_year": year}
        for col, key in PLAYER_FIELDS:
            value = item.get(key)
            row[col] = "" if value is None else value
        hometown = item.get(HOMETOWN_KEY) or {}
        if not isinstance(hometown, dict):
            raise ValueError(f"{year}: hometownInfo is not an object")
        for col, key in HOMETOWN_FIELDS:
            value = hometown.get(key)
            row[col] = "" if value is None else value
        rows.append(row)
    return rows


def fetch(path, year, headers):
    return cfbd_http.get_json(
        f"{BASE}{path}", params={"year": year},
        headers=headers, timeout=60, describe=f"GET {path} {year}")


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_season(year, headers, out_dir=None):
    """Write both of a season's CSVs; return the paths that were written.

    A season the endpoints cover but have no rows for writes NO file, rather
    than a header-only one: an empty file reads downstream as "nobody recruited
    anybody", which is the same shape as a real answer.

    `out_dir` is resolved HERE and not in the signature. A default binds at
    definition time, so `out_dir=OUT_DIR` could not be monkeypatched: a test
    patching the module attribute would pass while the code wrote to the real
    data/raw. That trap has bitten this repository four times.
    """
    out_dir = OUT_DIR if out_dir is None else Path(out_dir)
    written = []
    teams = team_rows(fetch("/recruiting/teams", year, headers), year)
    if teams:
        written.append(write_csv(out_dir / f"recruiting_teams_{year}.csv",
                                 TEAM_CSV, teams))
    players = player_rows(fetch("/recruiting/players", year, headers), year)
    if players:
        written.append(write_csv(out_dir / f"recruiting_players_{year}.csv",
                                 PLAYER_CSV, players))
    if not written:
        print(f"{year}: CFBD returned no recruiting data; no file written.")
        return written
    located = sum(1 for r in players if r["hometown_latitude"] != "")
    print(f"Wrote {len(teams)} class(es) and {len(players)} recruit(s) for {year} "
          f"({located} with hometown coordinates)")
    return written


def probe(year, headers):
    """Print what the endpoints actually return for one season, writing nothing."""
    for path, known in (("/recruiting/teams", TEAM_KNOWN),
                        ("/recruiting/players", PLAYER_KNOWN)):
        payload = fetch(path, year, headers)
        count = len(payload or [])
        print(f"--- {year} {path}: {count} row(s)")
        if not count:
            continue
        keys = sorted({key for item in payload if isinstance(item, dict) for key in item})
        print(f"keys: {keys}")
        print(f"keys with no column here: {unknown_keys(payload, known) or 'none'}")
        if path.endswith("players"):
            print(f"hometownInfo keys with no column here: "
                  f"{unknown_hometown_keys(payload) or 'none'}")
        print("sample: " + json.dumps(payload[0], sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("start", type=int)
    parser.add_argument("end", type=int, nargs="?")
    parser.add_argument("--probe", action="store_true",
                        help="print the endpoints' own schema and row counts; write nothing")
    args = parser.parse_args(argv)
    key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {key}"}
    end = args.end if args.end is not None else args.start
    if args.start < FIRST_SEASON:
        print(f"ERROR: CFBD recruiting cannot predate {FIRST_SEASON}.")
        return 2
    for year in range(args.start, end + 1):
        if args.probe:
            probe(year, headers)
        else:
            write_season(year, headers)
    return 0


if __name__ == "__main__":
    sys.exit(main())
