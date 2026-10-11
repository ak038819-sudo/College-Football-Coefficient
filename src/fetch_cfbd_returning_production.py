#!/usr/bin/env python3
"""
Returning production per team-season, from CFBD's /player/returning.

WHAT THIS IS FOR. The Elo offseason is currently one constant for every
program in the country:

    new = initial_rating + offseason_retention * (old - initial_rating)

with `offseason_retention` = 0.8 in config/model_config.json. A team that
returns its quarterback, its line and its secondary regresses toward average by
exactly as much as one that lost twenty starters. This file supplies the input
that could make that rate a team's own rather than everyone's.

It is INPUT DATA ONLY. Nothing reads these files until a fit has been scored
out of sample and the config says to use it, so landing this changes no
published number.

THE SEASON CONVENTION, which is the thing most likely to be got wrong.
CFBD's `year` is the season production returns INTO: /player/returning?year=2024
answers "how much of 2023's production is back for 2024". That is exactly the
season whose offseason transition would consume it, so the CSV keeps CFBD's
own numbering and calls the column `season_year`. A fit must use
returning_production(Y) for the transition INTO season Y and never Y-1, which
would be the previous offseason's answer.

UNKNOWN FIELDS FAIL THE RUN. This repository's recurring fault is a silent
drop -- a bare lookup that `continue`s past what it does not recognise (see
docs/identity-problems.md and team_identity.resolve_database_name). A metric
CFBD adds later is not something to discard quietly, so a key outside KNOWN
below stops the run and names itself. `--probe` prints what the endpoint
actually returns without writing anything, which is how the field list below
was established rather than assumed.

Usage:
    python src/fetch_cfbd_returning_production.py --probe 2024
    python src/fetch_cfbd_returning_production.py 2014 2026
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
# PPA-based returning production needs play-by-play, which CFBD has from 2001;
# the endpoint's own coverage is narrower and the probe reports where it really
# starts. FIRST_SEASON is a floor on what to ask for, not a claim about what
# exists -- a season the endpoint has nothing for writes no file.
FIRST_SEASON = 2001

# (CSV column, key in CFBD's response). Established by --probe, not from memory.
FIELDS = [
    ("total_ppa", "totalPPA"),
    ("total_passing_ppa", "totalPassingPPA"),
    ("total_receiving_ppa", "totalReceivingPPA"),
    ("total_rushing_ppa", "totalRushingPPA"),
    ("percent_ppa", "percentPPA"),
    ("percent_passing_ppa", "percentPassingPPA"),
    ("percent_receiving_ppa", "percentReceivingPPA"),
    ("percent_rushing_ppa", "percentRushingPPA"),
    ("usage", "usage"),
    ("passing_usage", "passingUsage"),
    ("receiving_usage", "receivingUsage"),
    ("rushing_usage", "rushingUsage"),
]
IDENTITY = ("season", "team", "conference")
KNOWN = set(IDENTITY) | {key for _col, key in FIELDS}
CSV_FIELDS = ["season_year", "team", "conference"] + [col for col, _key in FIELDS]


def unknown_keys(payload):
    """Keys the endpoint returned that this file has no column for."""
    seen = set()
    for item in payload or []:
        if isinstance(item, dict):
            seen |= set(item)
    return sorted(seen - KNOWN)


def returning_rows(payload, year):
    """One row per team. A metric CFBD does not report is blank, never 0.

    Blank matters here more than usual: 0% returning production is a real and
    occasionally true value, so writing 0 for "not reported" would feed a fit
    a measurement nobody made.
    """
    missing = unknown_keys(payload)
    if missing:
        raise ValueError(
            f"{year}: CFBD returned field(s) this fetcher has no column for: "
            f"{', '.join(missing)}. Add them to FIELDS rather than dropping them.")
    rows = []
    for item in payload or []:
        if not isinstance(item, dict):
            raise ValueError(f"{year}: returning-production entry is not an object")
        team = item.get("team")
        if not team:
            raise ValueError(f"{year}: returning-production entry has no team")
        season = item.get("season")
        if season is not None and int(season) != year:
            raise ValueError(
                f"{year}: CFBD labelled a returning-production row season {season}. "
                "The season convention decides which offseason this feeds; "
                "do not silently renumber it.")
        row = {"season_year": year, "team": team, "conference": item.get("conference") or ""}
        for col, key in FIELDS:
            value = item.get(key)
            row[col] = "" if value is None else value
        rows.append(row)
    if len({r["team"] for r in rows}) != len(rows):
        raise ValueError(f"{year}: duplicate team in returning production")
    return rows


def fetch_returning(year, headers):
    return cfbd_http.get_json(
        f"{BASE}/player/returning", params={"year": year},
        headers=headers, timeout=60, describe=f"GET /player/returning {year}")


def write_returning(year, headers, out_dir=None):
    """Write one season's CSV; return the path, or None when CFBD has no rows.

    A season the endpoint covers but has no data for writes NO file, rather
    than a header-only one: an empty file reads downstream as "every team
    returned nothing", which is the same shape as a real answer.

    `out_dir` is resolved HERE and not in the signature. A default binds at
    definition time, so `out_dir=OUT_DIR` could not be monkeypatched: a test
    patching the module attribute would pass while the code wrote to the real
    data/raw. That trap has now bitten this repository four times.
    """
    out_dir = OUT_DIR if out_dir is None else Path(out_dir)
    payload = fetch_returning(year, headers)
    rows = returning_rows(payload, year)
    if not rows:
        print(f"{year}: CFBD returned no returning production; no file written.")
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"returning_production_{year}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    reported = sum(1 for r in rows if r["percent_ppa"] != "")
    print(f"Wrote {path} ({len(rows)} teams, {reported} with percent_ppa)")
    return path


def probe(year, headers):
    """Print what the endpoint actually returns for one season, writing nothing."""
    payload = fetch_returning(year, headers)
    count = len(payload or [])
    print(f"--- {year}: {count} row(s)")
    if not count:
        return
    keys = sorted({key for item in payload if isinstance(item, dict) for key in item})
    print(f"keys: {keys}")
    missing = unknown_keys(payload)
    print(f"keys with no column here: {missing or 'none'}")
    print("sample: " + json.dumps(payload[0], sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("start", type=int)
    parser.add_argument("end", type=int, nargs="?")
    parser.add_argument("--probe", action="store_true",
                        help="print the endpoint's own schema and row counts; write nothing")
    args = parser.parse_args(argv)
    key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {key}"}
    end = args.end if args.end is not None else args.start
    if args.start < FIRST_SEASON:
        print(f"ERROR: PPA-based returning production cannot predate {FIRST_SEASON}.")
        return 2
    for year in range(args.start, end + 1):
        if args.probe:
            probe(year, headers)
        else:
            write_returning(year, headers)
    return 0


if __name__ == "__main__":
    sys.exit(main())
