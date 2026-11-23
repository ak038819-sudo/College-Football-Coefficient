#!/usr/bin/env python3
"""
The conventional team box score for every game, from CFBD's /games/teams.

WHAT THIS IS FOR. A game page here can tell you each player's line and each
team's efficiency -- success rate, PPA, explosiveness, all of it in
`game_team_advanced`. It cannot tell you the four numbers anyone actually
reads off a box score first: total yards, first downs, third-down
conversions, turnovers. Those are not derivable from what is stored. Player
receiving yards do not sum to team passing yards (sacks, laterals, and
yardage on plays with no receiving credit all break it), and no table here
holds a first down, a penalty, or time of possession at all. The NFL pages
show these; the college pages do not, and this is the missing input.

It is INPUT DATA ONLY. Nothing reads these files until src/load_game_team_stats.py
is wired into the pipeline, so landing this changes no published number.

  /games/teams -> data/raw/game_team_stats_<year>.csv

THE SHAPE, AND WHY IT IS STORED LONG. CFBD returns, per game, a list of two
teams, each with a list of {category, stat} pairs. Every `stat` is a STRING,
and what the string means depends on the category: "417" is a number,
"31:45" is a clock, "6-14" is made-of-attempted, and "5-45" is a count and a
yardage. There is no honest wide schema for that, because a new category
would need a new column and a parse rule invented on the spot. So one row
per (game, team, category) goes to disk exactly as the endpoint said it, and
the parsing into named numbers happens once, later, where it can be tested.
That keeps the archive lossless and the guesswork in one place.

ONE REQUEST PER WEEK. /games/teams will not answer for a year alone -- it
returns `400 Bad Request` unless one of `week`, `team` or `conference` comes
with it -- so a season is fetched week by week, and the weeks come from the
season's own committed games file rather than from a guessed range.

UNKNOWN FIELDS FAIL THE RUN. A key outside the known set stops the run and
names itself, because this repository's recurring fault is the silent drop.
An unfamiliar CATEGORY does not fail: categories are data, not schema, and
CFBD adds them season by season (`--probe` lists the ones a season uses).

Usage:
    python src/fetch_cfbd_game_team_stats.py --probe 2024
    python src/fetch_cfbd_game_team_stats.py 2001 2026
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
# Where box scores begin for this archive. `games` starts in 1869, but
# /games/teams has nothing like that reach. MEASURED, not assumed: the
# 2001-2025 backfill of 2026-11-23 asked for every week of 2001, 2002 and
# 2003 and the endpoint returned nothing for any of them, then 699 games for
# 2004. The floor moved from 2001 to 2004 so a re-run does not spend three
# seasons of requests re-establishing that.
FIRST_SEASON = 2004

GAME_KNOWN = {"id", "teams"}
# (CSV column, key in CFBD's team object).
TEAM_FIELDS = [
    ("team_id", "teamId"),
    ("school", "team"),
    ("conference", "conference"),
    ("home_away", "homeAway"),
    ("points", "points"),
]
TEAM_KNOWN = {"stats"} | {key for _col, key in TEAM_FIELDS}
STAT_KNOWN = {"category", "stat"}
CSV_COLUMNS = ["season_year", "game_id"] + [col for col, _key in TEAM_FIELDS] \
    + ["category", "stat"]


def _unknown(seen, known, what, year):
    missing = sorted(seen - known)
    if missing:
        raise ValueError(
            f"{year}: /games/teams returned {what} field(s) with no column here: "
            f"{', '.join(missing)}. Add them rather than dropping them.")


def stat_rows(payload, year):
    """One row per (game, team, category), the value left exactly as given.

    A team with an empty `stats` list still yields nothing, but the absence is
    then visible as a game with no categories rather than as a game that is
    missing -- the loader distinguishes the two.
    """
    seen_game = set()
    seen_team = set()
    seen_stat = set()
    for game in payload or []:
        if not isinstance(game, dict):
            raise ValueError(f"{year}: /games/teams entry is not an object")
        seen_game |= set(game)
        for side in game.get("teams") or []:
            if not isinstance(side, dict):
                raise ValueError(f"{year}: team entry is not an object")
            seen_team |= set(side)
            for stat in side.get("stats") or []:
                if not isinstance(stat, dict):
                    raise ValueError(f"{year}: stat entry is not an object")
                seen_stat |= set(stat)
    _unknown(seen_game, GAME_KNOWN, "game", year)
    _unknown(seen_team, TEAM_KNOWN, "team", year)
    _unknown(seen_stat, STAT_KNOWN, "stat", year)

    rows = []
    for game in payload or []:
        game_id = game.get("id")
        if game_id is None:
            raise ValueError(f"{year}: a /games/teams entry has no game id")
        for side in game.get("teams") or []:
            if side.get("teamId") is None:
                raise ValueError(f"{year}: game {game_id} has a team with no teamId")
            base = {"season_year": year, "game_id": game_id}
            for col, key in TEAM_FIELDS:
                value = side.get(key)
                # A blank is a blank. 0 points is a real score and "" is
                # "not reported"; writing 0 for the second would invent one.
                base[col] = "" if value is None else value
            for stat in side.get("stats") or []:
                category = stat.get("category")
                if not category:
                    raise ValueError(
                        f"{year}: game {game_id} has a stat with no category")
                value = stat.get("stat")
                rows.append(dict(base, category=category,
                                 stat="" if value is None else str(value)))
    seen = set()
    for row in rows:
        key = (row["game_id"], row["team_id"], row["category"])
        if key in seen:
            raise ValueError(
                f"{year}: duplicate category {row['category']} for team "
                f"{row['team_id']} in game {row['game_id']}")
        seen.add(key)
    return rows


def fetch(year, season_type, week, headers):
    """One week. A whole season cannot be asked for: /games/teams answers
    `400 Bad Request` for year alone, because one of `week`, `team` or
    `conference` is required alongside it."""
    return cfbd_http.get_json(
        f"{BASE}/games/teams",
        params={"year": year, "seasonType": season_type, "week": week},
        headers=headers, timeout=120,
        describe=f"GET /games/teams {year} {season_type} week {week}")


def season_weeks(year, season_type, raw_dir=None):
    """The weeks this season actually played, read from the committed games
    file rather than guessed.

    WHY NOT A RANGE. The endpoint has to be asked week by week, and a blind
    `range(1, 16)` is a guess that is wrong in both directions: 2001 played no
    week 4 at all (the September 11 cancellations) and ran through week 16, so
    a fixed range either wastes requests or drops a week in silence -- this
    repository's recurring fault. The archive already knows the answer.

    `raw_dir` is resolved HERE and not in the signature, so a test can patch
    OUT_DIR and have it take effect."""
    path = (OUT_DIR if raw_dir is None else Path(raw_dir)) / f"games_{year}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{year}: {path} is missing, so the weeks to request are unknown. "
            "Fetch the season's games first.")
    weeks = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if (row.get("season_type") or "").strip() != season_type:
                continue
            week = (row.get("week") or "").strip()
            if not week:
                raise ValueError(
                    f"{year}: game {row.get('game_id')} has no week, so its "
                    "box score could not be requested")
            weeks.add(int(week))
    return sorted(weeks)


def season_rows(year, headers, raw_dir=None):
    """Both halves of a season, week by week. The postseason is its own set of
    requests, and leaving it out would drop every bowl and playoff game in
    silence."""
    payload = []
    asked = 0
    for season_type in ("regular", "postseason"):
        for week in season_weeks(year, season_type, raw_dir):
            asked += 1
            payload.extend(fetch(year, season_type, week, headers) or [])
    if not asked:
        raise ValueError(
            f"{year}: the games file names no week, so nothing was requested")
    return stat_rows(payload, year)


def path_for(year, out_dir=None):
    """`out_dir` is resolved HERE and not in the signature: a default binds at
    definition time, so `out_dir=OUT_DIR` could not be monkeypatched and a test
    would pass while the code wrote into the real data/raw."""
    return (OUT_DIR if out_dir is None else Path(out_dir)) / f"game_team_stats_{year}.csv"


def write_season(year, headers, out_dir=None, force=False, raw_dir=None):
    """Returns the number of rows written; 0 means no file was written."""
    path = path_for(year, out_dir)
    if path.exists() and not force:
        print(f"{year}: already committed, skipping ({path})")
        return 0
    rows = season_rows(year, headers, raw_dir)
    if not rows:
        # Never truncate: an empty file would read downstream as a season in
        # which no team gained a yard, which is the shape of a real answer.
        print(f"{year}: the endpoint has no team box score; no file written")
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    games = len({row["game_id"] for row in rows})
    categories = len({row["category"] for row in rows})
    print(f"{year}: {len(rows)} rows, {games} games, {categories} categories -> {path}")
    return len(rows)


def probe(year, headers):
    """What the endpoint actually returns, written nowhere. This is how the
    category list is meant to be established rather than assumed."""
    payload = []
    for season_type in ("regular", "postseason"):
        for week in season_weeks(year, season_type):
            chunk = fetch(year, season_type, week, headers) or []
            print(f"--- {year} {season_type} week {week}: {len(chunk)} game(s)")
            payload.extend(chunk)
    if not payload:
        return
    categories = {}
    for game in payload:
        for side in game.get("teams") or []:
            for stat in side.get("stats") or []:
                categories.setdefault(stat.get("category"), stat.get("stat"))
    for name in sorted(categories, key=lambda n: (n is None, n)):
        print(f"  {name}: e.g. {categories[name]!r}")
    print("sample game: " + json.dumps(payload[0], sort_keys=True)[:600])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("start", type=int)
    parser.add_argument("end", type=int, nargs="?")
    parser.add_argument("--probe", action="store_true",
                        help="print the endpoint's categories and counts; write nothing")
    parser.add_argument("--force", action="store_true",
                        help="refetch a season whose file is already committed")
    args = parser.parse_args(argv)
    # Before the key check, so a mistyped year is answered the same way with or
    # without credentials.
    if args.start < FIRST_SEASON:
        print(f"ERROR: CFBD team box scores do not predate {FIRST_SEASON}.")
        return 2
    key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {key}"}
    end = args.end if args.end is not None else args.start
    for year in range(args.start, end + 1):
        if args.probe:
            probe(year, headers)
        else:
            write_season(year, headers, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
