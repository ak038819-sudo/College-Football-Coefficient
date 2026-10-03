#!/usr/bin/env python3
"""Archive CFBD season player statistics, one JSON file per season.

Phase 4 of the person-pages work needs season totals a page can show and a
leaderboard can sort. The guide names this endpoint for it, and the reason is
worth stating: the committed box-score archive carries NO athlete id, so a stat
line there can only be attached to a person by matching a name. `/stats/player/
season` returns the athlete id with every row, which is the id this project now
uses as `player_id`, so a statistic attaches to a person because the source says
so rather than because two strings looked alike.

Deriving season totals by scanning the game archive instead would be both slower
and weaker: the archive covers 2004 onward but cannot identify anybody, and a
page would have to re-add thousands of rows on every build.

The feed is long-form -- one row per player per category per stat type -- so the
snapshot is kept in that shape. Reducing it here would mean deciding now which
statistics the pages will ever want, and a season re-fetched later would then
silently differ from one archived today.

Written gzipped, because keeping that shape is only affordable compressed: a
season is 27 MB of JSON and 1.07 MB gzipped, so eighteen seasons are 19 MB in a
checkout rather than 490 MB. The repository already carries a 237 MB box-score
archive, and this container and every CI run check the whole tree out.

Coverage: `category` is the feed's own grouping (passing, rushing, receiving,
defensive, kicking, punting, interceptions, fumbles, puntReturns, kickReturns).
Nothing here is a rate or an efficiency metric: those are the model's own work
and are not read from this feed.

Usage:
    python src/fetch_cfbd_player_season_stats.py 2026        # one season
    python src/fetch_cfbd_player_season_stats.py 2004 2026   # a backfill range
"""
from __future__ import annotations

import gzip
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw/player_season_stats")
# CFBD's season player stats begin with its play-by-play coverage. An earlier
# season answers with an empty list rather than an error, which would otherwise
# be archived as a real but empty season.
FIRST_SEASON = 2004


def _pick(d: dict, *keys):
    for k in keys:
        if d.get(k) is not None:
            return d[k]
    return None


def _as_number(value):
    """A stat as a number where it is one, and as the source's own text where it
    is not. Every row in the 2025 season is numeric and `COMPLETIONS` is its own
    stat type, so nothing yet needs this; it is here because coercing an
    unexpected value would invent a number the source never gave, and the older
    seasons have not been fetched."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def stat_rows(payload, year: int) -> List[dict]:
    """Source rows with their keys normalized and nothing else changed.

    A row with no athlete id is dropped. It cannot be attached to a person by id,
    and attaching a statistic to a name is exactly what this project refuses to
    do -- a page would rather show no statistics than one person's yards on
    another person's page.
    """
    if not isinstance(payload, list):
        raise ValueError("season player stats response must be a list")
    rows = []
    for s in payload:
        if not isinstance(s, dict):
            continue
        athlete_id = _pick(s, "playerId", "player_id", "athleteId", "athlete_id", "id")
        name = _pick(s, "player", "name", "athlete")
        category = _pick(s, "category", "statCategory")
        stat_type = _pick(s, "statType", "stat_type", "type")
        if athlete_id is None or not name or not category or not stat_type:
            continue
        rows.append({
            "athlete_id": str(athlete_id).strip(),
            "name": str(name).strip(),
            "team": str(_pick(s, "team", "school") or "").strip(),
            "conference": (str(_pick(s, "conference") or "").strip() or None),
            "season_year": year,
            "category": str(category).strip(),
            "stat_type": str(stat_type).strip(),
            "stat": _as_number(_pick(s, "stat", "value")),
        })
    return rows


def fetch_season(year: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """A season's stat rows, or None if the request failed (nothing is overwritten)."""
    import requests
    if year < FIRST_SEASON:
        print(f"WARNING: CFBD season player stats begin in {FIRST_SEASON}; skipping {year}.")
        return None
    try:
        r = requests.get(f"{BASE}/stats/player/season", params={"year": year},
                         headers=headers, timeout=300)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:  # a failed fetch keeps the committed snapshot
        print(f"WARNING: could not fetch {year} season player stats ({e}); "
              "keeping any existing snapshot.")
        return None
    rows = stat_rows(payload, year)
    if not rows:
        print(f"WARNING: CFBD returned no usable stat rows for {year} "
              f"({len(payload) if isinstance(payload, list) else '?'} raw rows); "
              "keeping any existing snapshot.")
        return None
    return rows


def write_season(year: int, headers: Dict[str, str], out_dir: Path = OUT_DIR) -> Optional[Path]:
    rows = fetch_season(year, headers)
    if rows is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{year}.json.gz"
    # Sorted so re-fetching an unchanged season writes an identical file and
    # produces no commit, which is what keeps the sync workflow quiet.
    rows.sort(key=lambda r: (r["team"], r["name"], r["athlete_id"],
                             r["category"], r["stat_type"]))
    # mtime=0 so re-fetching an unchanged season writes an identical file: gzip
    # stamps the time by default, which would make every run look like a change.
    with gzip.GzipFile(path, "wb", compresslevel=9, mtime=0) as fh:
        fh.write((json.dumps(rows, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    # A snapshot from before this was compressed would otherwise be loaded too,
    # and the pipeline would read the same season twice.
    legacy = out_dir / f"{year}.json"
    if legacy.exists():
        legacy.unlink()
    people = len({r["athlete_id"] for r in rows})
    categories = len({r["category"] for r in rows})
    print(f"Wrote {path} ({len(rows)} stat rows, {people} people, {categories} categories)")
    return path


def main(argv: List[str]) -> int:
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not api_key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    years = [int(a) for a in argv if a.lstrip("-").isdigit()]
    if not years or len(years) > 2:
        print("Usage: fetch_cfbd_player_season_stats.py START_YEAR [END_YEAR]")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {api_key}"}
    failed = []
    for year in range(years[0], (years[1] if len(years) == 2 else years[0]) + 1):
        if write_season(year, headers) is None and year >= FIRST_SEASON:
            failed.append(year)
    if failed:
        print(f"ERROR: no snapshot written for {', '.join(str(y) for y in failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
