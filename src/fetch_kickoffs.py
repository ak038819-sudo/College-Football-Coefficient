#!/usr/bin/env python3
"""
Kickoff times for every FBS game (display only).

games.game_date is the UTC date of CFBD's start time, so a Saturday 10:30 PM
Eastern kickoff is stored as Sunday. That column also orders games for Elo, so
it is NOT changed. Instead this keeps CFBD's full start time per game in its
own file, data/raw/kickoffs_<year>.csv, loaded into game_kickoffs, which only
the website reads (dates are shown as US Eastern, like scheduled games).

Deliberately separate from fetch_cfbd_games.py: backfilling kickoff times must
never re-download game RESULTS, which could pull in CFBD corrections to old
scores and quietly change the ratings.

- Same requests as the game fetch: /games per season, regular + postseason, FBS.
- A failed request keeps any existing file and only warns.
- `requests` is imported only when downloading (loading needs no HTTP library).

Usage:
    python src/fetch_kickoffs.py 1980 2026    # one-time backfill
    python src/fetch_kickoffs.py 2026         # one season
(fetch_cfbd_games.py also refreshes the season it fetches.)
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw")
CSV_FIELDS = ["game_id", "season_year", "kickoff_utc", "start_time_tbd"]


def _pick(d: dict, *keys):
    for k in keys:
        if d.get(k) is not None:
            return d[k]
    return None


def kickoff_rows(payload, year: int) -> List[dict]:
    rows = []
    for g in payload or []:
        gid = _pick(g, "id", "game_id", "gameId")
        start = _pick(g, "start_date", "startDate")
        if gid is None or not start:
            continue
        tbd = _pick(g, "start_time_tbd", "startTimeTBD")
        rows.append({"game_id": int(gid), "season_year": year, "kickoff_utc": str(start),
                     "start_time_tbd": 1 if tbd in (True, 1, "true", "True", "1") else 0})
    return rows


def fetch_kickoffs(year: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """All of a season's kickoffs, or None if any request failed (so nothing is overwritten)."""
    import requests
    rows = []
    try:
        for season_type in ("regular", "postseason"):
            r = requests.get(f"{BASE}/games", params={"year": year, "seasonType": season_type, "division": "fbs"},
                             headers=headers, timeout=60)
            r.raise_for_status()
            rows += kickoff_rows(r.json(), year)
    except Exception as e:  # display-only data: never let it break anything else
        print(f"WARNING: could not fetch {year} kickoff times ({e}); keeping any existing file.")
        return None
    return rows


def write_kickoffs(year: int, headers: Dict[str, str], out_dir: Path = OUT_DIR) -> Optional[Path]:
    rows = fetch_kickoffs(year, headers)
    if rows is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"kickoffs_{year}.csv"
    rows.sort(key=lambda r: r["game_id"])
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    known = sum(1 for r in rows if not r["start_time_tbd"])
    print(f"Wrote {path} ({len(rows)} games, {known} with a known kickoff time)")
    return path


def main(argv: List[str]) -> int:
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not api_key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    years = [int(a) for a in argv if a.isdigit()]
    if not years or len(years) > 2:
        print("Usage: fetch_kickoffs.py START_YEAR [END_YEAR]")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {api_key}"}
    for year in range(years[0], (years[1] if len(years) == 2 else years[0]) + 1):
        write_kickoffs(year, headers)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
