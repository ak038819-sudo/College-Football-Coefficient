#!/usr/bin/env python3
"""
Season-level advanced team stats from CFBD's /stats/season/advanced (Milestone 7).

DISPLAY DATA ONLY. Stored separately from game results and never read by
Elo, CoE, the hybrid layer, standings, predictions or playoff selection.

- CFBD's play-by-play-based stats begin in 2001; earlier seasons are skipped
  without calling the API (the page shows N/A for them).
- Garbage time is excluded (the usual convention: blowout snaps distort
  efficiency). Recorded in the CSV so the page can say so.
- A metric CFBD doesn't report is written blank, never 0.
- A failed request keeps any existing file and only warns.

Usage:
    python src/fetch_cfbd_advanced.py 2001 2026     # backfill a range once
    python src/fetch_cfbd_advanced.py 2026          # just one season
(fetch_cfbd_games.py also calls this for the season it fetches.)
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw")
FIRST_SEASON = 2001

# (csv column, side, path inside that side's object). Names verified against
# CFBD's /stats/season/advanced schema (offense/defense objects, camelCase).
FIELDS = [
    ("off_plays", "offense", ("plays",)),
    ("off_ppa", "offense", ("ppa",)),
    ("off_success_rate", "offense", ("successRate",)),
    ("off_explosiveness", "offense", ("explosiveness",)),
    ("off_pts_per_opp", "offense", ("pointsPerOpportunity",)),
    ("off_line_yards", "offense", ("lineYards",)),
    ("off_stuff_rate", "offense", ("stuffRate",)),
    ("off_havoc", "offense", ("havoc", "total")),
    ("def_plays", "defense", ("plays",)),
    ("def_ppa", "defense", ("ppa",)),
    ("def_success_rate", "defense", ("successRate",)),
    ("def_explosiveness", "defense", ("explosiveness",)),
    ("def_pts_per_opp", "defense", ("pointsPerOpportunity",)),
    ("def_line_yards", "defense", ("lineYards",)),
    ("def_stuff_rate", "defense", ("stuffRate",)),
    ("def_havoc", "defense", ("havoc", "total")),
]
CSV_FIELDS = ["season_year", "team", "conference", "garbage_time_excluded"] + [f[0] for f in FIELDS]


def _dig(obj, path):
    for key in path:
        if not isinstance(obj, dict) or obj.get(key) is None:
            return None
        obj = obj[key]
    return obj


def advanced_rows(payload, year: int) -> List[dict]:
    """Flatten CFBD's nested per-team objects. Missing metrics -> "" (not reported), never 0."""
    rows = []
    for t in payload or []:
        team = t.get("team") or t.get("school")
        if not team:
            continue
        row = {"season_year": year, "team": team, "conference": t.get("conference") or "",
               "garbage_time_excluded": 1}
        for col, side, path in FIELDS:
            v = _dig(t.get(side), path)
            row[col] = "" if v is None else v
        rows.append(row)
    return rows


def fetch_advanced(year: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """None if the season predates coverage or the request failed; otherwise the rows."""
    if year < FIRST_SEASON:
        print(f"{year}: CFBD advanced stats begin in {FIRST_SEASON}; skipped.")
        return None
    # Imported here, not at the top: load_advanced.py reuses FIELDS from this module,
    # and loading a CSV must never require the HTTP library (CI's rebuild step
    # failed exactly that way before this was moved).
    import requests
    try:
        r = requests.get(f"{BASE}/stats/season/advanced",
                         params={"year": year, "excludeGarbageTime": "true", "classification": "fbs"},
                         headers=headers, timeout=60)
        r.raise_for_status()
        return advanced_rows(r.json(), year)
    except Exception as e:   # display-only data: never let it break anything else
        print(f"WARNING: could not fetch {year} advanced stats ({e}); keeping any existing file.")
        return None


def write_advanced(year: int, headers: Dict[str, str], out_dir: Path = OUT_DIR) -> Optional[Path]:
    rows = fetch_advanced(year, headers)
    if rows is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"advanced_{year}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {path} ({len(rows)} FBS teams, garbage time excluded)")
    return path


def main(argv: List[str]) -> int:
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not api_key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {api_key}"}
    if len(argv) not in (1, 2):
        print("Usage: fetch_cfbd_advanced.py START_YEAR [END_YEAR]")
        return 2
    start = int(argv[0])
    end = int(argv[1]) if len(argv) == 2 else start
    for year in range(start, end + 1):
        write_advanced(year, headers)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
