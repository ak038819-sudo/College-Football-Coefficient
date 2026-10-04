#!/usr/bin/env python3
"""Archive CFBD roster snapshots by season, one JSON file per season.

Snapshot first, load second, deliberately: /roster is a LIVE view of a team's
current roster, so it changes under you. Committing the snapshot means a build is
reproducible from the repository, and a season can be re-loaded months later
without asking CFBD what it thinks today.

Fields are reduced to what the person pages need (identity, team, season metadata
and optional bio). CFBD's recruiting ids and geocoded hometown coordinates are
dropped: the pages do not use them, and they are the bulk of the payload.

Coverage: CFBD's published /roster schema starts at 2009. Asking for an earlier
season returns an empty roster rather than an error, so this refuses it outright
instead of writing an empty snapshot that would look like a real one.

Usage:
    python src/fetch_cfbd_rosters.py 2026           # one season
    python src/fetch_cfbd_rosters.py 2009 2026      # a backfill range
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import cfbd_http

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw/rosters")
FIRST_SEASON = 2009  # CFBD's published /roster coverage boundary.


def _pick(d: dict, *keys):
    for k in keys:
        if d.get(k) is not None:
            return d[k]
    return None


def _as_int(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def roster_rows(payload, year: int) -> List[dict]:
    """Source rows reduced to the fields the loader reads. A row with no name is
    dropped: it cannot become a person, and a blank-named person is worse than a
    missing one."""
    if not isinstance(payload, list):
        raise ValueError("roster response must be a list")
    rows = []
    for p in payload:
        if not isinstance(p, dict):
            continue
        first = _pick(p, "firstName", "first_name")
        last = _pick(p, "lastName", "last_name")
        name = " ".join(str(part).strip() for part in (first, last) if part).strip()
        if not name:
            continue
        rows.append({
            "athlete_id": (str(_pick(p, "id", "athleteId", "athlete_id"))
                           if _pick(p, "id", "athleteId", "athlete_id") is not None else None),
            "first_name": str(first).strip() if first else None,
            "last_name": str(last).strip() if last else None,
            "name": name,
            "team": str(_pick(p, "team", "school") or "").strip(),
            "season_year": year,
            "jersey": _as_int(_pick(p, "jersey")),
            "position": (str(_pick(p, "position") or "").strip() or None),
            "class_year": _pick(p, "year", "classYear", "class_year"),
            "height": _as_int(_pick(p, "height")),
            "weight": _as_int(_pick(p, "weight")),
            "hometown": (str(_pick(p, "homeCity", "home_city") or "").strip() or None),
            "home_state": (str(_pick(p, "homeState", "home_state") or "").strip() or None),
        })
    return rows


def fetch_season(year: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """A season's roster rows, or None if the request failed (nothing is overwritten)."""
    if year < FIRST_SEASON:
        print(f"WARNING: CFBD rosters begin in {FIRST_SEASON}; skipping {year}.")
        return None
    try:
        rows = roster_rows(cfbd_http.get_json(
            f"{BASE}/roster", params={"year": year}, headers=headers, timeout=180,
            describe=f"GET /roster {year}"), year)
    except Exception as e:  # a failed fetch keeps the committed snapshot
        print(f"WARNING: could not fetch the {year} roster ({e}); keeping any existing snapshot.")
        return None
    if not rows:
        print(f"WARNING: CFBD returned no roster rows for {year}; keeping any existing snapshot.")
        return None
    return rows


def write_season(year: int, headers: Dict[str, str], out_dir: Path = OUT_DIR) -> Optional[Path]:
    rows = fetch_season(year, headers)
    if rows is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{year}.json"
    # Sorted so re-fetching an unchanged season produces an identical file and no
    # commit, which is what makes the sync workflow quiet when nothing moved.
    rows.sort(key=lambda r: (r["team"], r["name"], r["athlete_id"] or ""))
    path.write_text(json.dumps(rows, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    with_id = sum(1 for r in rows if r["athlete_id"])
    print(f"Wrote {path} ({len(rows)} players, {with_id} with a CFBD athlete id)")
    return path


def main(argv: List[str]) -> int:
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not api_key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    years = [int(a) for a in argv if a.isdigit()]
    if not years or len(years) > 2:
        print("Usage: fetch_cfbd_rosters.py START_YEAR [END_YEAR]")
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
