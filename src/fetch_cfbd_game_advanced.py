#!/usr/bin/env python3
"""
PER-GAME advanced team stats from CFBD's /stats/game/advanced (EXP-03).

Why this exists next to fetch_cfbd_advanced.py: that one fetches SEASON totals,
which are useless for the xSRDiff layer. A season's Success Rate includes the
game being rated and every game after it, so using it to value a game would
leak the future into a rating the model is supposed to have formed before
kickoff. xSRDiff needs the Success Rate of THAT game, which is what this
endpoint returns.

Unlike the season file, this IS model input: build_elo.py reads it through
game_team_advanced when the performance layer is xsrdiff or raw_srdiff. It is
still only ever read for a game that has already been played.

- CFBD's play-by-play-based stats begin in 2001; earlier seasons are skipped
  without calling the API, and those games take the Elo fallback path.
- Garbage time is excluded, matching the season fetcher's convention, and
  recorded in the CSV so downstream code can say so.
- A metric CFBD doesn't report is written blank, never 0.
- A failed request keeps any existing file and only warns.

Usage:
    python src/fetch_cfbd_game_advanced.py 2001 2026    # backfill a range once
    python src/fetch_cfbd_game_advanced.py 2026         # just one season

Needs CFBD_API_KEY in the environment (the same key the season fetcher uses).
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

# (csv column, side, path inside that side's object) against CFBD's
# /stats/game/advanced schema (offense/defense objects, camelCase).
FIELDS = [
    ("off_plays", "offense", ("plays",)),
    ("off_ppa", "offense", ("ppa",)),
    ("off_success_rate", "offense", ("successRate",)),
    ("off_explosiveness", "offense", ("explosiveness",)),
    ("def_plays", "defense", ("plays",)),
    ("def_ppa", "defense", ("ppa",)),
    ("def_success_rate", "defense", ("successRate",)),
    ("def_explosiveness", "defense", ("explosiveness",)),
]
CSV_FIELDS = ["season_year", "game_id", "week", "team", "opponent",
              "garbage_time_excluded"] + [f[0] for f in FIELDS]


def _dig(obj, path):
    for key in path:
        if not isinstance(obj, dict) or obj.get(key) is None:
            return None
        obj = obj[key]
    return obj


def game_advanced_rows(payload, year: int) -> List[dict]:
    """
    Flatten CFBD's per-team-per-game objects. Missing metrics -> "" (not
    reported), never 0. A row with no gameId is dropped: without it the stat
    cannot be attached to a game, and guessing by team+week would silently
    mis-attribute it.
    """
    rows = []
    for t in payload or []:
        team = t.get("team") or t.get("school")
        game_id = t.get("gameId") or t.get("game_id")
        if not team or game_id is None:
            continue
        row = {"season_year": t.get("season") or year, "game_id": game_id,
               "week": t.get("week") if t.get("week") is not None else "",
               "team": team, "opponent": t.get("opponent") or "",
               "garbage_time_excluded": 1}
        for col, side, path in FIELDS:
            v = _dig(t.get(side), path)
            row[col] = "" if v is None else v
        rows.append(row)
    return rows


def fetch_game_advanced(year: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """None if the season predates coverage or the request failed; otherwise the rows."""
    if year < FIRST_SEASON:
        print(f"{year}: CFBD advanced stats begin in {FIRST_SEASON}; skipped.")
        return None
    # Imported here, not at the top, for the same reason as the season fetcher:
    # load_game_advanced.py reuses FIELDS, and loading a CSV must never require
    # the HTTP library.
    import requests
    try:
        r = requests.get(f"{BASE}/stats/game/advanced",
                         params={"year": year, "excludeGarbageTime": "true", "classification": "fbs"},
                         headers=headers, timeout=120)
        r.raise_for_status()
        return game_advanced_rows(r.json(), year)
    except Exception as e:
        print(f"WARNING: could not fetch {year} game advanced stats ({e}); keeping any existing file.")
        return None


def write_game_advanced(year: int, headers: Dict[str, str], out_dir: Path = OUT_DIR) -> Optional[Path]:
    rows = fetch_game_advanced(year, headers)
    if rows is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"game_advanced_{year}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    games = len({r["game_id"] for r in rows})
    print(f"Wrote {path} ({len(rows)} team-games across {games} games, garbage time excluded)")
    return path


def main() -> None:
    key = os.environ.get("CFBD_API_KEY")
    if not key:
        sys.exit("CFBD_API_KEY is not set. Export it, or run this where the repository secret is available.")
    headers = {"Authorization": f"Bearer {key}"}
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    start = int(args[0])
    end = int(args[1]) if len(args) > 1 else start
    for year in range(start, end + 1):
        write_game_advanced(year, headers)


if __name__ == "__main__":
    main()
