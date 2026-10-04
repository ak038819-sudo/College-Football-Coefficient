#!/usr/bin/env python3
"""Archive CFBD's head-coaching history to data/raw/coaches/coaches.json.

CFBD's /coaches feed is one object per coach carrying every season of that
coach's head-coaching record, so this is a single whole-history snapshot rather
than per-season files. It is fetched in year slices only because the endpoint
caps a response, and merged back into one coach-per-object file.

Head coaches ONLY. The feed carries no coordinator or assistant history, and
nothing downstream may present it as though it did.

The feed carries no coach identifier, and its hire_date belongs to the JOB
rather than the person (one coach has one per school). The name is therefore
the only person-level signal available; see merge_records below and
load_coaches.py for how that is keyed and why the weakness is recorded rather
than hidden.

Usage:
    python src/fetch_cfbd_coaches.py              # 1980 to the current season
    python src/fetch_cfbd_coaches.py 2000 2026
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import cfbd_http

from person_identity import normalize_name

BASE = "https://api.collegefootballdata.com"
OUT_PATH = Path("data/raw/coaches/coaches.json")
FIRST_SEASON = 1980  # matches the project's earliest loaded games


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


def coach_records(payload) -> List[dict]:
    """Source objects reduced to name, hire date and season records."""
    if not isinstance(payload, list):
        raise ValueError("coaches response must be a list")
    out = []
    for c in payload:
        if not isinstance(c, dict):
            continue
        first = _pick(c, "firstName", "first_name")
        last = _pick(c, "lastName", "last_name")
        name = " ".join(str(part).strip() for part in (first, last) if part).strip()
        if not name:
            continue
        seasons = []
        for s in c.get("seasons") or []:
            if not isinstance(s, dict):
                continue
            year = _as_int(_pick(s, "year", "season"))
            school = str(_pick(s, "school", "team") or "").strip()
            if year is None or not school:
                continue
            seasons.append({
                "year": year,
                "school": school,
                "games": _as_int(_pick(s, "games")),
                "wins": _as_int(_pick(s, "wins")),
                "losses": _as_int(_pick(s, "losses")),
                "ties": _as_int(_pick(s, "ties")),
                "preseason_rank": _as_int(_pick(s, "preseasonRank", "preseason_rank")),
                "postseason_rank": _as_int(_pick(s, "postseasonRank", "postseason_rank")),
            })
        if not seasons:
            continue
        hire = _pick(c, "hireDate", "hire_date")
        out.append({"name": name,
                    "first_name": str(first).strip() if first else None,
                    "last_name": str(last).strip() if last else None,
                    "hire_date": str(hire) if hire else None,
                    "seasons": seasons})
    return out


def merge_records(records: List[dict]) -> List[dict]:
    """One object per PERSON, with hire date carried on each season.

    What the live feed actually returns, confirmed against a full 1980-2026 pull
    (5,714 records carrying 5,716 seasons between them):

      - Each record is ONE season, not a career. /coaches?year=N returns that
        year's row for every coach who worked it.
      - hire_date belongs to the JOB, not the person. Al Golden comes back with
        2005-12-08 for his five Temple seasons and 2010-12-12 for his five at
        Miami.

    So grouping on name AND hire date would split one person into one record per
    job, and grouping on overlapping seasons would split them into one record
    per season. The name is the only PERSON-level signal this feed carries, so
    that is what groups a career here -- and because a name is weak evidence,
    load_coaches.py records the resulting identity as name-only and flags the
    ambiguous ones for review rather than asserting them. Two coaches who truly
    share a name cannot be separated by this feed, and saying so is better than
    inventing a distinction.

    hire_date moves onto each season, which is where it is actually true.
    """
    people: Dict[str, dict] = {}
    for record in records:
        key = normalize_name(record["name"])
        if not key:
            continue
        person = people.setdefault(key, {"name": record["name"],
                                         "first_name": record.get("first_name"),
                                         "last_name": record.get("last_name"),
                                         "seasons": []})
        seen = {(s["year"], s["school"]) for s in person["seasons"]}
        for season in record["seasons"]:
            if (season["year"], season["school"]) in seen:
                continue
            person["seasons"].append({**season, "hire_date": record.get("hire_date")})
            seen.add((season["year"], season["school"]))
    out = list(people.values())
    for record in out:
        record["seasons"].sort(key=lambda s: (s["year"], s["school"]))
    out.sort(key=lambda r: normalize_name(r["name"]))
    return out


def fetch_coaches(start: int, end: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """Every coach across the year range, or None if ANY slice failed.

    All-or-nothing on purpose: a partial pull would drop coaches rather than
    merely delay them, and overwriting the snapshot with it would silently erase
    careers from the archive.
    """
    records: List[dict] = []
    for year in range(start, end + 1):
        try:
            records += coach_records(cfbd_http.get_json(
                f"{BASE}/coaches", params={"year": year}, headers=headers, timeout=120,
                describe=f"GET /coaches {year}"))
        except Exception as e:
            print(f"WARNING: could not fetch {year} coaches ({e}); keeping the existing snapshot.")
            return None
    if not records:
        print("WARNING: CFBD returned no coaches; keeping the existing snapshot.")
        return None
    return merge_records(records)


def write_coaches(start: int, end: int, headers: Dict[str, str],
                  out_path: Path = OUT_PATH) -> Optional[Path]:
    records = fetch_coaches(start, end, headers)
    if records is None:
        return None
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(records, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    seasons = sum(len(r["seasons"]) for r in records)
    print(f"Wrote {out_path} ({len(records)} head coaches, {seasons} coach-seasons)")
    return out_path


def main(argv: List[str]) -> int:
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    if not api_key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        return 2
    years = [int(a) for a in argv if a.isdigit()]
    start = years[0] if years else FIRST_SEASON
    end = years[1] if len(years) > 1 else (years[0] if years else dt.date.today().year)
    headers = {"Authorization": f"{os.getenv('CFBD_AUTH_SCHEME', 'Bearer')} {api_key}"}
    return 0 if write_coaches(start, end, headers) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
