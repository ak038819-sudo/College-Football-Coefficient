#!/usr/bin/env python3
"""Archive CFBD's head-coaching history to data/raw/coaches/coaches.json.

CFBD's /coaches feed is one object per coach carrying every season of that
coach's head-coaching record, so this is a single whole-history snapshot rather
than per-season files. It is fetched in year slices only because the endpoint
caps a response, and merged back into one coach-per-object file.

Head coaches ONLY. The feed carries no coordinator or assistant history, and
nothing downstream may present it as though it did.

The feed also carries no coach identifier -- see load_coaches.py for the
deterministic composite key the loader derives, and why it is stored as an
external id rather than assumed in code.

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
    """One object per coach, seasons unioned across the year slices fetched.

    Each year slice returns a coach's WHOLE career, so the same person appears
    once per slice that covers them, with overlapping season lists. That overlap
    is the signal used here: records sharing a name and hire date are merged only
    when their seasons actually intersect.

    Grouping on name and hire date ALONE would be wrong, and quietly so. Two
    different coaches can share a name and have no hire date at all (CFBD often
    omits it for older seasons); merging them would hand one coach_id two
    unrelated careers, and because the loader would then see a single record its
    collision handling would never run -- the blend would be permanent and
    undetectable. Disjoint careers under one name therefore stay separate
    records, and load_coaches.py separates them as a recorded collision.
    """
    groups: Dict[tuple, List[dict]] = {}
    for record in records:
        groups.setdefault((record["name"], record["hire_date"] or ""), []).append(record)

    out: List[dict] = []
    for (name, _hire), group in groups.items():
        careers: List[dict] = []
        for record in group:
            seasons = {(s["year"], s["school"]) for s in record["seasons"]}
            # Any existing career this record shares a season with is the same
            # person. It can touch more than one, so they are folded together.
            touching = [c for c in careers if c["keys"] & seasons]
            if not touching:
                careers.append({"record": {**record, "seasons": list(record["seasons"])},
                                "keys": set(seasons)})
                continue
            first = touching[0]
            for other in touching[1:]:
                for season in other["record"]["seasons"]:
                    if (season["year"], season["school"]) not in first["keys"]:
                        first["record"]["seasons"].append(season)
                        first["keys"].add((season["year"], season["school"]))
                careers.remove(other)
            for season in record["seasons"]:
                if (season["year"], season["school"]) not in first["keys"]:
                    first["record"]["seasons"].append(season)
                    first["keys"].add((season["year"], season["school"]))
        out.extend(career["record"] for career in careers)

    for record in out:
        record["seasons"].sort(key=lambda s: (s["year"], s["school"]))
    out.sort(key=lambda r: (r["name"], r["hire_date"] or "",
                            r["seasons"][0]["year"] if r["seasons"] else 0))
    return out


def fetch_coaches(start: int, end: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """Every coach across the year range, or None if ANY slice failed.

    All-or-nothing on purpose: a partial pull would drop coaches rather than
    merely delay them, and overwriting the snapshot with it would silently erase
    careers from the archive.
    """
    import requests
    records: List[dict] = []
    for year in range(start, end + 1):
        try:
            r = requests.get(f"{BASE}/coaches", params={"year": year}, headers=headers, timeout=120)
            r.raise_for_status()
            records += coach_records(r.json())
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
