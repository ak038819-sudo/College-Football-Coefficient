#!/usr/bin/env python3
"""
Fetch FBS games from CollegeFootballData (CFBD) and write a normalized CSV.

Key features:
- Pulls BOTH regular + postseason games for a given year (division=fbs)
- Writes a clean CSV with season_type coming from the API per-game (not the loop var)
- Classifies CFP playoff games using:
    1) CFBD's playoff flag when available (preferred)
    2) Fallback for the 4-team CFP era: if postseason AND both teams are in the final CFP Top-4
       (helps for years where notes/playoff flags are inconsistent)
"""

import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Small utilities
# ----------------------------
def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def to_bool01(x) -> int:
    if x is True:
        return 1
    if x is False or x is None:
        return 0
    try:
        return 1 if int(x) > 0 else 0
    except Exception:
        return 0


def norm_team(name: Optional[str]) -> str:
    """Normalize team name for matching across endpoints."""
    if not name:
        return ""
    s = name.strip().lower()
    # collapse whitespace
    s = re.sub(r"\s+", " ", s)
    # remove common punctuation
    s = re.sub(r"[^\w\s&-]", "", s)
    return s

def is_fbs_game(g: dict) -> bool:
    """
    CFBD sometimes leaks non-FBS postseason games even when division=fbs is requested.
    We filter them out defensively.
    """
    # Best-case: explicit classification fields exist
    hc = (pick(g, "homeClassification", "home_classification", default="") or "").strip().lower()
    ac = (pick(g, "awayClassification", "away_classification", default="") or "").strip().lower()
    if hc or ac:
        return hc == "fbs" and ac == "fbs"

    # Fallback: filter by notes text (works when classification isn't present)
    notes = (pick(g, "notes", default="") or "").lower()
    if "fcs" in notes or "championship subdivision" in notes:
        return False
    if "division ii" in notes or "division iii" in notes:
        return False

    return True


def parse_date_yyyy_mm_dd(start_date) -> str:
    if start_date:
        date_str = str(start_date)[:10]
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    return "1900-01-01"


# ----------------------------
# CFP Top-4 fetch (fallback classifier helper)
# ----------------------------
def extract_top4_from_rankings_payload(payload: list) -> Optional[Set[str]]:
    """
    CFBD /rankings payload shape (typical):
    [
      {
        "season": 2016,
        "seasonType": "regular",
        "week": 14,
        "polls": [
          { "poll": "Playoff Committee Rankings", "ranks": [ { "rank": 1, "school": "Alabama", ... }, ... ] },
          ...
        ]
      }
    ]
    But CFBD has varied slightly over time; we search polls and extract ranks 1-4.
    Returns set of normalized team names.
    """
    if not payload:
        return None

    # Sometimes it's a list of week objects; sometimes a single object.
    week_objs = payload if isinstance(payload, list) else [payload]

    # Prefer the last item (highest week) if multiple came back
    # (we also call by explicit week, so usually 1 item).
    for wobj in reversed(week_objs):
        polls = pick(wobj, "polls", default=[]) or []
        for poll in polls:
            poll_name = (pick(poll, "poll", default="") or "").strip().lower()
            if "playoff" in poll_name and "committee" in poll_name:
                ranks = pick(poll, "ranks", default=[]) or []
                top4 = []
                for r in ranks:
                    rank_num = pick(r, "rank")
                    school = pick(r, "school", "team", "name")
                    if rank_num in (1, 2, 3, 4) and school:
                        top4.append(norm_team(str(school)))
                if len(top4) == 4:
                    return set(top4)

            # Some payloads might label the poll as just "CFP"
            if poll_name in ("cfp", "college football playoff"):
                ranks = pick(poll, "ranks", default=[]) or []
                top4 = []
                for r in ranks:
                    rank_num = pick(r, "rank")
                    school = pick(r, "school", "team", "name")
                    if rank_num in (1, 2, 3, 4) and school:
                        top4.append(norm_team(str(school)))
                if len(top4) == 4:
                    return set(top4)

    return None


def fetch_final_cfp_top4(year: int, headers: Dict[str, str]) -> Optional[Set[str]]:
    """
    Fetch the final CFP Top-4 for a season.

    Strategy:
    - CFP rankings are typically published late regular season.
    - We probe weeks from 16 down to 10 (buffer) with seasonType=regular, poll=cfp if supported.
    - We stop at the first week where we can extract ranks 1-4 from the CFP poll.
    """
    # Keep this range wide enough across seasons; harmless to probe a few.
    for week in range(16, 9, -1):
        params = {"year": year, "seasonType": "regular", "week": week}
        try:
            r = requests.get(f"{BASE}/rankings", params=params, headers=headers, timeout=60)
            # If key missing, CFBD may 401; raise to surface clearly.
            r.raise_for_status()
            payload = r.json()
            top4 = extract_top4_from_rankings_payload(payload)
            if top4 and len(top4) == 4:
                return top4
        except requests.HTTPError:
            # If unauthorized or other HTTP error, bubble up later in main
            raise
        except Exception:
            # Week may not have CFP poll; keep probing.
            continue

    return None

def game_phase(season_type_val: str, game_type: str) -> str:
    if season_type_val == "regular":
        return "regular"
    if game_type == "playoff":
        return "cfp"
    return "bowl"


# -------------------------------------------------
# Game classification
# -------------------------------------------------
def classify_game(g: dict, season_type_val: str, cfp_top4: set | None = None) -> str:
    season_type_val = (season_type_val or "regular").lower()

    # 1) Trust CFBD API playoff flag (fixes 2024+ first round)
    is_playoff_flag = pick(g, "playoff", "is_playoff", "isPlayoff", default=False)
    if season_type_val == "postseason" and to_bool01(is_playoff_flag) == 1:
        return "playoff"

    # Normalize fields
    notes = (pick(g, "notes", default="") or "").lower()
    home = (pick(g, "home_team", "homeTeam") or "").strip().lower()
    away = (pick(g, "away_team", "awayTeam") or "").strip().lower()

    if season_type_val == "postseason":
        # 2) Notes-based detection (most seasons)
        if (
            "semifinal" in notes
            or "national championship" in notes
            or "college football playoff" in notes
            or "cfp" in notes
        ):
            return "playoff"

        # 3) Top-4 fallback (fixes 2015/2016)
        if cfp_top4 and home in cfp_top4 and away in cfp_top4:
            return "playoff"

    return "regular"




# ----------------------------
# Main
# ----------------------------
def main(year: int) -> int:
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    scheme = os.getenv("CFBD_AUTH_SCHEME", "Bearer")

    headers: Dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"{scheme} {api_key}"

    # Helpful error if user forgot to set the key.
    if not api_key:
        print("ERROR: Missing CFBD API key. Set CFBD_API_KEY in your environment.")
        print("Example: export CFBD_API_KEY='YOUR_KEY_HERE'")
        return 2

    # Fetch CFP Top-4 once (used only as a fallback classifier for 2014–2023)
    cfp_top4 = None
    if 2014 <= year <= 2023:
        cfp_top4 = fetch_final_cfp_top4(year, headers)
        if not cfp_top4:
            # Not fatal; just means fallback won't be applied.
            print(f"WARNING: Could not find final CFP Top-4 for {year}. Fallback playoff detection disabled.")

    all_games: List[dict] = []
    for season_type in ("regular", "postseason"):
        params = {"year": year, "seasonType": season_type, "division": "fbs"}
        r = requests.get(f"{BASE}/games", params=params, headers=headers, timeout=60)
        r.raise_for_status()
        all_games.extend(r.json())

    out_path = OUT_DIR / f"games_{year}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "season_year", "week", "date",
                "season_type",
                "home_team", "away_team",
                "home_score", "away_score",
                "went_ot",
                "game_type",
                "game_phase",      # ✅ add this
                "neutral_site",
                "notes",
            ],
        )

        w.writeheader()

        for g in all_games:
            if not is_fbs_game(g):
                continue

            game_id = pick(g, "id", "game_id", "gameId")
            home = pick(g, "home_team", "homeTeam")
            away = pick(g, "away_team", "awayTeam")
            home_pts = pick(g, "home_points", "homePoints")
            away_pts = pick(g, "away_points", "awayPoints")

            # Skip games without scores (rare for historical, common for future schedules)
            if home is None or away is None or home_pts is None or away_pts is None:
                continue

            start_date = pick(g, "start_date", "startDate")
            date_str = parse_date_yyyy_mm_dd(start_date)

            week_val = pick(g, "week")
            week_val = "" if week_val is None else str(week_val)

            # IMPORTANT: season_type should come from the per-game object
            season_type_val = (pick(g, "season_type", "seasonType", default="regular") or "regular").strip().lower()

            notes = pick(g, "notes", default="") or ""

            # classify game_type first
            game_type = classify_game(g, season_type_val, cfp_top4)

            # THEN derive phase
            phase = game_phase(season_type_val, game_type)

            ot_raw = pick(g, "overtime", "overtimes", "overTime", default=0)
            went_ot = 1 if (str(ot_raw).isdigit() and int(ot_raw) > 0) else to_bool01(ot_raw)

            neutral_site = to_bool01(pick(g, "neutral_site", "neutralSite", default=False))

            w.writerow({
                "game_id": game_id,
                "season_year": year,
                "week": week_val,
                "date": date_str,
                "season_type": season_type_val,
                "home_team": home,
                "away_team": away,
                "home_score": int(home_pts),
                "away_score": int(away_pts),
                "went_ot": went_ot,
                "game_type": game_type,
                "game_phase": phase,
                "neutral_site": neutral_site,
                "notes": notes,
            })


    print(f"Wrote {out_path} ({out_path.stat().st_size} bytes)")
    if cfp_top4:
        pretty = ", ".join(sorted(cfp_top4))
        print(f"CFP Top-4 (normalized) used for fallback: {pretty}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/fetch_cfbd_games.py 2016")
        raise SystemExit(2)
    raise SystemExit(main(int(sys.argv[1])))
