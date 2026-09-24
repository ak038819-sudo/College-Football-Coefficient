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

RANKINGS_FIELDS = ["season_year", "season_type", "week", "poll", "rank", "school", "conference",
                   "first_place_votes", "points"]


def poll_key(poll_name: str) -> Optional[str]:
    """Normalize CFBD poll names to 'ap' / 'cfp'; every other poll (Coaches, FCS, ...) -> None."""
    n = (poll_name or "").strip().lower()
    if n == "ap top 25":
        return "ap"
    if ("playoff" in n and "committee" in n) or n in ("cfp", "college football playoff"):
        return "cfp"
    return None


def _blank_if_none(v):
    return "" if v is None else v


def rankings_rows(payload, year: int) -> List[dict]:
    """Flatten a CFBD /rankings payload into AP + CFP rows (Milestone 5). Display data only."""
    rows = []
    for wobj in (payload if isinstance(payload, list) else [payload] if payload else []):
        season_type = (pick(wobj, "seasonType", "season_type", default="regular") or "regular").strip().lower()
        week = pick(wobj, "week")
        for poll in pick(wobj, "polls", default=[]) or []:
            key = poll_key(pick(poll, "poll", default=""))
            if key is None or week is None:
                continue
            for r in pick(poll, "ranks", default=[]) or []:
                school = pick(r, "school", "team", "name")
                if pick(r, "rank") is None or not school:
                    continue
                rows.append({
                    "season_year": year, "season_type": season_type, "week": int(week), "poll": key,
                    "rank": int(pick(r, "rank")), "school": school,
                    "conference": pick(r, "conference", default="") or "",
                    # None -> blank (not reported, e.g. the CFP poll); a real 0 stays 0.
                    "first_place_votes": _blank_if_none(pick(r, "firstPlaceVotes", "first_place_votes")),
                    "points": _blank_if_none(pick(r, "points")),
                })
    return rows


def fetch_rankings(year: int, headers: Dict[str, str]) -> Optional[List[dict]]:
    """Every AP + CFP release for the season (regular + postseason). None if the request failed."""
    rows = []
    try:
        for season_type in ("regular", "postseason"):
            r = requests.get(f"{BASE}/rankings", params={"year": year, "seasonType": season_type},
                             headers=headers, timeout=60)
            r.raise_for_status()
            rows.extend(rankings_rows(r.json(), year))
    except Exception as e:   # rankings are display-only; never let them break the games fetch
        print(f"WARNING: could not fetch {year} rankings ({e}); keeping any existing rankings file.")
        return None
    return rows


def compute_went_ot(g: dict) -> int:
    """
    CFBD's /games endpoint has NO top-level overtime boolean field at
    all (confirmed against the authoritative API schema,
    api.collegefootballdata.com/api/games -- the Game object's only
    relevant fields are homeLineScores/awayLineScores). A previous
    version searched for "overtime"/"overtimes"/"overTime", which don't
    exist on this endpoint -- went_ot was silently 0 for every game in
    the entire dataset as a result (a real, previously undiscovered
    gap, caught while building an Elo model that actually depended on
    this field). The correct signal: a regulation game's line-score
    array has exactly 4 entries (one per quarter); each overtime period
    adds one more entry.
    """
    line_scores = pick(g, "home_line_scores", "homeLineScores", default=None)
    if line_scores is None:
        line_scores = pick(g, "away_line_scores", "awayLineScores", default=None)
    return 1 if (isinstance(line_scores, list) and len(line_scores) > 4) else 0


def is_completed(g: dict) -> bool:
    """
    A game is final only if both scores exist AND CFBD doesn't mark it as
    unfinished. CFBD's `completed` flag is False for games in progress, which
    can already carry partial scores -- a fetch run during a Saturday must
    never turn a halftime score into a final result in the games table (and
    from there into Elo/CoE). Older payloads without the flag fall back to
    "both scores present", which is how every historical game was classified.
    """
    if pick(g, "home_points", "homePoints") is None or pick(g, "away_points", "awayPoints") is None:
        return False
    return g.get("completed") is not False


SCHEDULE_FIELDS = [
    "game_id", "season_year", "week", "kickoff_utc", "start_time_tbd", "season_type",
    "home_team", "away_team", "neutral_site", "game_phase", "notes",
]


def schedule_row(g: dict, year: int, cfp_top4) -> dict:
    """A not-yet-final game for data/raw/schedule_<year>.csv. Deliberately has no score fields."""
    season_type_val = (pick(g, "season_type", "seasonType", default="regular") or "regular").strip().lower()
    week_val = pick(g, "week")
    return {
        "game_id": pick(g, "id", "game_id", "gameId"),
        "season_year": year,
        "week": "" if week_val is None else str(week_val),
        "kickoff_utc": pick(g, "start_date", "startDate", default="") or "",
        "start_time_tbd": to_bool01(pick(g, "start_time_tbd", "startTimeTBD", default=False)),
        "season_type": season_type_val,
        "home_team": pick(g, "home_team", "homeTeam"),
        "away_team": pick(g, "away_team", "awayTeam"),
        "neutral_site": to_bool01(pick(g, "neutral_site", "neutralSite", default=False)),
        "game_phase": game_phase(season_type_val, classify_game(g, season_type_val, cfp_top4)),
        "notes": pick(g, "notes", default="") or "",
    }


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

    scheduled: List[dict] = []
    out_path = OUT_DIR / f"games_{year}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "season_year", "week", "date",
                "season_type",
                "home_team", "away_team",
                "home_conference", "away_conference",
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

            if home is None or away is None:
                continue
            # Not final yet (no scores, or CFBD says in progress): goes to the
            # schedule file instead, never the games file.
            if not is_completed(g):
                scheduled.append(schedule_row(g, year, cfp_top4))
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

            went_ot = compute_went_ot(g)

            # Per-game conference, per the authoritative /games schema
            # (home_conference/away_conference) -- this is the SAME field
            # source that home_line_scores/away_line_scores came from for
            # the went_ot fix, and it's been sitting in every game we've
            # already fetched this whole time. Lets team-conference
            # membership be DERIVED directly from real historical games
            # instead of needing a separate membership data source --
            # potentially extending real membership data back to 1980
            # (currently bounded at 2014, the earliest year we had an
            # explicit membership source for). None when CFBD doesn't
            # have it for a given historical game (independents, or gaps
            # in older data).
            home_conf = pick(g, "home_conference", "homeConference", default=None)
            away_conf = pick(g, "away_conference", "awayConference", default=None)

            neutral_site = to_bool01(pick(g, "neutral_site", "neutralSite", default=False))

            w.writerow({
                "game_id": game_id,
                "season_year": year,
                "week": week_val,
                "date": date_str,
                "season_type": season_type_val,
                "home_team": home,
                "away_team": away,
                "home_conference": home_conf or "",
                "away_conference": away_conf or "",
                "home_score": int(home_pts),
                "away_score": int(away_pts),
                "went_ot": went_ot,
                "game_type": game_type,
                "game_phase": phase,
                "neutral_site": neutral_site,
                "notes": notes,
            })


    print(f"Wrote {out_path} ({out_path.stat().st_size} bytes)")

    # Always written (even when empty) so a finished season's stale schedule gets cleared.
    sched_path = OUT_DIR / f"schedule_{year}.csv"
    with sched_path.open("w", newline="", encoding="utf-8") as f:
        sw = csv.DictWriter(f, fieldnames=SCHEDULE_FIELDS)
        sw.writeheader()
        sw.writerows(scheduled)
    print(f"Wrote {sched_path} ({len(scheduled)} not-yet-final FBS games)")

    # Full AP + CFP polls (Milestone 5). Only written when the request succeeded,
    # so a network hiccup can't replace good data with an empty file.
    ranking_rows = fetch_rankings(year, headers)
    if ranking_rows is not None:
        rank_path = OUT_DIR / f"rankings_{year}.csv"
        with rank_path.open("w", newline="", encoding="utf-8") as f:
            rw = csv.DictWriter(f, fieldnames=RANKINGS_FIELDS)
            rw.writeheader()
            rw.writerows(ranking_rows)
        releases = {(r["poll"], r["season_type"], r["week"]) for r in ranking_rows}
        print(f"Wrote {rank_path} ({len(ranking_rows)} rows across {len(releases)} AP/CFP releases)")

    # Advanced season stats for the same season (Milestone 7): display-only;
    # failures only warn. Imported here to keep the two scripts independent.
    # Set CFBD_FETCH_ADVANCED=0 to skip (the scheduled refresh reads this from
    # the repository variable FETCH_ADVANCED_STATS).
    if os.getenv("CFBD_FETCH_ADVANCED", "1").strip() != "0":
        import fetch_cfbd_advanced
        fetch_cfbd_advanced.write_advanced(year, headers, OUT_DIR)
    else:
        print("Advanced stats fetch skipped (CFBD_FETCH_ADVANCED=0).")
    if cfp_top4:
        pretty = ", ".join(sorted(cfp_top4))
        print(f"CFP Top-4 (normalized) used for fallback: {pretty}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/fetch_cfbd_games.py 2016")
        raise SystemExit(2)
    raise SystemExit(main(int(sys.argv[1])))
