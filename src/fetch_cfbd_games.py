import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

BASE = "https://api.collegefootballdata.com"  # CFBD API base
OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def to_bool01(x):
    # CFBD sometimes returns bool, int, or None
    if x is True:
        return 1
    if x is False or x is None:
        return 0
    try:
        return 1 if int(x) > 0 else 0
    except Exception:
        return 0


def classify_game(g: dict, season_type_val: str) -> str:


    season_type_val = (season_type_val or "").lower()

    # --- First: trust API playoff flag ---
    is_playoff_flag = pick(g, "playoff", "is_playoff", "isPlayoff", default=False)
    if to_bool01(is_playoff_flag) == 1:
        return "playoff"

    # --- Second: fallback detection using notes ---
    notes = (pick(g, "notes", default="") or "").lower()

    if season_type_val == "postseason":
        if (
            "semifinal" in notes
            or "national championship" in notes
            or "college football playoff" in notes
            or "cfp" in notes
        ):
            return "playoff"

    return "regular"


def main(year: int):
    api_key = os.getenv("CFBD_API_KEY") or os.getenv("COLLEGEFOOTBALLDATA_API_KEY")
    scheme = os.getenv("CFBD_AUTH_SCHEME", "Bearer")

    headers = {}
    if api_key:
        headers["Authorization"] = f"{scheme} {api_key}"

    all_games = []
    for season_type in ("regular", "postseason"):
        params = {
            "year": year,
            "seasonType": season_type,
            "division": "fbs",
        }
        r = requests.get(f"{BASE}/games", params=params, headers=headers, timeout=60)
        r.raise_for_status()
        all_games.extend(r.json())

    out_path = OUT_DIR / f"games_{year}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "season_year",
                "week",
                "date",
                "season_type",
                "home_team",
                "away_team",
                "home_score",
                "away_score",
                "went_ot",
                "game_type",
                "neutral_site",
                "notes",
            ],
        )
        w.writeheader()

        for g in all_games:
            home = pick(g, "home_team", "homeTeam")
            away = pick(g, "away_team", "awayTeam")
            home_pts = pick(g, "home_points", "homePoints")
            away_pts = pick(g, "away_points", "awayPoints")

            # Skip games without scores (fine for historical 2014+)
            if home is None or away is None or home_pts is None or away_pts is None:
                continue

            # Date formats vary; normalize to YYYY-MM-DD
            start_date = pick(g, "start_date", "startDate")
            if start_date:
                date_str = str(start_date)[:10]
                datetime.strptime(date_str, "%Y-%m-%d")  # validate
            else:
                date_str = f"{year}-01-01"

            week = pick(g, "week")
            week_val = int(week) if week is not None and str(week).isdigit() else ""

            ot_raw = pick(g, "overtime", "overtimes", "overTime", default=0)
            went_ot = 1 if (str(ot_raw).isdigit() and int(ot_raw) > 0) else to_bool01(ot_raw)

            neutral_site = to_bool01(pick(g, "neutral_site", "neutralSite", default=False))

            game_id = pick(g, "id", "game_id", "gameId")
            notes = pick(g, "notes", default="") or ""

            # Use the season_type loop value so we don't depend on CFBD field naming quirks
            game_type = classify_game(g, season_type)

            season_type_val = (pick(g, "season_type", "seasonType", default="regular") or "regular").lower()
            game_type = classify_game(g, season_type_val)


            w.writerow(
                {
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
                    "neutral_site": neutral_site,
                    "notes": notes,
                }
            )

    print(f"Wrote {out_path} ({out_path.stat().st_size} bytes)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/fetch_cfbd_games.py 2014")
        raise SystemExit(2)
    main(int(sys.argv[1]))