#!/usr/bin/env python3
"""
Drive-by-drive data from CFBD's /drives: what happened, and WHEN.

Two things need this, and neither can be done without it:

1. A game page that tells the story of the game. Today a finished game shows a
   final score, a line score and box-score lines -- nothing that says the
   visitors scored twice in four minutes to take the lead. A drive list, and
   the scoring drives pulled out of it, is that story in the data's own terms.

2. The live win-probability model's time curve. `src/win_probability.py`
   shrinks its spread by `fraction_remaining ** time_exponent`, and 0.5 -- a
   plain random walk -- is the only parameter in that model that is assumed
   rather than measured, because fitting it needs the score at known moments
   DURING games. Every drive here ends at a known period and clock with a known
   score, which is exactly that.

Each drive row therefore carries the clock and the score at both ends of the
drive. The scores are CFBD's running totals (offense/defense perspective);
`load_drives.py` turns them into home/away using `is_home_offense`, so nothing
downstream has to remember which side had the ball.

Coverage: CFBD's drive data comes from play-by-play, which begins in 2001.
Earlier seasons are skipped without calling the API.

A season is written as one gzipped CSV, `data/raw/drives/<year>.csv.gz`:
about 20,000 drives a season, which is small gzipped and never read except by
the loader.

Usage:
    python src/fetch_cfbd_drives.py 2026          # one season
    python src/fetch_cfbd_drives.py 2001 2026     # a range, oldest first

Needs CFBD_API_KEY in the environment -- the same key every other fetcher uses.
"""
from __future__ import annotations

import csv
import gzip
import os
import sys
from pathlib import Path

import cfbd_http

BASE = "https://api.collegefootballdata.com"
OUT_DIR = Path("data/raw/drives")
FIRST_SEASON = 2001
# CFBD splits a season into these; a bowl game's drives are only in the second.
SEASON_TYPES = ("regular", "postseason")

CSV_FIELDS = [
    "season_year", "season_type", "week", "game_id", "drive_id", "drive_number",
    "offense", "defense", "is_home_offense", "scoring",
    "start_period", "start_clock", "end_period", "end_clock",
    "start_yards_to_goal", "end_yards_to_goal", "plays", "yards", "drive_result",
    "start_offense_score", "start_defense_score", "end_offense_score", "end_defense_score",
]


def pick(obj: dict, *names, default=None):
    """CFBD has served both camelCase and snake_case for these fields over the
    years, and a fetcher that knows only one silently writes blanks."""
    for name in names:
        if isinstance(obj, dict) and obj.get(name) is not None:
            return obj[name]
    return default


def clock_seconds(value) -> int | None:
    """Seconds left in the PERIOD, from CFBD's clock in any shape it has used.

    `{"minutes": 12, "seconds": 7}` is the documented one. A bare number is
    read as seconds, and "12:07" as minutes and seconds. Anything else is None,
    which is a drive whose timing cannot be used rather than a drive timed at
    zero -- the difference matters, because zero is a real and common clock.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        minutes = value.get("minutes", value.get("minute"))
        seconds = value.get("seconds", value.get("second"))
        if minutes is None and seconds is None:
            return None
        try:
            total = int(minutes or 0) * 60 + int(seconds or 0)
        except (TypeError, ValueError):
            return None
        return total if total >= 0 else None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value) if value >= 0 else None
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
        parts = text.split(":")
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            minutes, seconds = (int(p) for p in parts)
            return minutes * 60 + seconds if seconds <= 59 else None
    return None


def _int_or_blank(value):
    if value is None or isinstance(value, bool):
        return ""
    try:
        return int(value)
    except (TypeError, ValueError):
        return ""


def drive_rows(payload, year: int, season_type: str) -> tuple[list[dict], int]:
    """Flatten CFBD's drive objects. Returns (rows, dropped).

    A drive is dropped only when it cannot be attached to a game or ordered
    within it -- no game id, or no drive number. Everything else is kept with
    blanks where the source said nothing, because a drive with an unreadable
    clock is still a drive that happened, and the loader is where a consumer
    decides whether it needs the timing.
    """
    rows, dropped = [], 0
    for drive in payload or []:
        if not isinstance(drive, dict):
            dropped += 1
            continue
        game_id = pick(drive, "gameId", "game_id")
        number = pick(drive, "driveNumber", "drive_number")
        if game_id is None or number is None:
            dropped += 1
            continue
        home_offense = pick(drive, "isHomeOffense", "is_home_offense")
        rows.append({
            "season_year": pick(drive, "season", default=year),
            "season_type": season_type,
            "week": _int_or_blank(pick(drive, "week")),
            "game_id": _int_or_blank(game_id),
            "drive_id": _int_or_blank(pick(drive, "id", "driveId", "drive_id")),
            "drive_number": _int_or_blank(number),
            "offense": pick(drive, "offense", default=""),
            "defense": pick(drive, "defense", default=""),
            # Blank, not 0: "CFBD did not say which side was home" is not the
            # same claim as "the away team had the ball".
            "is_home_offense": "" if home_offense is None else int(bool(home_offense)),
            "scoring": "" if pick(drive, "scoring") is None else int(bool(drive.get("scoring"))),
            "start_period": _int_or_blank(pick(drive, "startPeriod", "start_period")),
            "start_clock": _blank_if_none(clock_seconds(pick(drive, "startTime", "start_time"))),
            "end_period": _int_or_blank(pick(drive, "endPeriod", "end_period")),
            "end_clock": _blank_if_none(clock_seconds(pick(drive, "endTime", "end_time"))),
            "start_yards_to_goal": _int_or_blank(pick(drive, "startYardsToGoal", "start_yards_to_goal")),
            "end_yards_to_goal": _int_or_blank(pick(drive, "endYardsToGoal", "end_yards_to_goal")),
            "plays": _int_or_blank(pick(drive, "plays")),
            "yards": _int_or_blank(pick(drive, "yards")),
            "drive_result": str(pick(drive, "driveResult", "drive_result", default=""))[:60],
            "start_offense_score": _int_or_blank(pick(drive, "startOffenseScore", "start_offense_score")),
            "start_defense_score": _int_or_blank(pick(drive, "startDefenseScore", "start_defense_score")),
            "end_offense_score": _int_or_blank(pick(drive, "endOffenseScore", "end_offense_score")),
            "end_defense_score": _int_or_blank(pick(drive, "endDefenseScore", "end_defense_score")),
        })
    return rows, dropped


def _blank_if_none(value):
    return "" if value is None else value


def fetch_drives(year: int, headers: dict) -> list[dict] | None:
    """Both season types for one year, or None when nothing could be fetched.

    A failure on one season type is not allowed to publish a half season as if
    it were whole: the regular season is what almost every drive is in, so a
    failure there returns None and keeps whatever file already exists, while a
    postseason failure only warns -- a season still being played has no
    postseason to fetch and must not be treated as broken.
    """
    if year < FIRST_SEASON:
        print(f"{year}: CFBD drive data begins in {FIRST_SEASON}; skipped.")
        return None
    rows: list[dict] = []
    for season_type in SEASON_TYPES:
        try:
            payload = cfbd_http.get_json(
                f"{BASE}/drives",
                params={"year": year, "seasonType": season_type},
                headers=headers, timeout=180,
                describe=f"GET /drives {year} {season_type}")
        except Exception as exc:  # noqa: BLE001 -- the reason is printed, not swallowed
            if season_type == "regular":
                print(f"WARNING: could not fetch {year} regular-season drives ({exc}); "
                      "keeping any existing file.")
                return None
            print(f"WARNING: could not fetch {year} {season_type} drives ({exc}); "
                  "regular season kept.")
            continue
        part, dropped = drive_rows(payload, year, season_type)
        if dropped:
            print(f"  {year} {season_type}: dropped {dropped} drives with no game or drive number")
        rows.extend(part)
    return rows


def write_drives(year: int, headers: dict, out_dir: Path = OUT_DIR) -> Path | None:
    rows = fetch_drives(year, headers)
    if rows is None:
        return None
    if not rows:
        print(f"{year}: CFBD returned no drives; keeping any existing file.")
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{year}.csv.gz"
    with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    games = len({r["game_id"] for r in rows})
    timed = sum(1 for r in rows if r["end_period"] != "" and r["end_clock"] != "")
    print(f"Wrote {path} ({len(rows)} drives across {games} games, {timed} with an end clock)")
    return path


def main(argv=None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    key = os.environ.get("CFBD_API_KEY")
    if not key:
        sys.exit("CFBD_API_KEY is not set. Export it, or run this where the repository secret is available.")
    if not argv:
        sys.exit(__doc__)
    start = int(argv[0])
    end = int(argv[1]) if len(argv) > 1 else start
    headers = {"Authorization": f"Bearer {key}"}
    failed = []
    for year in range(start, end + 1):
        if write_drives(year, headers) is None and year >= FIRST_SEASON:
            failed.append(year)
    if failed:
        # A backfill must stop at the first season it could not write rather
        # than reporting success with a hole in the archive.
        sys.exit(f"Failed to write drives for: {', '.join(str(y) for y in failed)}")


if __name__ == "__main__":
    main()
