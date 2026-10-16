#!/usr/bin/env python3
"""
Betting lines from CFBD's /lines: what the market expected, game by game.

The site already says what Elo expected before every game since 1980. It has
never said what anyone else expected, which means the one question a reader
asks of a rating -- is it better than the number in the newspaper? -- cannot be
answered on it. The closing spread is the honest benchmark: it is the market's
own point estimate of the margin, set by people with money at stake, and it is
the hardest thing in sports forecasting to beat.

One row per game PER PROVIDER, because the providers disagree and averaging
them here would hide that. `load_lines.py` is where a reader picks one.

Sign convention: CFBD's `spread` is from the HOME team's point of view and is
NEGATIVE when the home team is favoured (-7 means home by 7). That is the
opposite sign to every margin in this database, so the loader stores a
`market_home_margin` of +7 alongside it and nothing downstream has to
remember. See sql/line_tables.sql.

Coverage: CFBD has lines from the mid-1980s for some games and near-complete
coverage from about 2013. There is no hard floor here -- a season that comes
back with nothing is a season with no lines, which is a fact about the source
and not a failure.

A season is written as one gzipped CSV, `data/raw/lines/<year>.csv.gz`.

Usage:
    python src/fetch_cfbd_lines.py 2024          # one season
    python src/fetch_cfbd_lines.py 2013 2026     # a range, oldest first

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
OUT_DIR = Path("data/raw/lines")
# A bowl game's line is only in the postseason payload, exactly as with drives.
SEASON_TYPES = ("regular", "postseason")

CSV_FIELDS = [
    "season_year", "season_type", "week", "game_id", "provider",
    "home_team", "away_team", "home_score", "away_score",
    "spread", "spread_open", "over_under", "over_under_open",
    "home_moneyline", "away_moneyline", "formatted_spread",
]


def pick(obj: dict, *names, default=None):
    """CFBD has served both camelCase and snake_case for these fields."""
    for name in names:
        if isinstance(obj, dict) and obj.get(name) is not None:
            return obj[name]
    return default


def _number(value):
    """A float, or "" -- never 0 for a missing line.

    A spread of 0 is a real line (a pick'em), so a blank that became 0 would
    read as "the market called this game even" on every game nobody priced.
    """
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return ""
    try:
        return float(text)
    except ValueError:
        return ""


def _int_or_blank(value):
    if value is None or isinstance(value, bool):
        return ""
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return ""


def line_rows(payload, year: int, season_type: str) -> tuple[list[dict], int]:
    """Flatten CFBD's per-game line objects. Returns (rows, dropped).

    A game is dropped only when it cannot be identified (no game id). A game
    CFBD lists with an empty `lines` array contributes no rows and is not a
    drop: plenty of games were never priced, and recording a blank row for each
    would turn "nobody posted a line" into a line of its own.
    """
    rows, dropped = [], 0
    for game in payload or []:
        if not isinstance(game, dict):
            dropped += 1
            continue
        game_id = pick(game, "id", "gameId", "game_id")
        if game_id is None:
            dropped += 1
            continue
        common = {
            "season_year": pick(game, "season", default=year),
            "season_type": pick(game, "seasonType", "season_type", default=season_type),
            "week": _int_or_blank(pick(game, "week")),
            "game_id": _int_or_blank(game_id),
            "home_team": pick(game, "homeTeam", "home_team", default=""),
            "away_team": pick(game, "awayTeam", "away_team", default=""),
            "home_score": _int_or_blank(pick(game, "homeScore", "home_score")),
            "away_score": _int_or_blank(pick(game, "awayScore", "away_score")),
        }
        for line in pick(game, "lines", default=[]) or []:
            if not isinstance(line, dict):
                dropped += 1
                continue
            rows.append({
                **common,
                "provider": str(pick(line, "provider", default=""))[:40],
                "spread": _number(pick(line, "spread")),
                "spread_open": _number(pick(line, "spreadOpen", "spread_open")),
                "over_under": _number(pick(line, "overUnder", "over_under")),
                "over_under_open": _number(pick(line, "overUnderOpen", "over_under_open")),
                "home_moneyline": _int_or_blank(pick(line, "homeMoneyline", "home_moneyline")),
                "away_moneyline": _int_or_blank(pick(line, "awayMoneyline", "away_moneyline")),
                "formatted_spread": str(pick(line, "formattedSpread", "formatted_spread",
                                             default=""))[:60],
            })
    return rows, dropped


def fetch_lines(year: int, headers: dict) -> list[dict] | None:
    """Both season types for one year, or None when either could not be fetched.

    None on EITHER failure, for the reason the drive fetcher does it: what this
    returns is written over the whole season file, so a postseason request that
    times out would otherwise replace a complete archive with one holding no
    bowl or playoff lines, and a backfill seeing a success would commit it.
    """
    rows: list[dict] = []
    for season_type in SEASON_TYPES:
        try:
            payload = cfbd_http.get_json(
                f"{BASE}/lines",
                params={"year": year, "seasonType": season_type},
                headers=headers, timeout=180,
                describe=f"GET /lines {year} {season_type}")
        except Exception as exc:  # noqa: BLE001 -- the reason is printed, not swallowed
            print(f"WARNING: could not fetch {year} {season_type} lines ({exc}); "
                  "keeping any existing file.")
            return None
        part, dropped = line_rows(payload, year, season_type)
        if dropped:
            print(f"  {year} {season_type}: dropped {dropped} entries with no game id")
        rows.extend(part)
    return rows


def write_lines(year: int, headers: dict, out_dir: Path = OUT_DIR) -> str:
    """Fetches and writes one season. Returns what happened, in one word.

    "failed" is the only one that should stop a backfill. "empty" is a season
    CFBD has no lines for, which is most of the 1980s and not an error, and it
    leaves any existing file alone rather than truncating it.
    """
    rows = fetch_lines(year, headers)
    if rows is None:
        return "failed"
    if not rows:
        print(f"{year}: CFBD has no lines for this season.")
        return "empty"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{year}.csv.gz"
    with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    games = len({r["game_id"] for r in rows})
    providers = sorted({r["provider"] for r in rows if r["provider"]})
    priced = sum(1 for r in rows if r["spread"] != "")
    print(f"Wrote {path} ({len(rows)} lines across {games} games, {priced} with a spread; "
          f"providers: {', '.join(providers) or 'none named'})")
    return "written"


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
    failed = [year for year in range(start, end + 1)
              if write_lines(year, headers) == "failed"]
    if failed:
        # A backfill must stop at the first season it could not fetch rather
        # than reporting success with a hole in the archive.
        sys.exit(f"Failed to fetch lines for: {', '.join(str(y) for y in failed)}")


if __name__ == "__main__":
    main()
