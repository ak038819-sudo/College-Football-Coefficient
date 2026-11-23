#!/usr/bin/env python3
"""Load data/raw/game_team_stats_<year>.csv into game_team_stats.

WHAT THIS IS FOR. The game page can show each player's line and each team's
efficiency, and not one of the four numbers a reader looks for first: total
yards, first downs, third-down conversions, turnovers. This is the table
those come from.

THE TWO SKIP RULES, both reported and neither silent:

  - A game_id the `games` table does not carry is skipped. The same rule as
    load_game_advanced: a stat that cannot be tied to a real game is worse
    than no stat, because it would be counted against nothing.
  - A school name this database cannot resolve is skipped. /games/teams
    covers games against FCS opponents, so an unresolvable name is usually a
    real school that is simply not carried here, not a defect.

Both counts are printed. A run that silently loaded half the archive is the
failure this repository keeps re-learning.

Replaces each season's rows, so re-running the same archive is idempotent.

Usage:
    python src/load_game_team_stats.py --db db/league.db
    python src/load_game_team_stats.py --db db/league.db --raw-dir data/raw
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from load_games import resolve_team_name, team_id  # noqa: E402

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "game_team_stats_tables.sql"
DEFAULT_RAW = Path("data/raw")
DEFAULT_DB = Path("db/league.db")
PREFIX = "game_team_stats_"


def season_of(path):
    stem = Path(path).stem
    if not stem.startswith(PREFIX):
        raise ValueError(f"{path} is not a {PREFIX}* file")
    season = stem[len(PREFIX):]
    if not season.isdigit():
        raise ValueError(f"{path} has no season in its name")
    return int(season)


def team_resolver(cur):
    """School name -> internal team_id, or None when this database has no such
    team. Memoised, because a season's archive names the same school hundreds
    of times and the identity lookup is not free."""
    cache: dict[str, int | None] = {}

    def resolve(raw):
        name = (raw or "").strip()
        if not name:
            return None
        if name not in cache:
            try:
                cache[name] = team_id(cur, resolve_team_name(cur, name))
            except ValueError:
                cache[name] = None
        return cache[name]

    return resolve


def load_file(conn, path):
    """Load one season's file. Returns a dict of counts."""
    season = season_of(path)
    cur = conn.cursor()
    resolve = team_resolver(cur)
    known_games = {g for (g,) in cur.execute("SELECT game_id FROM games")}
    with Path(path).open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        if int(row["season_year"]) != season:
            raise ValueError(
                f"{path}: a row says season {row['season_year']}. The file name "
                "decides which season a box score belongs to; they must agree.")
    # Delete only after the whole file has been read and checked: a half-read
    # file must not leave the season emptier than it found it.
    cur.execute("DELETE FROM game_team_stats WHERE season_year = ?", (season,))
    loaded = 0
    unknown_games, unmatched = set(), set()
    for row in rows:
        gid = int(row["game_id"])
        if gid not in known_games:
            unknown_games.add(gid)
            continue
        tid = resolve(row["school"])
        if tid is None:
            unmatched.add((row["school"] or "").strip())
            continue
        value = (row["stat"] or "").strip()
        if value == "":
            # Nothing reported is nothing stored. A blank row would be
            # indistinguishable from a measured zero to every later reader.
            continue
        cur.execute(
            "INSERT OR REPLACE INTO game_team_stats "
            "(game_id, team_id, season_year, category, stat) VALUES (?, ?, ?, ?, ?)",
            (gid, tid, season, row["category"], value))
        loaded += 1
    conn.commit()
    return {"season": season, "loaded": loaded,
            "unknown_games": sorted(unknown_games),
            "unmatched": sorted(unmatched)}


def load_all(conn, raw_dir):
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    return [load_file(conn, path)
            for path in sorted(Path(raw_dir).glob(f"{PREFIX}*.csv"))]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW))
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    results = load_all(conn, args.raw_dir)
    conn.close()
    if not results:
        print("No game_team_stats_*.csv in " + args.raw_dir)
        return 0
    total = sum(r["loaded"] for r in results)
    games = sum(len(r["unknown_games"]) for r in results)
    names = sorted({n for r in results for n in r["unmatched"]})
    print(f"Loaded {total} team box-score value(s) across {len(results)} season(s).")
    if games:
        print(f"{games} game id(s) skipped: not in the games table.")
    if names:
        print(f"{len(names)} school name(s) skipped, unresolvable here: "
              f"{', '.join(names[:12])}{' ...' if len(names) > 12 else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
