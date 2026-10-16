#!/usr/bin/env python3
"""
Loads data/raw/lines/<year>.csv.gz (from fetch_cfbd_lines.py) into game_lines.

What this does beyond copying rows:

  * turns CFBD's spread into this database's direction once. CFBD's number is
    the home team's handicap, negative when the home team is favoured; every
    margin here is home minus away. Flipping it at load means no reader has to
    remember, and a reader who gets it backwards would report the model beating
    the market on exactly the games it lost to it;
  * keeps a missing line missing. A blank spread stays NULL, never 0, because
    0 is a real line;
  * drops a line for a game this project does not hold. CFBD prices games
    against FCS opponents and the games table is FBS-vs-FBS, so a season's file
    always has more games than this database; that is counted, not an error.

Replaces that season's rows on every run, so re-running is idempotent.

Usage:
    python src/load_lines.py data/raw/lines/2024.csv.gz --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import gzip
import sqlite3
from pathlib import Path

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "line_tables.sql"

COLUMNS = [
    "game_id", "provider", "season_year", "season_type", "week",
    "spread", "market_home_margin", "spread_open", "market_home_open",
    "over_under", "over_under_open", "home_moneyline", "away_moneyline",
    "formatted_spread", "snapshot_home_score", "snapshot_away_score",
]


def _int(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _float(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def market_home_margin(spread) -> float | None:
    """CFBD's home handicap -> the margin this database counts in.

    -7 (home favoured by 7) becomes +7. None stays None, and 0 stays 0: a
    pick'em is a line, and turning it into None would throw away the games the
    market found hardest to call.
    """
    value = _float(spread)
    return None if value is None else -value


def _read_rows(path: str) -> list[dict]:
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def load_lines(conn: sqlite3.Connection, csv_path: str) -> dict:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    cur = conn.cursor()
    rows = _read_rows(csv_path)
    seasons = {_int(r.get("season_year")) for r in rows} - {None}
    for season in seasons:
        cur.execute("DELETE FROM game_lines WHERE season_year = ?", (season,))
    known_games = {g for (g,) in cur.execute("SELECT game_id FROM games")}
    placeholders = ",".join("?" for _ in COLUMNS)
    statement = f"INSERT OR REPLACE INTO game_lines ({', '.join(COLUMNS)}) VALUES ({placeholders})"

    loaded, skipped_games, unpriced, providers = 0, set(), 0, set()
    # A snapshot with no result on it was taken before the game ended, so its
    # spread is not a closing line. Counted here and excluded by whoever
    # measures against the close -- see sql/line_tables.sql.
    pre_kickoff = 0
    for row in rows:
        game_id = _int(row.get("game_id"))
        season = _int(row.get("season_year"))
        provider = (row.get("provider") or "").strip()
        if game_id is None or season is None or not provider:
            continue
        if game_id not in known_games:
            skipped_games.add(game_id)
            continue
        spread = _float(row.get("spread"))
        if spread is None:
            unpriced += 1
        providers.add(provider)
        cur.execute(statement, [
            game_id, provider, season, (row.get("season_type") or None), _int(row.get("week")),
            spread, market_home_margin(row.get("spread")),
            _float(row.get("spread_open")), market_home_margin(row.get("spread_open")),
            _float(row.get("over_under")), _float(row.get("over_under_open")),
            _int(row.get("home_moneyline")), _int(row.get("away_moneyline")),
            (row.get("formatted_spread") or None),
            _int(row.get("home_score")), _int(row.get("away_score")),
        ])
        loaded += 1
        if _int(row.get("home_score")) is None or _int(row.get("away_score")) is None:
            pre_kickoff += 1
    conn.commit()
    return {"loaded": loaded, "skipped_games": sorted(skipped_games),
            "unpriced": unpriced, "providers": sorted(providers),
            "pre_kickoff": pre_kickoff}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Load one season of CFBD betting lines.")
    parser.add_argument("csv_path")
    parser.add_argument("--db", default="db/league.db")
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.db)
    try:
        stats = load_lines(conn, args.csv_path)
    finally:
        conn.close()
    message = f"{args.csv_path}: {stats['loaded']} lines loaded"
    if stats["providers"]:
        message += f" from {len(stats['providers'])} providers ({', '.join(stats['providers'][:6])})"
    if stats["unpriced"]:
        message += f" ({stats['unpriced']} with no spread)"
    if stats["pre_kickoff"]:
        message += (f" ({stats['pre_kickoff']} captured before the game ended, "
                    f"not closing lines)")
    if stats["skipped_games"]:
        message += f" ({len(stats['skipped_games'])} games not in the games table, skipped)"
    print(message)


if __name__ == "__main__":
    main()
