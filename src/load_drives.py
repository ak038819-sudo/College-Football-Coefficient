#!/usr/bin/env python3
"""
Loads data/raw/drives/<year>.csv.gz (from fetch_cfbd_drives.py) into game_drives.

What this does beyond copying rows:

  * resolves offense and defense through the same canonical/alias lookup every
    other loader uses, so a drive's teams are the teams the rest of the
    database knows. A name that will not resolve keeps its text and leaves the
    id NULL -- the drive still belongs to its game, and dropping it would lose
    part of a game's story over a spelling;
  * turns CFBD's offense/defense running scores into home/away once, so no
    reader has to track which side had the ball;
  * computes regulation seconds remaining at the end of each drive, which is
    what the live win-probability fit reads.

A drive whose game_id is not in the games table is skipped and reported. The
games table is FBS-vs-FBS, so a season's drives include games this project
does not hold; that is expected and counted, not an error.

Replaces that season's rows on every run, so re-running is idempotent.

Usage:
    python src/load_drives.py data/raw/drives/2024.csv.gz --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import gzip
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from load_games import resolve_team_name, team_id  # noqa: E402
from win_probability import PERIOD_SECONDS, REGULATION_PERIODS  # noqa: E402

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "drive_tables.sql"

COLUMNS = [
    "game_id", "drive_number", "season_year", "season_type", "week", "drive_id",
    "offense_team_id", "defense_team_id", "offense_name", "defense_name",
    "is_home_offense", "scoring", "drive_result",
    "start_period", "start_clock", "end_period", "end_clock", "seconds_remaining",
    "start_yards_to_goal", "end_yards_to_goal", "plays", "yards",
    "home_score_before", "away_score_before", "home_score_after", "away_score_after",
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


def regulation_seconds_remaining(period, clock) -> int | None:
    """Regulation seconds left at a period and clock, or None if either is unknown.

    Overtime is 0: regulation is over. A missing clock is None rather than 0,
    because 0 says the game ended there -- a claim an unreadable clock does not
    support, and one the win-probability fit would read as a final state.
    """
    period, clock = _int(period), _int(clock)
    if period is None or clock is None or period < 1:
        return None
    if period > REGULATION_PERIODS:
        return 0
    return (REGULATION_PERIODS - period) * PERIOD_SECONDS + min(clock, PERIOD_SECONDS)


def home_away(row: dict, prefix: str) -> tuple[int | None, int | None]:
    """CFBD's offense/defense score at one end of a drive -> (home, away).

    `is_home_offense` is the only thing that says which is which, so without it
    the pair is (None, None): a 50/50 guess about which team has 21 points would
    be worse than an honest gap.
    """
    offense = _int(row.get(f"{prefix}_offense_score"))
    defense = _int(row.get(f"{prefix}_defense_score"))
    home_offense = _int(row.get("is_home_offense"))
    if home_offense is None or offense is None or defense is None:
        return None, None
    return (offense, defense) if home_offense == 1 else (defense, offense)


def _read_rows(path: str) -> list[dict]:
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def load_drives(conn: sqlite3.Connection, csv_path: str) -> dict:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    cur = conn.cursor()
    rows = _read_rows(csv_path)
    seasons = {_int(r.get("season_year")) for r in rows} - {None}
    for season in seasons:
        cur.execute("DELETE FROM game_drives WHERE season_year = ?", (season,))
    known_games = {g for (g,) in cur.execute("SELECT game_id FROM games")}
    placeholders = ",".join("?" for _ in COLUMNS)
    statement = f"INSERT OR REPLACE INTO game_drives ({', '.join(COLUMNS)}) VALUES ({placeholders})"

    loaded, skipped_games, unmatched, untimed = 0, set(), set(), 0
    for row in rows:
        game_id, number = _int(row.get("game_id")), _int(row.get("drive_number"))
        season = _int(row.get("season_year"))
        if game_id is None or number is None or season is None:
            continue
        if game_id not in known_games:
            skipped_games.add(game_id)
            continue
        ids = []
        for side in ("offense", "defense"):
            name = (row.get(side) or "").strip()
            try:
                ids.append(team_id(cur, resolve_team_name(cur, name)))
            except ValueError:
                if name:
                    unmatched.add(name)
                ids.append(None)
        seconds = regulation_seconds_remaining(row.get("end_period"), row.get("end_clock"))
        if seconds is None:
            untimed += 1
        home_before, away_before = home_away(row, "start")
        home_after, away_after = home_away(row, "end")
        cur.execute(statement, [
            game_id, number, season, (row.get("season_type") or None), _int(row.get("week")),
            _int(row.get("drive_id")), ids[0], ids[1],
            (row.get("offense") or "").strip(), (row.get("defense") or "").strip(),
            _int(row.get("is_home_offense")), _int(row.get("scoring")),
            (row.get("drive_result") or None),
            _int(row.get("start_period")), _int(row.get("start_clock")),
            _int(row.get("end_period")), _int(row.get("end_clock")), seconds,
            _int(row.get("start_yards_to_goal")), _int(row.get("end_yards_to_goal")),
            _int(row.get("plays")), _int(row.get("yards")),
            home_before, away_before, home_after, away_after,
        ])
        loaded += 1
    conn.commit()
    return {"loaded": loaded, "skipped_games": sorted(skipped_games),
            "unmatched": sorted(unmatched), "untimed": untimed}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Load one season of CFBD drives.")
    parser.add_argument("csv_path")
    parser.add_argument("--db", default="db/league.db")
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.db)
    try:
        stats = load_drives(conn, args.csv_path)
    finally:
        conn.close()
    message = f"{args.csv_path}: {stats['loaded']} drives loaded"
    if stats["untimed"]:
        message += f" ({stats['untimed']} with no usable end clock)"
    if stats["skipped_games"]:
        message += f" ({len(stats['skipped_games'])} games not in the games table, skipped)"
    if stats["unmatched"]:
        shown = ", ".join(stats["unmatched"][:8])
        message += f" ({len(stats['unmatched'])} team names unresolved: {shown})"
    print(message)


if __name__ == "__main__":
    main()
