#!/usr/bin/env python3
"""
Loads data/raw/game_advanced_<year>.csv (from fetch_cfbd_game_advanced.py)
into game_team_advanced. EXP-03.

Unlike load_advanced.py's season totals, this is MODEL INPUT: the xSRDiff
performance layer reads per-game Success Rate from here. So it is stricter --
a row whose game_id is not in the games table is skipped and reported rather
than stored, because a stat that cannot be tied to a real game must never
reach a rating.

- Replaces that season's rows each run (idempotent).
- Team names go through the same canonical/alias lookup as load_games.py;
  unmatched names are skipped and reported.
- Blank metrics stay NULL ("not reported"), never 0.

Usage:
    python src/load_game_advanced.py data/raw/game_advanced_2015.csv --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_cfbd_game_advanced import FIELDS  # noqa: E402
from load_games import resolve_team_name, team_id  # noqa: E402

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "game_advanced_tables.sql"
METRIC_COLS = [c for c, _, _ in FIELDS]


def _num(v, integer=False):
    if v is None or str(v).strip() == "":
        return None
    return int(float(v)) if integer else float(v)


def load_game_advanced(conn: sqlite3.Connection, csv_path: str) -> dict:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    cur = conn.cursor()
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for s in {int(r["season_year"]) for r in rows}:
        cur.execute("DELETE FROM game_team_advanced WHERE season_year = ?", (s,))
    known_games = {g for (g,) in cur.execute("SELECT game_id FROM games")}
    loaded, unmatched, unknown_games = 0, [], []
    cols = ["game_id", "team_id", "season_year", "source", "garbage_time_excluded"] + METRIC_COLS
    placeholders = ",".join("?" for _ in cols)
    for r in rows:
        gid = int(r["game_id"])
        if gid not in known_games:
            unknown_games.append(gid)
            continue
        try:
            tid = team_id(cur, resolve_team_name(cur, r["team"]))
        except ValueError:
            unmatched.append(r["team"])
            continue
        values = [_num(r.get(c), integer=c.endswith("_plays")) for c in METRIC_COLS]
        cur.execute(
            f"INSERT OR REPLACE INTO game_team_advanced ({', '.join(cols)}) VALUES ({placeholders})",
            [gid, tid, int(r["season_year"]), "cfbd", int(r.get("garbage_time_excluded") or 1)] + values,
        )
        loaded += 1
    conn.commit()
    return {"loaded": loaded, "unmatched": sorted(set(unmatched)),
            "unknown_games": sorted(set(unknown_games))}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--db", default="db/league.db")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    stats = load_game_advanced(conn, args.csv_path)
    conn.close()
    msg = f"{args.csv_path}: {stats['loaded']} team-games loaded"
    if stats["unmatched"]:
        msg += f" ({len(stats['unmatched'])} unmatched team names skipped: {', '.join(stats['unmatched'])})"
    if stats["unknown_games"]:
        msg += f" ({len(stats['unknown_games'])} rows skipped: game_id not in the games table)"
    print(msg)


if __name__ == "__main__":
    main()
