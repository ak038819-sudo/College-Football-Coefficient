#!/usr/bin/env python3
"""
Loads data/raw/schedule_<year>.csv (written by fetch_cfbd_games.py) into the
scheduled_games table. Milestone 3.

- Replaces that season's rows each run, so reschedules and games that have
  since been played drop out automatically.
- Skips any game_id already in `games` (i.e. already final), belt and braces.
- Resolves team names with the same canonical/alias lookup as load_games.py;
  unresolvable names (non-FBS opponents) are skipped and counted.
- Never touches `games`, so it cannot affect Elo, CoE, standings or playoffs.

Usage:
    python src/load_schedule.py data/raw/schedule_2026.csv --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from load_games import resolve_team_name, team_id  # noqa: E402

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "schedule_tables.sql"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))


def load_schedule(conn: sqlite3.Connection, csv_path: str) -> dict:
    ensure_schema(conn)
    cur = conn.cursor()
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    seasons = {int(r["season_year"]) for r in rows}
    if not seasons:  # empty file: infer the season from the filename and clear it
        stem = Path(csv_path).stem.replace("schedule_", "")
        seasons = {int(stem)} if stem.isdigit() else set()
    for s in seasons:
        cur.execute("DELETE FROM scheduled_games WHERE season_year = ?", (s,))

    final_ids = {r[0] for r in cur.execute("SELECT game_id FROM games")}
    stats = {"inserted": 0, "already_final": 0, "unresolved": 0}
    for r in rows:
        gid = int(r["game_id"])
        if gid in final_ids:
            stats["already_final"] += 1
            continue
        try:
            home = team_id(cur, resolve_team_name(cur, r["home_team"]))
            away = team_id(cur, resolve_team_name(cur, r["away_team"]))
        except ValueError:
            stats["unresolved"] += 1
            continue
        cur.execute(
            """INSERT OR REPLACE INTO scheduled_games
               (game_id, season_year, week, season_type, kickoff_utc, start_time_tbd,
                home_team_id, away_team_id, neutral_site, game_phase, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (gid, int(r["season_year"]), int(r["week"]) if r["week"] else None, r["season_type"] or None,
             r["kickoff_utc"] or None, int(r["start_time_tbd"] or 0), home, away,
             int(r["neutral_site"] or 0), r["game_phase"] or "regular", r["notes"] or None),
        )
        stats["inserted"] += 1
    conn.commit()
    return stats


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--db", default="db/league.db")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    stats = load_schedule(conn, args.csv_path)
    conn.close()
    print(f"{args.csv_path}: {stats['inserted']} scheduled games loaded "
          f"({stats['already_final']} already final, {stats['unresolved']} non-FBS/unknown teams skipped)")


if __name__ == "__main__":
    main()
