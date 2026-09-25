#!/usr/bin/env python3
"""
Loads data/raw/kickoffs_<year>.csv (from fetch_kickoffs.py) into game_kickoffs.
Display data only. Replaces that season's rows each run (idempotent). Rows with an
unparseable timestamp are skipped and counted, never guessed at.

Date-only seasons: when EVERY kickoff in a season is exactly 00:00:00Z and none is
flagged TBD, CFBD has dates but no clock times for that season (true of 1980-2000
when this was written; from 2001 real times appear, and midnight UTC is then a
genuine 8 PM Eastern kickoff). Such a season is stored with date_only = 1 so the
website shows the stored date instead of converting a fake midnight to Eastern.

Usage:
    python src/load_kickoffs.py data/raw/kickoffs_2025.csv --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sqlite3
from pathlib import Path

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "kickoff_tables.sql"


def _valid(ts: str) -> bool:
    try:
        dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _midnight_utc(ts: str) -> bool:
    return ts[10:19] == "T00:00:00" and ts.endswith("Z")


def load_kickoffs(conn: sqlite3.Connection, csv_path: str) -> dict:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    # Upgrade a table created before date_only existed (CREATE IF NOT EXISTS won't).
    if "date_only" not in {r[1] for r in conn.execute("PRAGMA table_info(game_kickoffs)")}:
        conn.execute("ALTER TABLE game_kickoffs ADD COLUMN date_only INTEGER NOT NULL DEFAULT 0")
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    valid = [(r.get("kickoff_utc") or "").strip() for r in rows]
    date_only = int(bool(valid) and all(_midnight_utc(ts) for ts in valid)
                    and not any(str(r.get("start_time_tbd")).strip() == "1" for r in rows))
    for season in {int(r["season_year"]) for r in rows}:
        conn.execute("DELETE FROM game_kickoffs WHERE season_year = ?", (season,))
    loaded, bad = 0, 0
    for r in rows:
        ts = (r.get("kickoff_utc") or "").strip()
        if not ts or not _valid(ts):
            bad += 1
            continue
        conn.execute("INSERT OR REPLACE INTO game_kickoffs (game_id, season_year, kickoff_utc, time_tbd, date_only) "
                     "VALUES (?, ?, ?, ?, ?)",
                     (int(r["game_id"]), int(r["season_year"]), ts,
                      1 if str(r.get("start_time_tbd")).strip() == "1" else 0, date_only))
        loaded += 1
    conn.commit()
    return {"loaded": loaded, "skipped": bad, "date_only": bool(date_only)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--db", default="db/league.db")
    a = p.parse_args()
    conn = sqlite3.connect(a.db)
    stats = load_kickoffs(conn, a.csv_path)
    conn.close()
    print(f"{a.csv_path}: {stats['loaded']} kickoff times loaded"
          + (" (dates only: CFBD has no clock times for this season)" if stats["date_only"] else "")
          + (f" ({stats['skipped']} unparseable, skipped)" if stats["skipped"] else ""))


if __name__ == "__main__":
    main()
