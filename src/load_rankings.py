#!/usr/bin/env python3
"""
Loads data/raw/rankings_<year>.csv (written by fetch_cfbd_games.py) into
poll_rankings. Milestone 5. Display data only -- nothing here feeds a model.

- Replaces that season's rows each run (idempotent; picks up new releases).
- Maps CFBD school names through the same canonical/alias lookup load_games.py
  uses. Unmatched names keep team_id = NULL and their raw name, and are reported.
- Blank first-place votes / points stay NULL ("not reported"), never 0.

Usage:
    python src/load_rankings.py data/raw/rankings_2026.csv --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from load_games import resolve_team_name, team_id  # noqa: E402

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "rankings_tables.sql"


def _int_or_none(v):
    return None if v is None or str(v).strip() == "" else int(float(v))


def load_rankings(conn: sqlite3.Connection, csv_path: str) -> dict:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    cur = conn.cursor()
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    seasons = {int(r["season_year"]) for r in rows}
    if not seasons:
        stem = Path(csv_path).stem.replace("rankings_", "")
        seasons = {int(stem)} if stem.isdigit() else set()
    for s in seasons:
        cur.execute("DELETE FROM poll_rankings WHERE season_year = ?", (s,))

    unmatched = set()
    for r in rows:
        try:
            tid = team_id(cur, resolve_team_name(cur, r["school"]))
        except ValueError:
            tid = None
            unmatched.add(r["school"])
        cur.execute(
            """INSERT OR REPLACE INTO poll_rankings
               (season_year, season_type, week, poll, rank, team_id, school, first_place_votes, points)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (int(r["season_year"]), r["season_type"], int(r["week"]), r["poll"], int(r["rank"]), tid,
             r["school"], _int_or_none(r["first_place_votes"]), _int_or_none(r["points"])),
        )
    conn.commit()
    return {"rows": len(rows), "unmatched": sorted(unmatched)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--db", default="db/league.db")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    stats = load_rankings(conn, args.csv_path)
    conn.close()
    msg = f"{args.csv_path}: {stats['rows']} poll rows loaded"
    if stats["unmatched"]:
        msg += f" ({len(stats['unmatched'])} school names not matched to a team, shown unlinked: {', '.join(stats['unmatched'])})"
    print(msg)


if __name__ == "__main__":
    main()
