#!/usr/bin/env python3
"""
Loads data/raw/advanced_<year>.csv (from fetch_cfbd_advanced.py) into
team_season_advanced. Milestone 7. Display data only -- nothing here feeds a model.

- Replaces that season's rows each run (idempotent).
- Team names go through the same canonical/alias lookup as load_games.py;
  unmatched names are skipped and reported.
- Blank metrics stay NULL ("not reported"), never 0.

Usage:
    python src/load_advanced.py data/raw/advanced_2025.csv --db db/league.db
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_cfbd_advanced import FIELDS  # noqa: E402
from load_games import resolve_team_name, team_id  # noqa: E402

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "advanced_tables.sql"
METRIC_COLS = [c for c, _, _ in FIELDS]


def _num(v, integer=False):
    if v is None or str(v).strip() == "":
        return None
    return int(float(v)) if integer else float(v)


def load_advanced(conn: sqlite3.Connection, csv_path: str) -> dict:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    cur = conn.cursor()
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for s in {int(r["season_year"]) for r in rows}:
        cur.execute("DELETE FROM team_season_advanced WHERE season_year = ?", (s,))
    loaded, unmatched = 0, []
    placeholders = ",".join("?" for _ in range(4 + len(METRIC_COLS)))
    for r in rows:
        try:
            tid = team_id(cur, resolve_team_name(cur, r["team"]))
        except ValueError:
            unmatched.append(r["team"])
            continue
        values = [_num(r.get(c), integer=c.endswith("_plays")) for c in METRIC_COLS]
        cur.execute(
            f"INSERT OR REPLACE INTO team_season_advanced (team_id, season_year, source, garbage_time_excluded, "
            f"{', '.join(METRIC_COLS)}) VALUES ({placeholders})",
            [tid, int(r["season_year"]), "cfbd", int(r.get("garbage_time_excluded") or 1)] + values,
        )
        loaded += 1
    conn.commit()
    return {"loaded": loaded, "unmatched": sorted(unmatched)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--db", default="db/league.db")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    stats = load_advanced(conn, args.csv_path)
    conn.close()
    msg = f"{args.csv_path}: {stats['loaded']} team-seasons loaded"
    if stats["unmatched"]:
        msg += f" ({len(stats['unmatched'])} unmatched team names skipped: {', '.join(stats['unmatched'])})"
    print(msg)


if __name__ == "__main__":
    main()
