#!/usr/bin/env python3
"""
Derives each conference's champion (conf_rank = 1) for a season
from conference_standings_by_year, and writes to
conference_champions_by_year.
"""
import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path("db/league.db")

def main(year: int):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "CREATE TABLE IF NOT EXISTS conference_champions_by_year ("
        "  season_year INTEGER NOT NULL,"
        "  conference  TEXT NOT NULL,"
        "  team_id     INTEGER NOT NULL,"
        "  source      TEXT NOT NULL DEFAULT 'conf_rank_1',"
        "  computed_at TEXT DEFAULT (datetime('now')),"
        "  PRIMARY KEY (season_year, conference)"
        ")"
    )

    cur.execute(
        "DELETE FROM conference_champions_by_year WHERE season_year = ?",
        (year,),
    )

    cur.execute(
        """
        INSERT INTO conference_champions_by_year (season_year, conference, team_id)
        SELECT season_year, conference, team_id
        FROM conference_standings_by_year
        WHERE season_year = ? AND conf_rank = 1
        """,
        (year,),
    )

    conn.commit()
    n = cur.execute(
        "SELECT COUNT(*) FROM conference_champions_by_year WHERE season_year = ?",
        (year,),
    ).fetchone()[0]
    print(f"Computed {n} conference champions for {year}.")
    conn.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()
    main(args.year)