#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--formula-version", default="v0")
    p.add_argument("--ruleset", default="year2")
    args = p.parse_args()

    conn = connect(args.db)

    sql = """
    SELECT
        q.team_id,
        t.team_name,
        q.conference,
        q.bid_type,
        r.total_points_5yr,
        r.points_per_game_5yr
    FROM playoff_qualifiers_by_year q
    JOIN teams t ON t.team_id = q.team_id
    LEFT JOIN team_coefficient_rolling_5yr r
      ON r.team_id = q.team_id
     AND r.season_year = q.season_year
     AND r.formula_version = q.formula_version
    WHERE q.season_year=?
      AND q.formula_version=?
      AND q.ruleset=?
    ORDER BY
        r.total_points_5yr DESC,
        r.points_per_game_5yr DESC,
        t.team_name ASC;
    """

    rows = conn.execute(sql, (args.year, args.formula_version, args.ruleset)).fetchall()

    print(f"\nPlayoff Seeding — {args.year}\n")

    for seed, row in enumerate(rows, start=1):
        bye = " (BYE)" if seed <= 8 else ""
        print(
            f"{seed:>2}. {row['team_name']:<18} "
            f"{row['conference']:<18} "
            f"{row['bid_type']:<8} "
            f"{row['total_points_5yr']:>6.1f} "
            f"{row['points_per_game_5yr']:>5.3f}"
            f"{bye}"
        )

    conn.close()


if __name__ == "__main__":
    main()