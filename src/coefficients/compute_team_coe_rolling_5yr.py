#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def compute_rolling(conn: sqlite3.Connection, season_year: int, window: int, formula_version: str) -> None:
    start_year = season_year - (window - 1)
    end_year = season_year

    conn.execute(
        "DELETE FROM team_coefficient_rolling_5yr WHERE season_year=? AND formula_version=?",
        (season_year, formula_version),
    )

    sql = """
    WITH windowed AS (
      SELECT
        team_id,
        SUM(total_points) AS total_points_5yr,
        SUM(games_counted) AS games_counted_5yr
      FROM team_coefficient_by_year
      WHERE formula_version=?
        AND season_year BETWEEN ? AND ?
      GROUP BY team_id
    )
    INSERT INTO team_coefficient_rolling_5yr
      (season_year, team_id, window_start_year, window_end_year,
       total_points_5yr, games_counted_5yr, points_per_game_5yr, formula_version)
    SELECT
      ? AS season_year,
      team_id,
      ? AS window_start_year,
      ? AS window_end_year,
      COALESCE(total_points_5yr, 0.0) AS total_points_5yr,
      COALESCE(games_counted_5yr, 0) AS games_counted_5yr,
      CASE
        WHEN COALESCE(games_counted_5yr, 0) > 0 THEN (1.0 * total_points_5yr) / games_counted_5yr
        ELSE 0.0
      END AS points_per_game_5yr,
      ? AS formula_version
    FROM windowed;
    """
    conn.execute(sql, (formula_version, start_year, end_year, season_year, start_year, end_year, formula_version))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--window", type=int, default=5)
    p.add_argument("--formula-version", default="v0")
    args = p.parse_args()

    conn = connect(args.db)
    try:
        compute_rolling(conn, args.year, args.window, args.formula_version)
        conn.commit()
    finally:
        conn.close()

    print(f"Rolling {args.window}-year Team CoE computed for {args.year} (formula_version={args.formula_version})")


if __name__ == "__main__":
    main()