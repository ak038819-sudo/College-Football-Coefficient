#!/usr/bin/env python3
"""
Derives conference_standings_by_year FROM conference_team_records_by_year
(computed locally from our own loaded games), using the EXACT same
tiebreak heuristic as fetch_cfbd_conference_standings.py:

  1) conference win% desc
  2) conference wins desc
  3) conference losses asc
  4) overall win% desc
  5) overall wins desc
  6) team name asc

NOTE: same as the CFBD-based script, this is NOT a perfect recreation
of each conference's actual tiebreak rules (head-to-head, divisions,
etc.) -- it's a stable proxy ordering. The two scripts differ only in
data source (our own loaded games vs a live CFBD API pull), not in
method.

Requires compute_conference_team_records.py to have already been run
for the target year.

Usage:
    python src/coefficients/derive_conference_standings_local.py --year 2025
"""
from __future__ import annotations

import argparse
import sqlite3

SOURCE = "computed_local"
SOURCE_DETAIL = "derived from conference_team_records_by_year (our own loaded games), same tiebreak heuristic as fetch_cfbd_conference_standings.py"


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def derive_standings(conn: sqlite3.Connection, season_year: int) -> int:
    rows = conn.execute(
        """
        SELECT r.conference, r.team_id, t.team_name,
               r.conf_win_pct, r.conf_wins, r.conf_losses,
               r.overall_win_pct, r.overall_wins
        FROM conference_team_records_by_year r
        JOIN teams t ON t.team_id = r.team_id
        WHERE r.season_year = ?
        """,
        (season_year,),
    ).fetchall()

    by_conf: dict[str, list] = {}
    for r in rows:
        by_conf.setdefault(r["conference"], []).append(r)

    for conf, teams in by_conf.items():
        teams.sort(
            key=lambda r: (
                -r["conf_win_pct"],
                -r["conf_wins"],
                r["conf_losses"],
                -r["overall_win_pct"],
                -r["overall_wins"],
                r["team_name"],
            )
        )

    conn.execute("DELETE FROM conference_standings_by_year WHERE season_year=?", (season_year,))

    inserted = 0
    for conf, teams in by_conf.items():
        for rank, r in enumerate(teams, start=1):
            conn.execute(
                """
                INSERT INTO conference_standings_by_year
                  (season_year, conference, team_id, conf_rank, source, source_detail)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (season_year, conf, r["team_id"], rank, SOURCE, SOURCE_DETAIL),
            )
            inserted += 1

    return inserted


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    conn = connect(args.db)
    try:
        n = derive_standings(conn, args.year)
        conn.commit()
    finally:
        conn.close()

    print(f"Derived conference standings for {args.year}: {n} rows")


if __name__ == "__main__":
    main()
