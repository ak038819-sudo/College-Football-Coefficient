#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from typing import Optional, Tuple

FORMULA_VERSION = "v0"

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def compute_nonconf_component(conn: sqlite3.Connection, season_year: int) -> None:
    """
    Conference CoE non-conference scoring (base, no bounty yet):
      Win = 2
      OT loss = 1
      Loss = 0

    Only counts games where both conferences are known and is_nonconference=1.
    Points are credited to each team's conference independently (home + away).
    """
    # Home conference points from non-conf games
    home_sql = """
    WITH base AS (
      SELECT
        season_year,
        home_conference AS conference,
        CASE
          WHEN is_nonconference != 1 THEN 0
          WHEN home_conference IS NULL OR away_conference IS NULL THEN 0
          WHEN winner = 'home' THEN 2
          WHEN winner = 'away' AND went_ot = 1 THEN 1
          ELSE 0
        END AS pts,
        CASE
          WHEN is_nonconference = 1 AND home_conference IS NOT NULL AND away_conference IS NOT NULL THEN 1
          ELSE 0
        END AS game_ct
      FROM v_games_enriched
      WHERE season_year = ?
    )
    SELECT season_year, conference, SUM(pts) AS points, SUM(game_ct) AS games_counted
    FROM base
    WHERE conference IS NOT NULL
    GROUP BY season_year, conference;
    """

    # Away conference points from non-conf games
    away_sql = """
    WITH base AS (
      SELECT
        season_year,
        away_conference AS conference,
        CASE
          WHEN is_nonconference != 1 THEN 0
          WHEN home_conference IS NULL OR away_conference IS NULL THEN 0
          WHEN winner = 'away' THEN 2
          WHEN winner = 'home' AND went_ot = 1 THEN 1
          ELSE 0
        END AS pts,
        CASE
          WHEN is_nonconference = 1 AND home_conference IS NOT NULL AND away_conference IS NOT NULL THEN 1
          ELSE 0
        END AS game_ct
      FROM v_games_enriched
      WHERE season_year = ?
    )
    SELECT season_year, conference, SUM(pts) AS points, SUM(game_ct) AS games_counted
    FROM base
    WHERE conference IS NOT NULL
    GROUP BY season_year, conference;
    """

    rows = {}
    for sql in (home_sql, away_sql):
        for r in conn.execute(sql, (season_year,)):
            key = (r["season_year"], r["conference"])
            if key not in rows:
                rows[key] = {"points": 0.0, "games": 0}
            rows[key]["points"] += float(r["points"] or 0.0)
            rows[key]["games"] += int(r["games_counted"] or 0)

    # Upsert component rows
    conn.execute(
        "DELETE FROM conference_coe_components WHERE season_year=? AND component='nonconf_base' AND formula_version=?",
        (season_year, FORMULA_VERSION),
    )
    conn.executemany(
        """
        INSERT INTO conference_coe_components
          (season_year, conference, component, points, games_counted, formula_version, notes)
        VALUES (?, ?, 'nonconf_base', ?, ?, ?, ?)
        """,
        [
            (sy, conf, vals["points"], vals["games"], FORMULA_VERSION, "Win=2, OT loss=1, Loss=0; non-conf only; no bounty")
            for (sy, conf), vals in rows.items()
        ],
    )

def compute_playoff_components(conn: sqlite3.Connection, season_year: int) -> None:
    """
    Conference CoE playoff bonuses (baseline interpretation):
      - participation: +6 per team that appears in >=1 CFP game (game_phase='cfp'), capped at 12 per team (future)
      - per playoff game played: +1.5 per CFP game appearance

    Applied to the conference of the team in that season (home and away appearances).
    """
    # Playoff game appearances per conference (count teams per game appearance)
    appearances_sql = """
    WITH team_games AS (
      SELECT season_year, home_team_id AS team_id, home_conference AS conference
      FROM v_games_enriched
      WHERE season_year=? AND game_phase='cfp' AND home_conference IS NOT NULL
      UNION ALL
      SELECT season_year, away_team_id AS team_id, away_conference AS conference
      FROM v_games_enriched
      WHERE season_year=? AND game_phase='cfp' AND away_conference IS NOT NULL
    ),
    games_by_conf AS (
      SELECT season_year, conference, COUNT(*) AS playoff_games
      FROM team_games
      GROUP BY season_year, conference
    ),
    participants_by_conf AS (
      SELECT season_year, conference, COUNT(DISTINCT team_id) AS participants
      FROM team_games
      GROUP BY season_year, conference
    )
    SELECT
      g.season_year,
      g.conference,
      COALESCE(p.participants, 0) AS participants,
      COALESCE(g.playoff_games, 0) AS playoff_games
    FROM games_by_conf g
    LEFT JOIN participants_by_conf p
      ON p.season_year=g.season_year AND p.conference=g.conference;
    """

    conn.execute(
        "DELETE FROM conference_coe_components WHERE season_year=? AND component IN ('playoff_participation','playoff_games') AND formula_version=?",
        (season_year, FORMULA_VERSION),
    )

    comp_rows = []
    for r in conn.execute(appearances_sql, (season_year, season_year)):
        sy = int(r["season_year"])
        conf = r["conference"]
        participants = int(r["participants"])
        playoff_games = int(r["playoff_games"])

        participation_points = 6.0 * participants
        per_game_points = 1.5 * playoff_games

        comp_rows.append((sy, conf, "playoff_participation", participation_points, participants, FORMULA_VERSION, "+6 per participating team (baseline)"))
        comp_rows.append((sy, conf, "playoff_games", per_game_points, playoff_games, FORMULA_VERSION, "+1.5 per CFP game appearance"))

    conn.executemany(
        """
        INSERT INTO conference_coe_components
          (season_year, conference, component, points, games_counted, formula_version, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        comp_rows,
    )

def rollup_totals(conn: sqlite3.Connection, season_year: int) -> None:
    """
    Sum components into conference_coefficient_by_year.
    games_counted = nonconf_base games_counted + playoff_games games_counted (not participation count).
    """
    rollup_sql = """
    WITH sums AS (
      SELECT
        season_year,
        conference,
        SUM(points) AS total_points,
        SUM(CASE WHEN component='nonconf_base' THEN games_counted ELSE 0 END) +
        SUM(CASE WHEN component='playoff_games' THEN games_counted ELSE 0 END) AS games_counted
      FROM conference_coe_components
      WHERE season_year=? AND formula_version=?
      GROUP BY season_year, conference
    )
    SELECT
      season_year,
      conference,
      total_points,
      games_counted,
      CASE WHEN games_counted > 0 THEN total_points / games_counted ELSE 0 END AS ppg
    FROM sums;
    """

    conn.execute(
        "DELETE FROM conference_coefficient_by_year WHERE season_year=? AND formula_version=?",
        (season_year, FORMULA_VERSION),
    )

    conn.executemany(
        """
        INSERT INTO conference_coefficient_by_year
          (season_year, conference, total_points, games_counted, points_per_game, formula_version)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (int(r["season_year"]), r["conference"], float(r["total_points"]), int(r["games_counted"]), float(r["ppg"]), FORMULA_VERSION)
            for r in conn.execute(rollup_sql, (season_year, FORMULA_VERSION))
        ],
    )

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    conn = connect(args.db)
    try:
        compute_nonconf_component(conn, args.year)
        compute_playoff_components(conn, args.year)
        rollup_totals(conn, args.year)
        conn.commit()
    finally:
        conn.close()

    print(f"Conference CoE computed for {args.year} (formula_version={FORMULA_VERSION})")

if __name__ == "__main__":
    main()