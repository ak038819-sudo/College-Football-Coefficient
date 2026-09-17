#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3


FORMULA_VERSION = "v0"


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def compute_nonconf_component(conn: sqlite3.Connection, season_year: int) -> None:
    # Base scoring: win=2, OT loss=1, loss=0. Non-conference games only.
    conn.execute(
        """
        DELETE FROM team_coe_components
        WHERE season_year=? AND component='nonconf_base' AND formula_version=?
        """,
        (season_year, FORMULA_VERSION),
    )

    sql = """
    WITH per_team AS (
      -- HOME side
      SELECT
        g.season_year AS season_year,
        g.home_team_id AS team_id,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0.0
          WHEN g.home_score > g.away_score THEN 2.0
          WHEN g.went_ot = 1 AND g.home_score < g.away_score THEN 1.0
          ELSE 0.0
        END AS pts,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          ELSE 1
        END AS game_ct
      FROM v_games_enriched g
      WHERE g.season_year = ?
        AND g.is_nonconference = 1

      UNION ALL

      -- AWAY side
      SELECT
        g.season_year AS season_year,
        g.away_team_id AS team_id,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0.0
          WHEN g.away_score > g.home_score THEN 2.0
          WHEN g.went_ot = 1 AND g.away_score < g.home_score THEN 1.0
          ELSE 0.0
        END AS pts,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          ELSE 1
        END AS game_ct
      FROM v_games_enriched g
      WHERE g.season_year = ?
        AND g.is_nonconference = 1
    ),
    agg AS (
      SELECT team_id, SUM(pts) AS points, SUM(game_ct) AS games_counted
      FROM per_team
      GROUP BY team_id
    )
    INSERT INTO team_coe_components (season_year, team_id, component, points, games_counted, formula_version, notes)
    SELECT
      ? AS season_year,
      team_id,
      'nonconf_base' AS component,
      COALESCE(points, 0.0) AS points,
      COALESCE(games_counted, 0) AS games_counted,
      ? AS formula_version,
      'Win=2, OT loss=1, Loss=0; non-conf only; no bounty'
    FROM agg;
    """
    conn.execute(sql, (season_year, season_year, season_year, FORMULA_VERSION))


def compute_conf_component(conn: sqlite3.Connection, season_year: int) -> None:
    # Same base scoring, but conference games only (both teams same conference in v_games_enriched).
    conn.execute(
        """
        DELETE FROM team_coe_components
        WHERE season_year=? AND component='conf_base' AND formula_version=?
        """,
        (season_year, FORMULA_VERSION),
    )

    sql = """
    WITH conf_games AS (
      SELECT *
      FROM v_games_enriched g
      WHERE g.season_year = ?
        AND g.home_conference IS NOT NULL
        AND g.away_conference IS NOT NULL
        AND g.home_conference = g.away_conference
    ),
    per_team AS (
      -- HOME side
      SELECT
        season_year,
        home_team_id AS team_id,
        CASE
          WHEN home_score IS NULL OR away_score IS NULL THEN 0.0
          WHEN home_score > away_score THEN 2.0
          WHEN went_ot = 1 AND home_score < away_score THEN 1.0
          ELSE 0.0
        END AS pts,
        CASE
          WHEN home_score IS NULL OR away_score IS NULL THEN 0
          ELSE 1
        END AS game_ct
      FROM conf_games

      UNION ALL

      -- AWAY side
      SELECT
        season_year,
        away_team_id AS team_id,
        CASE
          WHEN home_score IS NULL OR away_score IS NULL THEN 0.0
          WHEN away_score > home_score THEN 2.0
          WHEN went_ot = 1 AND away_score < home_score THEN 1.0
          ELSE 0.0
        END AS pts,
        CASE
          WHEN home_score IS NULL OR away_score IS NULL THEN 0
          ELSE 1
        END AS game_ct
      FROM conf_games
    ),
    agg AS (
      SELECT team_id, SUM(pts) AS points, SUM(game_ct) AS games_counted
      FROM per_team
      GROUP BY team_id
    )
    INSERT INTO team_coe_components (season_year, team_id, component, points, games_counted, formula_version, notes)
    SELECT
      ? AS season_year,
      team_id,
      'conf_base' AS component,
      COALESCE(points, 0.0) AS points,
      COALESCE(games_counted, 0) AS games_counted,
      ? AS formula_version,
      'Win=2, OT loss=1, Loss=0; conference games only; team-only CoE'
    FROM agg;
    """
    conn.execute(sql, (season_year, season_year, FORMULA_VERSION))


def compute_playoff_components(conn: sqlite3.Connection, season_year: int) -> None:
    # CFP games are game_phase='cfp' (your stable concept). Team gets:
    # - +6 once if appears in any CFP game
    # - +1.5 per CFP game played
    conn.execute(
        """
        DELETE FROM team_coe_components
        WHERE season_year=? AND component IN ('playoff_participation','playoff_games') AND formula_version=?
        """,
        (season_year, FORMULA_VERSION),
    )

    sql = """
    WITH cfp AS (
      SELECT *
      FROM v_games_enriched
      WHERE season_year = ?
        AND game_phase = 'cfp'
    ),
    appearances AS (
      SELECT home_team_id AS team_id FROM cfp
      UNION ALL
      SELECT away_team_id AS team_id FROM cfp
    ),
    agg AS (
      SELECT team_id, COUNT(*) AS games_played
      FROM appearances
      GROUP BY team_id
    )
    INSERT INTO team_coe_components (season_year, team_id, component, points, games_counted, formula_version, notes)
    SELECT
      ? AS season_year,
      team_id,
      'playoff_participation' AS component,
      6.0 AS points,
      1 AS games_counted,
      ? AS formula_version,
      '+6 per participating team (baseline)'
    FROM agg;

    """
    conn.execute(sql, (season_year, season_year, FORMULA_VERSION))

    sql2 = """
    WITH cfp AS (
      SELECT *
      FROM v_games_enriched
      WHERE season_year = ?
        AND game_phase = 'cfp'
    ),
    appearances AS (
      SELECT home_team_id AS team_id FROM cfp
      UNION ALL
      SELECT away_team_id AS team_id FROM cfp
    ),
    agg AS (
      SELECT team_id, COUNT(*) AS games_played
      FROM appearances
      GROUP BY team_id
    )
    INSERT INTO team_coe_components (season_year, team_id, component, points, games_counted, formula_version, notes)
    SELECT
      ? AS season_year,
      team_id,
      'playoff_games' AS component,
      1.5 * games_played AS points,
      games_played AS games_counted,
      ? AS formula_version,
      '+1.5 per CFP game appearance'
    FROM agg;
    """
    conn.execute(sql2, (season_year, season_year, FORMULA_VERSION))


def rollup_totals(conn: sqlite3.Connection, season_year: int) -> None:
    conn.execute(
        "DELETE FROM team_coefficient_by_year WHERE season_year=? AND formula_version=?",
        (season_year, FORMULA_VERSION),
    )

    rollup_sql = """
    WITH agg AS (
      SELECT
        season_year,
        team_id,
        SUM(points) AS total_points,
        SUM(games_counted) AS games_counted
      FROM team_coe_components
      WHERE season_year=? AND formula_version=?
      GROUP BY season_year, team_id
    )
    INSERT INTO team_coefficient_by_year
      (season_year, team_id, total_points, games_counted, points_per_game, formula_version)
    SELECT
      season_year,
      team_id,
      COALESCE(total_points, 0.0) AS total_points,
      COALESCE(games_counted, 0) AS games_counted,
      CASE WHEN COALESCE(games_counted, 0) > 0 THEN (1.0 * total_points) / games_counted ELSE 0.0 END AS ppg,
      ? AS formula_version
    FROM agg;
    """
    conn.execute(rollup_sql, (season_year, FORMULA_VERSION, FORMULA_VERSION))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    conn = connect(args.db)
    try:
        compute_nonconf_component(conn, args.year)
        compute_conf_component(conn, args.year)
        compute_playoff_components(conn, args.year)
        rollup_totals(conn, args.year)
        conn.commit()
    finally:
        conn.close()

    print(f"Team CoE computed for {args.year} (formula_version={FORMULA_VERSION})")


if __name__ == "__main__":
    main()