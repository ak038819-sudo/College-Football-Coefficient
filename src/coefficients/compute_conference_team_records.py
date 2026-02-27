#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def compute_records(conn: sqlite3.Connection, season_year: int) -> int:
    """
    Populates conference_team_records_by_year for the given season_year using v_games_enriched.

    Definitions:
      - overall record: all games with known score AND known conference for that team in that season
        (includes regular/bowl/cfp; excludes rows where team conference is NULL)
      - conference record: games where BOTH teams have known conferences AND home_conference == away_conference
        (i.e., intra-conference games). This automatically excludes non-conference games.
    """
    # Clear existing rows for the year so this is deterministic/idempotent.
    conn.execute("DELETE FROM conference_team_records_by_year WHERE season_year=?", (season_year,))

    insert_sql = """
    WITH per_team AS (
      -- HOME team perspective
      SELECT
        g.season_year AS season_year,
        g.home_team_id AS team_id,
        g.home_conference AS conference,

        -- Overall result (only if scores exist)
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_score > g.away_score THEN 1 ELSE 0
        END AS overall_win,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_score < g.away_score THEN 1 ELSE 0
        END AS overall_loss,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_score = g.away_score THEN 1 ELSE 0
        END AS overall_tie,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          ELSE 1
        END AS overall_game,

        -- Conference game flag
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference = g.away_conference THEN 1
          ELSE 0
        END AS is_conf_game,

        -- Conference result (only when is_conf_game=1)
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference != g.away_conference THEN 0
          WHEN g.home_score > g.away_score THEN 1 ELSE 0
        END AS conf_win,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference != g.away_conference THEN 0
          WHEN g.home_score < g.away_score THEN 1 ELSE 0
        END AS conf_loss,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference != g.away_conference THEN 0
          WHEN g.home_score = g.away_score THEN 1 ELSE 0
        END AS conf_tie

      FROM v_games_enriched g
      WHERE g.season_year = ?
        AND g.home_conference IS NOT NULL

      UNION ALL

      -- AWAY team perspective
      SELECT
        g.season_year AS season_year,
        g.away_team_id AS team_id,
        g.away_conference AS conference,

        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.away_score > g.home_score THEN 1 ELSE 0
        END AS overall_win,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.away_score < g.home_score THEN 1 ELSE 0
        END AS overall_loss,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.away_score = g.home_score THEN 1 ELSE 0
        END AS overall_tie,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          ELSE 1
        END AS overall_game,

        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference = g.away_conference THEN 1
          ELSE 0
        END AS is_conf_game,

        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference != g.away_conference THEN 0
          WHEN g.away_score > g.home_score THEN 1 ELSE 0
        END AS conf_win,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference != g.away_conference THEN 0
          WHEN g.away_score < g.home_score THEN 1 ELSE 0
        END AS conf_loss,
        CASE
          WHEN g.home_score IS NULL OR g.away_score IS NULL THEN 0
          WHEN g.home_conference IS NULL OR g.away_conference IS NULL THEN 0
          WHEN g.home_conference != g.away_conference THEN 0
          WHEN g.away_score = g.home_score THEN 1 ELSE 0
        END AS conf_tie

      FROM v_games_enriched g
      WHERE g.season_year = ?
        AND g.away_conference IS NOT NULL
    ),
    agg AS (
      SELECT
        season_year,
        conference,
        team_id,

        SUM(conf_win) AS conf_wins,
        SUM(conf_loss) AS conf_losses,
        SUM(conf_tie) AS conf_ties,
        SUM(is_conf_game) AS conf_games,

        SUM(overall_win) AS overall_wins,
        SUM(overall_loss) AS overall_losses,
        SUM(overall_tie) AS overall_ties,
        SUM(overall_game) AS overall_games

      FROM per_team
      GROUP BY season_year, conference, team_id
    )
    INSERT INTO conference_team_records_by_year (
      season_year,
      conference,
      team_id,
      conf_wins, conf_losses, conf_ties, conf_games,
      overall_wins, overall_losses, overall_ties, overall_games,
      conf_win_pct, overall_win_pct
    )
    SELECT
      season_year,
      conference,
      team_id,
      conf_wins, conf_losses, conf_ties, conf_games,
      overall_wins, overall_losses, overall_ties, overall_games,
      CASE
        WHEN conf_games > 0 THEN (1.0 * conf_wins + 0.5 * conf_ties) / conf_games
        ELSE 0.0
      END AS conf_win_pct,
      CASE
        WHEN overall_games > 0 THEN (1.0 * overall_wins + 0.5 * overall_ties) / overall_games
        ELSE 0.0
      END AS overall_win_pct
    FROM agg;
    """

    conn.execute(insert_sql, (season_year, season_year))
    return conn.execute(
        "SELECT COUNT(*) AS n FROM conference_team_records_by_year WHERE season_year=?",
        (season_year,),
    ).fetchone()["n"]


def refresh_validation_table(conn: sqlite3.Connection, season_year: int) -> None:
    """
    Optional helper: keep a single table that lets you compare imported conf_rank vs computed records.
    Imported standings may not exist yet; conf_rank will be NULL until then.
    """
    conn.execute("DELETE FROM conference_standings_validation WHERE season_year=?", (season_year,))

    conn.execute(
        """
        INSERT INTO conference_standings_validation (
          season_year, conference, team_id,
          conf_rank, conf_wins, conf_losses, conf_games,
          overall_wins, overall_losses
        )
        SELECT
          r.season_year,
          r.conference,
          r.team_id,
          s.conf_rank,
          r.conf_wins,
          r.conf_losses,
          r.conf_games,
          r.overall_wins,
          r.overall_losses
        FROM conference_team_records_by_year r
        LEFT JOIN conference_standings_by_year s
          ON s.season_year=r.season_year
         AND s.conference=r.conference
         AND s.team_id=r.team_id
        WHERE r.season_year=?;
        """,
        (season_year,),
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--no-validation", action="store_true", help="Skip refreshing conference_standings_validation.")
    args = p.parse_args()

    conn = connect(args.db)
    try:
        n = compute_records(conn, args.year)
        if not args.no_validation:
            refresh_validation_table(conn, args.year)
        conn.commit()
    finally:
        conn.close()

    print(f"Computed conference_team_records_by_year for {args.year}: {n} rows")


if __name__ == "__main__":
    main()