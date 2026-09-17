#!/usr/bin/env python3
"""
Team CoE, formula_version='v1': bounty-adjusted.

Adjusted points per game = base_points * (1 + opponent_ppg)
  where opponent_ppg = opponent's own TEAM rolling 5yr CoE PPG (v1),
  taken as of season_year - 1 (the prior season's rolling window).

Cold start: if the opponent has no v1 rolling row for season_year - 1,
or that row's window isn't a full 5 years (window_end_year -
window_start_year != 4), opponent_ppg is treated as 0 -- which, under
this additive formula, is identical to awarding raw base points
(2*(1+0)=2, 1*(1+0)=1, 0*(1+0)=0). So early seasons naturally fall
back to unweighted scoring with no special-casing needed downstream.

Applies to BOTH non-conference and conference games (per project
decision -- this differs from compute_team_coe_v0.py, which is a flat,
unweighted baseline kept for comparison/audit).

Playoff bonuses (+6 participation, +1.5/game) are flat bonuses, not
win/loss scoring, so they are NOT bounty-weighted -- copied over
unchanged from the v0 logic, just tagged formula_version='v1'.

This script computes ONE season at a time and must be run in
chronological order (earliest season first) via run_team_coe_v1_pipeline.py,
since year Y depends on every opponent's v1 rolling PPG as of year Y-1.
"""
from __future__ import annotations

import argparse
import sqlite3
from typing import Dict, Optional

FORMULA_VERSION = "v1"
WINDOW = 5


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def load_opponent_ppg_as_of_prior_year(
    conn: sqlite3.Connection, season_year: int
) -> Dict[int, float]:
    """
    Returns {team_id: rolling_5yr_ppg} as of season_year - 1, formula_version='v1',
    but ONLY for teams that actually have a computed yearly row (in
    team_coefficient_by_year) for EVERY one of the 5 years in that
    trailing window. Teams without a qualifying row are absent from
    the dict; callers should treat a missing team as ppg=0 (cold start).

    NOTE: compute_rolling() always labels window_start_year as
    season_year-4 regardless of whether that far back actually has
    data, so the window's *label* span is never a reliable "is this a
    full window" signal -- it's always season_year-4..season_year even
    in the very first season with any data at all. We check actual
    yearly presence instead.
    """
    prior_year = season_year - 1
    window_start = prior_year - (WINDOW - 1)

    rows = conn.execute(
        """
        SELECT r.team_id, r.points_per_game_5yr
        FROM team_coefficient_rolling_5yr r
        WHERE r.season_year = ? AND r.formula_version = ?
          AND (
            SELECT COUNT(DISTINCT y.season_year)
            FROM team_coefficient_by_year y
            WHERE y.team_id = r.team_id
              AND y.formula_version = r.formula_version
              AND y.season_year BETWEEN ? AND ?
          ) = ?
        """,
        (prior_year, FORMULA_VERSION, window_start, prior_year, WINDOW),
    ).fetchall()

    return {int(r["team_id"]): float(r["points_per_game_5yr"]) for r in rows}


def base_pts(home_score: Optional[int], away_score: Optional[int], went_ot: int, is_home: bool) -> Optional[float]:
    if home_score is None or away_score is None:
        return None
    if is_home:
        if home_score > away_score:
            return 2.0
        if went_ot == 1 and home_score < away_score:
            return 1.0
        return 0.0
    else:
        if away_score > home_score:
            return 2.0
        if went_ot == 1 and away_score < home_score:
            return 1.0
        return 0.0


def compute_bounty_components(
    conn: sqlite3.Connection, season_year: int, opponent_ppg: Dict[int, float]
) -> None:
    """
    Writes 'nonconf_base' and 'conf_base' components for season_year,
    formula_version='v1', with bounty applied to both.
    """
    conn.execute(
        """
        DELETE FROM team_coe_components
        WHERE season_year=? AND component IN ('nonconf_base','conf_base') AND formula_version=?
        """,
        (season_year, FORMULA_VERSION),
    )

    games = conn.execute(
        """
        SELECT home_team_id, away_team_id, home_score, away_score, went_ot, is_nonconference
        FROM v_games_enriched
        WHERE season_year = ?
          AND home_conference IS NOT NULL
          AND away_conference IS NOT NULL
        """,
        (season_year,),
    ).fetchall()

    # team_id -> component -> [points_sum, games_sum]
    agg: Dict[int, Dict[str, list]] = {}

    def add(team_id: int, component: str, pts: float) -> None:
        d = agg.setdefault(team_id, {"nonconf_base": [0.0, 0], "conf_base": [0.0, 0]})
        d[component][0] += pts
        d[component][1] += 1

    for g in games:
        if g["home_score"] is None or g["away_score"] is None:
            continue

        component = "nonconf_base" if g["is_nonconference"] == 1 else "conf_base"

        home_base = base_pts(g["home_score"], g["away_score"], g["went_ot"], True)
        away_base = base_pts(g["home_score"], g["away_score"], g["went_ot"], False)

        home_opp_ppg = opponent_ppg.get(int(g["away_team_id"]), 0.0)
        away_opp_ppg = opponent_ppg.get(int(g["home_team_id"]), 0.0)

        home_adj = home_base * (1.0 + home_opp_ppg)
        away_adj = away_base * (1.0 + away_opp_ppg)

        add(int(g["home_team_id"]), component, home_adj)
        add(int(g["away_team_id"]), component, away_adj)

    rows = []
    for team_id, comps in agg.items():
        for component, (pts, ct) in comps.items():
            if ct == 0:
                continue
            rows.append(
                (
                    season_year,
                    team_id,
                    component,
                    pts,
                    ct,
                    FORMULA_VERSION,
                    "Bounty-adjusted: base*(1+opponent_team_ppg_5yr_as_of_prior_year)",
                )
            )

    conn.executemany(
        """
        INSERT INTO team_coe_components
          (season_year, team_id, component, points, games_counted, formula_version, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def copy_playoff_components(conn: sqlite3.Connection, season_year: int) -> None:
    """
    Playoff bonuses are flat, not bounty-weighted. Reuse the same logic
    as v0, just tagged as v1.
    """
    conn.execute(
        """
        DELETE FROM team_coe_components
        WHERE season_year=? AND component IN ('playoff_participation','playoff_games') AND formula_version=?
        """,
        (season_year, FORMULA_VERSION),
    )

    sql = """
    WITH cfp AS (
      SELECT * FROM v_games_enriched WHERE season_year = ? AND game_phase = 'cfp'
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
    SELECT ? , team_id, 'playoff_participation', 6.0, 1, ?, '+6 per participating team (flat, not bounty-weighted)'
    FROM agg;
    """
    conn.execute(sql, (season_year, season_year, FORMULA_VERSION))

    sql2 = """
    WITH cfp AS (
      SELECT * FROM v_games_enriched WHERE season_year = ? AND game_phase = 'cfp'
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
    SELECT ? , team_id, 'playoff_games', 1.5 * games_played, games_played, ?, '+1.5 per CFP game appearance (flat)'
    FROM agg;
    """
    conn.execute(sql2, (season_year, season_year, FORMULA_VERSION))


def rollup_totals(conn: sqlite3.Connection, season_year: int) -> None:
    conn.execute(
        "DELETE FROM team_coefficient_by_year WHERE season_year=? AND formula_version=?",
        (season_year, FORMULA_VERSION),
    )
    sql = """
    WITH agg AS (
      SELECT season_year, team_id, SUM(points) AS total_points, SUM(games_counted) AS games_counted
      FROM team_coe_components
      WHERE season_year=? AND formula_version=?
      GROUP BY season_year, team_id
    )
    INSERT INTO team_coefficient_by_year
      (season_year, team_id, total_points, games_counted, points_per_game, formula_version)
    SELECT
      season_year, team_id,
      COALESCE(total_points, 0.0),
      COALESCE(games_counted, 0),
      CASE WHEN COALESCE(games_counted,0) > 0 THEN (1.0*total_points)/games_counted ELSE 0.0 END,
      ?
    FROM agg;
    """
    conn.execute(sql, (season_year, FORMULA_VERSION, FORMULA_VERSION))


def compute_rolling(conn: sqlite3.Connection, season_year: int) -> None:
    start_year = season_year - (WINDOW - 1)
    conn.execute(
        "DELETE FROM team_coefficient_rolling_5yr WHERE season_year=? AND formula_version=?",
        (season_year, FORMULA_VERSION),
    )
    sql = """
    WITH windowed AS (
      SELECT team_id, SUM(total_points) AS total_points_5yr, SUM(games_counted) AS games_counted_5yr
      FROM team_coefficient_by_year
      WHERE formula_version=? AND season_year BETWEEN ? AND ?
      GROUP BY team_id
    )
    INSERT INTO team_coefficient_rolling_5yr
      (season_year, team_id, window_start_year, window_end_year,
       total_points_5yr, games_counted_5yr, points_per_game_5yr, formula_version)
    SELECT
      ?, team_id, ?, ?,
      COALESCE(total_points_5yr,0.0), COALESCE(games_counted_5yr,0),
      CASE WHEN COALESCE(games_counted_5yr,0)>0 THEN (1.0*total_points_5yr)/games_counted_5yr ELSE 0.0 END,
      ?
    FROM windowed;
    """
    conn.execute(
        sql,
        (FORMULA_VERSION, start_year, season_year, season_year, start_year, season_year, FORMULA_VERSION),
    )


def compute_season(conn: sqlite3.Connection, season_year: int) -> None:
    opponent_ppg = load_opponent_ppg_as_of_prior_year(conn, season_year)
    compute_bounty_components(conn, season_year, opponent_ppg)
    copy_playoff_components(conn, season_year)
    rollup_totals(conn, season_year)
    compute_rolling(conn, season_year)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    conn = connect(args.db)
    try:
        compute_season(conn, args.year)
        conn.commit()
    finally:
        conn.close()

    print(f"Team CoE (v1, bounty-adjusted) computed for {args.year}")


if __name__ == "__main__":
    main()
