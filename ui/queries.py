# ui/queries.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List

DB_DEFAULT = Path("db/league.db")


def connect(db_path: str | Path = DB_DEFAULT) -> sqlite3.Connection:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path.resolve()}")

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row["name"] for row in cur.fetchall()]


def get_rulesets(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        """
        SELECT DISTINCT ruleset
        FROM playoff_field_by_year
        ORDER BY ruleset
        """
    )
    return [str(r["ruleset"]) for r in cur.fetchall()]


def get_formula_versions(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        """
        SELECT DISTINCT formula_version
        FROM playoff_field_by_year
        ORDER BY formula_version
        """
    )
    return [str(r["formula_version"]) for r in cur.fetchall()]


def get_available_seasons(
    conn: sqlite3.Connection,
    ruleset: str,
    formula_version: str,
) -> List[int]:
    cur = conn.execute(
        """
        SELECT DISTINCT season_year
        FROM playoff_field_by_year
        WHERE ruleset = ?
          AND formula_version = ?
        ORDER BY season_year DESC
        """,
        (ruleset, formula_version),
    )
    return [int(r["season_year"]) for r in cur.fetchall()]


def fetch_playoff_field_flexible(
    conn: sqlite3.Connection,
    season_year: int,
    ruleset: str,
    formula_version: str,
):
    """
    Matches your current playoff_field_by_year schema:

      season_year
      team_id
      conference
      conf_rank
      conf_coe_rank
      bid_type
      pot
      formula_version
      ruleset
      created_at

    Derived fields:
      is_bye       -> pot == 0
      is_champion  -> bid_type == 'champion'
    """

    # Optional rolling table
    join_rolling = ""
    rolling_select = "NULL AS total_points_5yr, NULL AS points_per_game_5yr"

    if table_exists(conn, "team_coefficient_rolling_5yr"):
        roll_cols = get_table_columns(conn, "team_coefficient_rolling_5yr")

        tp = "r.total_points_5yr" if "total_points_5yr" in roll_cols else "NULL"
        ppg = (
            "r.points_per_game_5yr"
            if "points_per_game_5yr" in roll_cols
            else "NULL"
        )

        rolling_select = f"{tp} AS total_points_5yr, {ppg} AS points_per_game_5yr"

        join_rolling = """
        LEFT JOIN team_coefficient_rolling_5yr r
               ON r.team_id = f.team_id
              AND r.season_year = f.season_year
        """

    sql = f"""
        SELECT
            f.season_year,
            f.ruleset,
            f.formula_version,
            t.team_name,
            f.conference,
            f.pot,
            f.bid_type,
            f.conf_coe_rank,
            f.conf_rank,

            -- Derived flags
            CASE WHEN f.pot = 0 THEN 1 ELSE 0 END AS is_bye,
            CASE WHEN LOWER(f.bid_type) = 'champion' THEN 1 ELSE 0 END AS is_champion,

            {rolling_select}

        FROM playoff_field_by_year f
        JOIN teams t ON t.team_id = f.team_id
        {join_rolling}
        WHERE f.season_year = ?
          AND f.ruleset = ?
          AND f.formula_version = ?
        ORDER BY
            CASE WHEN f.pot = 0 THEN 0 ELSE 1 END,
            f.pot ASC,
            f.conf_coe_rank ASC,
            t.team_name ASC
    """

    cur = conn.execute(sql, (season_year, ruleset, formula_version))
    return cur.fetchall()