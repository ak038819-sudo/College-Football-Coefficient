#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
from typing import Dict, Iterable


def _upsert_conferences_new(conn: sqlite3.Connection, conf_names: Iterable[str]) -> None:
    confs = sorted({str(c).strip() for c in conf_names if c and str(c).strip()})
    if not confs:
        return

    conn.executemany(
        "INSERT OR IGNORE INTO conferences_new (conference_name) VALUES (?);",
        [(c,) for c in confs],
    )
    conn.commit()


def load_team_conference_map(conn: sqlite3.Connection, season_year: int) -> Dict[str, str]:
    """
    Returns team_name -> conference_name for a given season_year.

    Source priority:
      1) team_membership_by_season (exact year)
      2) new_alignment (fills missing teams only)
    """
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    out: Dict[str, str] = {}

    # 1) Historical membership table
    cols = [r[1] for r in cur.execute("PRAGMA table_info(team_membership_by_season)").fetchall()]
    if {"team_id", "season_year", "conference_real"}.issubset(set(cols)):

        rows = cur.execute(
            """
            SELECT DISTINCT t.team_name, m.conference_real
            FROM team_membership_by_season m
            JOIN teams t ON t.team_id = m.team_id
            WHERE m.season_year = ?
              AND COALESCE(m.is_fbs, 1) = 1
              AND m.conference_real IS NOT NULL
              AND TRIM(m.conference_real) != ''
            """,
            (season_year,),
        ).fetchall()

        _upsert_conferences_new(conn, (conf for _team, conf in rows))

        for team, conf in rows:
            if team and conf:
                out[str(team).strip()] = str(conf).strip()

    # 2) Supplement from new_alignment
    cols2 = [r[1] for r in cur.execute("PRAGMA table_info(new_alignment)").fetchall()]
    if {"team_id", "conference_name", "effective_year_start"}.issubset(set(cols2)):

        rows2 = cur.execute(
            """
            SELECT DISTINCT t.team_name, a.conference_name
            FROM new_alignment a
            JOIN teams t ON t.team_id = a.team_id
            WHERE a.effective_year_start <= ?
              AND a.conference_name IS NOT NULL
              AND TRIM(a.conference_name) != ''
            """,
            (season_year,),
        ).fetchall()

        _upsert_conferences_new(conn, (conf for _team, conf in rows2))

        for team, conf in rows2:
            team = str(team).strip()
            if team and conf and team not in out:
                out[team] = str(conf).strip()

    if not out:
        raise RuntimeError(
            f"Could not build team->conference map for season_year={season_year}"
        )

    return out